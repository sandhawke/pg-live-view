'use strict'

const canonicalizePropertiesArgument = require('./props-arg')
const pg = require('pg')
const EventEmitter = require('eventemitter3')
const SQL = require('sql-template-strings')
const debug = require('debug')('View')
const QueryStream = require('pg-query-stream')

class View {
  constructor (filter, tablename, opts = {}) {
    this._ee = new EventEmitter()
    this.filter = canonicalizePropertiesArgument(filter)
    this.tableName = tablename
    console.assert(tablename)

    // If you have a lot of views, it makes a lot of sense to have
    // them use the same pool, so create it yourself and pass it in
    if (opts.pool) {
      this.pool = opts.pool
    } else {
      this._database = opts.database
      this.pool = new pg.Pool({database: this._database})
      this.myPool = true
    }

    this.proxies = new Map()

    this.rowEE = new Map() // id => the EventEmitter for that row, if there is one
    this.handler = {
      get: (target, name) => {
        // console.log('PROXY GET', target, JSON.stringify(name), typeof name)
        if (name === 'on') {
          let ee = this.rowEE.get(target.id)
          if (ee == undefined) {
            ee = new EventEmitter()
            this.rowEE.set(target.id, ee)
            debug('new ee for row', target.id, ee)
          } else {
            debug('existing ee for row', target.id, ee)
          }
          return ee.on.bind(ee)
        } else if (name === '_gotDatabaseUpdate') {
          // weird trick to reach through the proxy   :-/
          // Maybe make this a Symbol so there chance of collision?
          return (newdata => {
            debug('database update from', target)
            debug('database update to  ', newdata)
            const old = Object.assign({}, target)
            for (let key of Object.keys(target)) {
              target[key] = newdata[key]
              delete newdata[key]
            }
            for (let key of Object.keys(newdata)) {
              target[key] = newdata[key]
            }
            debug('database update left target as', target)
            debug('database returning old as     ', old)
            return old
          })
        }
        return target[name]
      },
      set: (target, name, value) => {
        // mark it dirty, and SQL UPDATE soon
        //
        // and use .deleted = true as delete record, I think.
        // 
        console.error('set', target, name, value)
      }
    }
    
    // returns syncronously; will buffer operations until connection is ready
  }

  /**
   * Add a new database record with this data
   *
   * Returns a promise of the data being added (not currently of the
   * data coming back, with the proxy, which might be nice.)
   */
  add (data) {
    // check that id is unique
    // return a promise of saved-to-disk?  or of loop-back?!
    // check that it meets filter?
    // check that properties match schema?
    // maybe distribute in-memory as well.
    const props = []
    const dollars = []
    const values = []
    let counter = 1
    for (let key of Object.keys(data)) {
      props.push(key)
      dollars.push('$' + counter++)
      values.push(data[key])
    }
    return this.query(`INSERT INTO ${this.tableName} (${props.join(', ')}) VALUES (${dollars.join(', ')})`, values)
  }
  
  close () {
    // remove triggers/function?   Nah.
    debug('calling stopListen', this.stopListen)
    if (this.stopListen) {
      this.stopListen()
    }
    if (this.myPool) {
      debug('calling pool.end', this.pool.end)
      this.pool.end()
        .then(() => {
          debug('pool end resolved')
        })
    }
  }
  
  // I like to hide the eventemitter interface, for now at least, to
  // reduce ... weird things.
  
  on (eventName, callback) {
    if (eventName === 'appear') {
      this._ee.on(eventName, callback)

      // We could probably do this in constructor, knowing no events
      // could occur until at least a tick(), but maybe someday we'll
      // have some of this data cached in memory and can give some
      // immediate results.  So, better to wait for the 'appear' handler
      // to be added, I think.
      this.startListening()
        .then(() => this.startQuery())
        .then(() => {
          debug('query completed')
        })
    } else {
      throw Error('unknown event name:', eventName)
    }
  }

  off (...args) {
    return this._ee.off(...args)
  }

  query (text, data) {
    debug('QUERY', text, data)
    return this.pool.query(text, data)
  }

  /**
   * Set up the necessary database triggers and start listening to our
   * notification channe.  Async: until this resolves, we might miss
   * database changes.
   */
  startListening () {
    const all = []
    all.push(this.makeFunction()
             .then(() => {
               return this.makeTrigger()
             })
            )

    all.push(this.pool.connect()
             .then(client => {
               this.stopListen = () => {
                 client.release()
                 // debug('client.release()', client.release, client.release())
                 // debug('client.end()', client.end, client.end())
                 debug('listen-client .release called')
               }
               const sql = `LISTEN ${this.tableName}_notify`
               return (
                 client.query(sql)
                   .then(() => {
                     debug('listening via', sql)
                     // console.log('ON NOTIFICATION')
                     client.on('notification', msg => {
                       debug('postgres notification received:', msg)
                       // console.log('***GOT NOTIFICATION', msg.payload)
                       const [op, data] = JSON.parse(msg.payload)
                       if (op === 'INSERT') {
                         this.appear(data)
                       } else if (op === 'DELETE') {
                         const ee = this.rowEE.get(data.id)
                         if (ee) {
                           ee.emit('disappear', data)
                         }
                         this.rowEE.delete(data.id)
                         this.proxies.delete(data.id)
                       } else if (op === 'UPDATE') {
                         const id = data.id // data.id gets deleted in copying
                         debug('update on', data.id, JSON.stringify(data))
                         const proxy = this.proxies.get(id)
                         const old = proxy._gotDatabaseUpdate(data)
                         const ee = this.rowEE.get(id)
                         if (ee) {
                           debug('emit change', old, proxy)
                           ee.emit('change', old, proxy)
                         } else {
                           debug('no event emitter')
                         }
                       } else {
                         throw new Error('unexpected database change code')
                       }
                     })
                   })
                   .catch(e => {
                     client.release()
                     console.error('cant listen', e.message, e.stack)
                   })
               )
             })
            )

    return Promise.all(all)
  }             
  
  makeFunction () {
  return this.query(`
    CREATE OR REPLACE FUNCTION live_view_notify() RETURNS TRIGGER AS $$
    DECLARE 
        row json;
        msg json;
    BEGIN
        IF (TG_OP = 'DELETE') THEN
            row = json_build_object('id', OLD.id);
        ELSE
            row = row_to_json(NEW);
        END IF;
        msg = json_build_array(TG_OP, row);
        PERFORM pg_notify(TG_TABLE_NAME || '_notify', msg::text);
        RETURN NULL; 
    END;
    $$ LANGUAGE plpgsql;
    `)
  }

  makeTrigger () {
    // DO *NOT* SQL-QUOTE the table names
    return (
      this.query(`
           DROP TRIGGER IF EXISTS ${this.tableName}_live_view
           ON ${this.tableName} CASCADE
        `).then(() => {
          return this.query(`
              CREATE TRIGGER ${this.tableName}_live_view
              AFTER INSERT OR UPDATE OR DELETE ON ${this.tableName}
                  FOR EACH ROW EXECUTE PROCEDURE live_view_notify();
            `)
        })
    )
  }

  startQuery () {
    return (
      this.pool.query(`SELECT * FROM ${this.tableName}`)
        .then(res => {
          for (let row of res.rows) {
            this.appear(row)
          }
        })
    )
  }

                      /*  not using this for the moment,
                          in trying to debug...
   startQuery () {
    this.pool.connect((err, client, done) => {
      if (err) throw err
      const q = new QueryStream(`SELECT * FROM ${this.tableName}`)
      const stream = client.query(q)
      //release the client when the stream is finished
      stream.on('end', () => {
        debug('***query stream ended')
        done();
        client.end()
      })
      stream.on('data', data => {
        this.appear(data)
      })
    })
  }
                      */

  appear (data) {
    debug('generating APPEAR', data)
    const old = this.proxies.get(data.id)
    if (old) throw Error('second appear')
    const proxy = new Proxy(data, this.handler)
    this.proxies.set(data.id, proxy)
    debug('generating APPEAR on proxy', proxy)
    this._ee.emit('appear', proxy)
  }
}

module.exports = View
