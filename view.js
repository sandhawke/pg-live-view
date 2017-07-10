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

    this.rowEE = new Map() // target->EventEmitter for that target (if exists)
    this.handler = {
      get: (target, name) => {
        // console.log('PROXY GET', target, JSON.stringify(name), typeof name)
        if (name === 'on') {
          let ee = this.rowEE.get(target)
          if (ee == undefined) {
            ee = new EventEmitter()
            this.rowEE.set(target, ee)
            debug('new ee for row', target, ee)
          } else {
            debug('existing ee for row', target, ee)
          }
          return ee.on.bind(ee)
        } else if (name === '_targetBehindProxy') {
          // weird trick to reach through the proxy   :-/
          // Maybe make this a Symbol so there chance of collision?
          return target
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


    // this WAS down in on('appear'), but then I wanted the listenClient
    // for use in .add(), so I moved it up here.  The comment was:
    //
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
    
    // returns syncronously; will buffer operations until connection is ready
  }

  /**
   * Add a new database record with this data.  
   *
   * For now, don't give it an id; let the remote end asign one.
   * 
   * Returns a promise for that id.
   *
   * It would be NICE to allow us to create the proxy here, now,
   * synchronously, but I believe that would require having another
   * column in the table (creation_id) so that we could recognize our
   * own creation coming back in the trigger, since it might come back
   * before 'RETURNING id' tells us our response.  Oh, unless maybe we
   * can do it OVER THE LISTEN CLIENT?



A proxy will * If data has a .id property, it must be distinct from
any other * .id value in the database.  If it has no .id, a temporary
* negative one will be assigned before returning; it will be *
modified to the global one from the database at some point.
   *
   * Returns quickly (synchronously) with a usable proxy object, which
   * you can watch and keep altering.
   */
  add (data) {
    if (data.id) {
      throw ('not implemented')
    }
    
    // check that it meets filter?

    // check that properties match schema?

    // make a copy of the backing object, to avoid the temptation for
    // caller to edit it, rather than going throught he proxy
    const target = Object.assign({}, data)
    const proxy = new Proxy(target, this.handler)    

    // If you want to know about rows added by other parts of the same
    // process, you want listen for 'added', but these objects do not
    // necessarily have .id fields.
    this._ee.emit('added', proxy)

    const props = []
    const dollars = []
    const values = []
    let counter = 1
    for (let key of Object.keys(data)) {
      props.push(key)
      dollars.push('$' + counter++)
      values.push(data[key])
    }

    // we have to do this over the listenClient, not the pool, to make
    // sure we get the id back here BEFORE the trigger runs.
    // Otherwise, when appear looks in this.proxies, this wont be
    // there, and it'll create a new proxy.
    this.listenClient.query(`INSERT INTO ${this.tableName} (${props.join(', ')}) VALUES (${dollars.join(', ')}) RETURNING id`, values)
      .then(res => {
        debug('creation INSERT returned', res.rows[0].id)
        target.id = res.rows[0].id
        this.proxies.set(target.id, proxy)
        const ee = this.rowEE.get(target)
        if (ee) {
          ee.emit('saved', proxy)
        }
        this._ee.emit('appear', proxy)
      })

    return proxy
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

  handleUpdateEvent (newdata) {
    debug('update on', newdata.id, JSON.stringify(newdata))
    const id = newdata.id // in one version we overwrote .id later
    const proxy = this.proxies.get(id)
    const target = proxy._targetBehindProxy
    const ee = this.rowEE.get(target)
    let old
    if (ee) {
      // only save a "Before" copy if we have an ee
      old = Object.assign({}, target)
    }
    
    debug('database update from', target)
    debug('database update to  ', newdata)

    // This totally does not work for deep or linked objects...
    //
    // It should be if a field is a .id for another object, then
    // we make this a js link here.   That'd be cool.

    for (let key of Object.keys(target)) {
      delete target[key]
    }
    Object.assign(target, newdata)

    debug('database update left target as', target)
    debug('database returning old as     ', old)
    
    if (ee) {
      debug('emit change', old, proxy)
      ee.emit('change', old, proxy)
    } else {
      debug('no event emitter')
    }
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
               this.listenClient = client
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
                         const proxy = this.proxies.get(data.id)
                         const target = proxy._targetBehindProxy
                         const ee = this.rowEE.get(target)
                         if (ee) {
                           ee.emit('disappear', data)
                         }
                         this.rowEE.delete(target)
                         this.proxies.delete(data.id)
                       } else if (op === 'UPDATE') {
                         this.handleUpdateEvent(data)
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
    let proxy = this.proxies.get(data.id)
    if (!proxy) {
      debug('new proxy needed for',  data.id)
      proxy = new Proxy(data, this.handler)
      this.proxies.set(data.id, proxy)
      debug('generating APPEAR on proxy', proxy)
      this._ee.emit('appear', proxy)
    } else {
      debug('duplicate appear for', data.id)
    }
  }
}

module.exports = View
