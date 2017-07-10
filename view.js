'use strict'

const pg = require('pg')
const debug = require('debug')('View')
const EventEmitter = require('eventemitter3')
const setdefault = require('setdefault')
const IdDispenser = require('pg-id-dispenser')

// const canonicalizePropertiesArgument = require('./props-arg')
// const SQL = require('sql-template-strings')
// const QueryStream = require('pg-query-stream')

/**
 *
 * Options:
 *   pool: a pg connection pool, if you want us to use yours
 *   database: a pg database id, otherwise we'll use the environment
 *   dropTableFirst: useful for testing
 *   createUsingSQL: create table if missing, using these columns
 *                   (we'll supply the id column)
 *
 */
class View {
  constructor (tablename, opts = {}) {
    this.tableName = tablename
    this._ee = new EventEmitter()

    // I'm slightly dubious about this approach, but it's nicely DRY.
    Object.assign(this, opts)

    // this.filter = canonicalizePropertiesArgument(filter)

    // If you have a lot of views, it makes a lot of sense to have
    // them use the same pool, so create it yourself and pass it in
    if (!this.pool) {
      this.pool = new pg.Pool({database: this.database})
      this.endPoolOnClose = true
    }

    // For each row we hold in memory, there's a Proxy and its Target
    //
    // We hand out the Proxy, and keep the data cache in the Target.
    //
    // We (lazy) create an EventEmitter for the Target, only if someone
    // accesses its .on property.

    this.proxiesById = new Map()
    this.rowEE = new Map() // target->EventEmitter for that target
    this.handler = {
      get: this.proxyHandlerGet.bind(this),
      set: this.proxyHandlerSet.bind(this)
    }
    this.needToSave = new Map()

    this.dispenser = new IdDispenser({database: this.database})
    
    // returns syncronously, of course, since it's a constructor
    //
    // on(...) and add(...) will invoke connect() for us and buffer until
    // it's ready.
  }

  proxyHandlerGet (target, name) {
    debug('proxy get', target, JSON.stringify(name), typeof name)

    // Maybe instead of returning a real EE, give a subset that requires the
    // events be 'disappear' and 'change', to avoid typos ?

    if (name === 'on') {
      let ee = this.rowEE.get(target)
      if (ee === undefined) {
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
  }

  proxyHandlerSet (target, name, value) {
    debug('set', target, name, value)
    target[name] = value
    setdefault(this.needToSave, target, new Set()).add(name)
    // do it on the nextTick so several of these in a row
    // end up as only one UPDATE operation
    process.nextTick(this.save.bind(this, target))
  }

  save (target) {
    /*
      This is a blind-overwrite save.

      To have an etag-type if-not-modified save, we'd need to add a
      version (or etag) column to the table, then we could have that
      match be part of the the WHERE clause.

      And the etag is the sha of the stable-json of the row ?  Or it's
      an objid from our objid generator?
     */
    const names = this.needToSave.get(target)
    if (!names || names.length === 0) {
      // someone else did it already
      return Promise.resolve()
    }
    this.needToSave.delete(target)
    const sets = []
    const vals = []
    let counter = 1
    for (let key of Object.keys(target)) {
      sets.push(key + '=$' + counter++)
      vals.push(target[key])
    }
    const setstr = sets.join(', ')
    const q1 = `UPDATE ${this.tableName} SET ${setstr} WHERE id=${target.id}`
    return this.query(q1, vals)
  }

  /**
   * Returns the promise of an id that will be unique in the life of
   * the SERVER, suitable as a id= for a new object or a version= for
   * an update object.
   *
   * It's basically: SELECT nextval('client_assigned_id');
   *
   * ... Except we get them 10,000 at a time so 99.99% of the time this
   * will resolve without any io, let along a server round-trip.
   *
   * If that seems "wasteful", just think of objids as 15-digits of
   * client id (assigned by server) + 4 digits of local objid
   * (assigned by that client).  And when the client runs out, it just
   * gets a new client-id.  It hurts my soul a little to use digits
   * instead of bits, but it makes debugging a little easier, and
   * the computer doesn't care.
   */
  nextObjId () {
    if (this.nextId < this.maxId) {
      this.nextId++
      return Promise.resolve(this.nextId)
    }
    return (
      this.query("SELECT nextval('client_assigned_id')")
        .then(res => {
          
        })
    )
  }

/**
 * Add a new database record with this data.
 *
 * For now, don't give it an id; let the database asign one.
 *
 * Returns a promise for the object when it appears back again, with
 * its id.
 *
 * It'd be cool to return the id-less proxy now, so you can link it
 * into structures and stuff, but I don't know how to do that
 * without adding some kind of a creation-id column to each table we
 * view, or at least a flag that we created it so there's already
 * a proxy for it.  Right now, when we get the NOTIFY, there is NO WAY
 * to tell that was a row we just created and for which we are awaiting
 * the RETURNING value.
 */
  add (data) {
    return new Promise((resolve, reject) => {
      if (data.id) {
        throw Error('not implemented')
      }

    // check that it meets filter?

    // check that properties match schema?

      const props = []
      const dollars = []
      const values = []
      let counter = 1
      for (let key of Object.keys(data)) {
        props.push(key)
        dollars.push('$' + counter++)
        values.push(data[key])
      }

      const f = () => {
        this.query(`INSERT INTO ${this.tableName} (${props.join(', ')}) VALUES (${dollars.join(', ')}) RETURNING *`, values)
        .then(res => {
          debug('creation INSERT returned', res.rows[0].id)
          // it doesn't matter if the NOTIFY arrives before or after the INSERT
          // returns; whichever one is first will create the Proxy and the
          // other will just look it up
          resolve(this.appear(res.rows[0]))
          this._ee.emit('stable')
        })
      }

      if (this.ready) {
        f()
      } else {
        debug('.add but not ready, queued')
        this.connect()
        this._ee.on('ready', f)
      }
    })
  }

  close () {
  // remove triggers/function?   Nah.

    if (this.ready) {
      this.stopListen()
      if (this.endPoolOnClose) {
        debug('calling pool.end', this.pool.end)
        this.pool.end()
        .then(() => {
          debug('pool end resolved')
        })
      }
    } else {
    // it's too soon in the process, so just set a flag
      this.pleaseClose = true
    }
  }

// I like to hide the eventemitter interface, for now at least, to
// reduce ... weird things.

  on (eventName, callback) {
    if (eventName in {appear: 1, stable: 1}) {
      this._ee.on(eventName, callback)
      this.connect()
    } else {
      throw Error('unknown event name: ' + JSON.stringify(eventName))
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
    const proxy = this.proxiesById.get(id)
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
   * Start the process of connecting, and poking at the database
   * however we need. Runs the startup query near the end. Emits
   * 'ready' event and sets this.ready=true at the point where it's
   * safe to start doing adds & updates.
   *
   * async
   */
  connect () {
    if (this.connecting || this.ready) return Promise.resolve()
    this.connecting = true
    return (
      this.dropTableIfNeeded()
        .then(this.createTableIfNeeded.bind(this))
        .then(this.startListening.bind(this))
        .then(() => {
          this.connecting = false
          this.ready = true
          this._ee.emit('ready')
          if (this.pleaseClose) this.close()
        })
        .then(this.startQuery.bind(this))
        .then(() => {
          debug('query completed')
          if (this.pleaseClose) this.close()
        })
    )
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
                       const proxy = this.proxiesById.get(data.id)
                       const target = proxy._targetBehindProxy
                       const ee = this.rowEE.get(target)
                       if (ee) {
                         ee.emit('disappear', data)
                       }
                       this.rowEE.delete(target)
                       this.proxiesById.delete(data.id)
                     } else if (op === 'UPDATE') {
                       this.handleUpdateEvent(data)
                     } else {
                       throw new Error('unexpected database change code')
                     }
                     this._ee.emit('stable')
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
    if (this.pleaseClose) return Promise.resolve()
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
    if (this.pleaseClose) return Promise.resolve()
  // DO *NOT* SQL-QUOTE the table names

  /* DO NOT drop the trigger, because that creates a window
     where we'll miss events if we open up a second view on
     the same table.  Trust that a trigger with this name
     was defined by us.
     return (
     this.query(`
     DROP TRIGGER IF EXISTS ${this.tableName}_live_view
     ON ${this.tableName} CASCADE
     `).then(() => {
  */

    return (
    this.query(`
       CREATE TRIGGER ${this.tableName}_live_view
       AFTER INSERT OR UPDATE OR DELETE ON ${this.tableName}
           FOR EACH ROW EXECUTE PROCEDURE live_view_notify(); `)
      .catch(e => {
        if (e.code === '42710' && e.routine === 'CreateTrigger') {
          // ignore error "Trigger Already Exists"
        } else {
          throw e
        }
      })
    )
  }

  dropTableIfNeeded () {
    debug('dropTableIfNeeded', this.dropTableFirst)
    if (this.dropTableFirst) {
      debug('trying to drop table')
      return this.query(`DROP TABLE IF EXISTS ${this.tableName}`)
    } else {
      return Promise.resolve()
    }
  }

  createTableIfNeeded () {
    debug('createTableIfNeeded', this.createUsingSQL)
    if (this.createUsingSQL) {
      debug('trying to create table')
      return this.query(
      `CREATE TABLE IF NOT EXISTS ${this.tableName} (
           id serial primary key,
           ${this.createUsingSQL}
         )`)
    } else {
      return Promise.resolve()
    }
  }

  startQuery () {
    return (
    this.query(`SELECT * FROM ${this.tableName}`)
      .then(res => {
        for (let row of res.rows) {
          this.appear(row)
        }
        this._ee.emit('stable')
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
    let proxy = this.proxiesById.get(data.id)
    if (!proxy) {
      debug('new proxy needed for', data.id)
      proxy = new Proxy(data, this.handler)
      this.proxiesById.set(data.id, proxy)
      debug('generating APPEAR on proxy', proxy)
      this._ee.emit('appear', proxy)
      return proxy
    } else {
      debug('duplicate appear for ' + data.id)
      return proxy
    }
  }
}

module.exports = View
