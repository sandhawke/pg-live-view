'use strict'

const pg = require('pg')
const debug = require('debug')('View')
const EventEmitter = require('eventemitter3')
const setdefault = require('./setdefault')
const IdDispenser = require('./pg-id-dispenser')

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
 *   changeNow: if truthy, then x.p=v ; x.p will show v, even before
 *              it's been confirmed as in the database
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
    this.newLocalValue = new Map()

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
      debug('MAKING this.rowEE', this.rowEE)
      // return ee.on.bind(ee)
      return (...args) => {
        debug('row.on called with', args)
        return ee.on(...args)
      }
    } else if (name === '_targetBehindProxy') {
      // weird trick to reach through the proxy   :-/
      // Maybe make this a Symbol so there chance of collision?
      return target
    }
    if (this.changeNow) {
      const localValues = this.newLocalValue.get(target)
      if (localValues) {
        const result = localValues.get(name)
        if (result !== undefined) {
          return result
        }
      }
    }
    return target[name]
  }

  proxyHandlerSet (target, name, value, receiver) {
    // not sure what to do about receiver...
    debug('set', target, name, value, receiver)
    setdefault(this.newLocalValue, target, new Map()).set(name, value)
    // do it on the nextTick so several of these in a row
    // end up as only one UPDATE operation
    process.nextTick(this.save.bind(this, target))
    return true
  }

  save (target) {
    /*
      This is a blind-overwrite save.

      To have an etag-type if-not-modified save, we'd need to add a
      version (or etag) column to the table, then we could have that
      match be part of the the WHERE clause.

      And the etag is the sha of the stable-json of the row ?  Or it's
      an opague dispensed id?
    */
    const localValues = this.newLocalValue.get(target)
    if (!localValues || localValues.size === 0) {
      // this has already gone though...
      return Promise.resolve()
    }

    // At this point nothing stops us from saving many times; the
    // localValues only go away when we get a change from the remote.

    const sets = []
    const vals = []
    let counter = 1
    for (let [key, value] of localValues.entries()) {
      sets.push(key + '=$' + counter++)
      vals.push(value)
    }
    const targref = '$' + counter++
    vals.push(target.id)
    const setstr = sets.join(', ')
    const q1 = `UPDATE ${this.tableName} SET ${setstr} WHERE id=${targref}`
    return this.query(q1, vals)
  }

  delete (id) {
    if (typeof id === 'object') {
      throw TypeError('view.delete() given an object, not obj.id')
    }
    const q1 = `DELETE FROM ${this.tableName} WHERE id=$1`
    return this.query(q1, [id])
  }

  /**
   * Add a new database record with this data.
   *
   * Returns immediately with a proxy, like you'd have gotten from an
   * 'appear' event, except it will not have a .id assigned for a
   * little while.  If you have changeNow set, the row data you
   * provided will be visible, otherwise the object will appear empty
   * until it's confirmed from the server (which might do some data
   * validation, etc).
   */
  add (data) {
    if (data.id) {
      throw Error('adding with id provided: not implemented')
    }

    // Leave the body empty until it comes back from database
    const target = { _newlyThIng: data }
    const proxy = new Proxy(target, this.handler)

    const props = []
    const dollars = []
    const values = []
    let counter = 1
    for (let key of Object.keys(data)) {
      // but let body will appear full, if you have changeNow set
      setdefault(this.newLocalValue, target, new Map()).set(key, data[key])

      props.push(key)
      dollars.push('$' + counter++)
      values.push(data[key])
    }

    // we return synchronously before we know the id, but we'll
    // fill it in pretty soon, here.
    //
    // WARNING: what might happen before the id is set, or before the
    // INSERT is complete?   Like, what if you want to delete it?
    // Or set it as the value of some other object?  Neither of those
    // will work well before having an id, I expect.
    this.dispenser.next()
      .then(id => {
        debug('id has been assigned', id)
        target.id = id
        this.proxiesById.set(id, proxy)
        debug('PROXIES SET', this.proxiesById)

        // probably not needed, but still, might be nice...
        debug('USING this.rowEE', this.rowEE)
        const ee = this.rowEE.get(target)
        if (ee) {
          ee.emit('id-assigned', id)
        } else {
          debug('no ee to get id-assigned event')
        }

        props.push('id')
        dollars.push('$' + counter++)
        values.push(id)
      })
      .then(() => {
        const f = () => {
          this.query(`INSERT INTO ${this.tableName} (${props.join(', ')}) 
                      VALUES (${dollars.join(', ')}) RETURNING *`, values)
            .then(res => {
              debug('creation INSERT returned row', res.rows[0])

              // Whenever the NOTIFY arrives, which migth be before or
              // after this point where the INSERT returns, it will
              // ignore the event because the id is already in proxiesById.

              Object.assign(target, res.rows[0])

              // we could look for conflicts and warn someone... Emit
              // a 'change-refused' event or something.
              this.newLocalValue.delete(target)

              const ee = this.rowEE.get(target)
              if (ee) {
                debug('emit change', {}, proxy)
                ee.emit('change', {}, proxy)
              }

              debug('emitting appear')
              this._ee.emit('appear', proxy)
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

    debug('add returning a proxy')
    return proxy
  }

  close () {
    // UGH -- this should return a promise,
    // so we can be fully shut down before
    // calling t.end() in testing

    // remove triggers/function?   Nah.
    return new Promise((resolve, reject) => {
      this.dispenser.close()
      if (this.ready) {
        this.stopListen()
        if (this.endPoolOnClose) {
          debug('calling pool.end', this.pool.end)
          this.pool.end()
            .then(() => {
              debug('pool end resolved')
              this._ee.emit('closed')
              resolve()
            })
        }
      } else {
        // it's too soon in the process, so just set a flag
        this.pleaseClose = true
        this._ee.on('closed', resolve)
      }
    })
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

    // whatever local changes we might have made get wiped out at this
    // point, having gotten a new value from the server.  With etags/versions
    // we could perhaps be more graceful about this.
    this.newLocalValue.delete(target)

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
    debug('PROXIES GET', this.proxiesById, proxy)
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
