'use strict'

const pg = require('pg')
const debug = require('debug')('View')
const EventEmitter = require('eventemitter3')
const setdefault = require('setdefault')
const IdDispenser = require('./pg-id-dispenser')
const filter = require('./filter')

// const canonicalizePropertiesArgument = require('./props-arg')
// const SQL = require('sql-template-strings')
// const QueryStream = require('pg-query-stream')

// This is a random number I generated; it must be different from
// what any other software is using, if it's also got postgres
// clients connecting to this server.  Anyone touch the same tables
// and triggers must use this lock around trigger function creation.
// See https://stackoverflow.com/questions/40525684/tuple-concurrently-updated-when-creating-functions-in-postgresql-pl-pgsql

const myLockId = 8872355600218495943

/*
 * View's this.state is always one of these values, and it only increments.
 *
 * this.connect() requests advancing to CONNECTED, if possible,
 *                and resolves as soon as that state is reached
 * this.close()   requests advancing to CLOSED, if possible,
 *                and resolves as soon as that state is reached
 *
 * CONNECTING and CLOSING might last a long time, given various
 * database shenanigans that might be going on.
 */
const INITIALIZING = 0
const INITIALIZED = 1  // here, when returning from constructor
const CONNECTING = 2
const CONNECTED = 3    // emits 'connected' on reaching this state
const CLOSING = 4
const CLOSED = 5       // emits 'closed' on reaching this state

function eventOccurs (emitter, event) {
  return new Promise((resolve, reject) => {
    emitter.once(event, resolve)
  })
}

let viewcount = 0

const allowedOptions = {
  name: 'a reference name for this view, needed for ref',
  table: 'sql table to use, if any (defaults to same as name)',
  pool: 'a pg connection pool, if you want us to use yours',
  dispenser: 'tool for creating new unique ids',
  database: `a pg database id, otherwise we'll use the environment`,
  changeNow: `if truthy, then x.p=v ; x.p will show v, even before
              it's been confirmed as in the database`,
  createIfMissing: `use the filter to figure out what table we need`,
  createUsingSQL: `create table if missing, using these columns
                   (we'll supply the id column)`,
  filter: `expression describing columns and allowed values`
}

class View {
  constructor (opts) {
    Object.keys(opts).forEach(key => {
      if (!allowedOptions[key]) throw Error('unknown option: ' + key)
    })
    Object.assign(this, opts)

    if (!this.viewid) {
      this.viewid = ++viewcount
    }
    this.debug = (...a) => debug(...a, '#' + this.viewid)

    this.setState(INITIALIZING)

    // instead of extending, to give us a little more control
    this._ee = new EventEmitter()

    if (this.dropTableFirst) {
      throw Error('view.dropTableFirst option has been removed')
    }

    if (!this.filterList && this.filter) {
      this.filterList = filter.toList(this.filter)
    }

    if (!this.createUsingSQL && this.createIfMissing && this.filterList) {
      this.createUsingSQL = filter.toSQLCreate(this.filterList)
    }

    if (!this.table) {
      this.table = this.name
    }

    if (!this.pool) {
      this.pool = new pg.Pool({database: this.database})
      this.endPoolOnClose = true
    }

    if (!this.dispenser) {
      this.dispenser = new IdDispenser({pool: this.pool})
      this.closeDispenser = true
    }

    // For each row we hold in memory, there's a Proxy and its Target
    //
    // We hand out the Proxy, and keep the data cached in the Target.
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
    this.dblock = false

    this.setState(INITIALIZED)

    // returns syncronously, of course, since it's a constructor
  }

  inspect () {
    return `view(${this.table} rows=${this.proxiesById.size})`
  }

  setState (newState) {
    debug('state change from', this.state, '===>', newState)
    if (newState <= this.state) {
      throw Error('internal error, invalid state transition: ' + this.state +
                  ' ==> ' + newState)
    }
    this.state = newState
  }

  proxyHandlerGet (target, name) {
    // debug('proxy get', target, JSON.stringify(name), typeof name)

    // Maybe instead of returning a real EE, give a subset that requires the
    // events be 'disappear' and 'change', to avoid typos ?

    if (name === 'on') {
      let ee = this.rowEE.get(target)
      if (ee === undefined) {
        ee = new EventEmitter()
        this.rowEE.set(target, ee)
        // debug('new ee for row', target, ee)
      } else {
        // debug('existing ee for row', target, ee)
      }
      // debug('MAKING this.rowEE', this.rowEE)
      // XX return ee.on.bind(ee)
      return (...args) => {
        // debug('row.on called with', args)
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

  async overlay (target, overlay) {
    for (let key of Object.keys(overlay)) {
      this.proxyHandlerSet(target, key, overlay[key])
    }
  }

  proxyHandlerSet (target, name, value, receiver) {
    // not sure what to do about receiver...
    this.debug('set', target, name, value, receiver)
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
    const q1 = `UPDATE ${this.table} SET ${setstr} WHERE id=${targref}`
    return this.query(q1, vals)
  }

  delete (id) {
    if (typeof id === 'object') {
      throw TypeError('view.delete() given an object, not obj.id')
    }
    const q1 = `DELETE FROM ${this.table} WHERE id=$1`
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
    const target = {} //  _newlyThIng: data }
    const proxy = new Proxy(target, this.handler)
    // addAsync will fill in the id soon, but not yet
    this.addAsync(data, target, proxy)
    return proxy
  }

  /*
   *
   * WARNING: what might happen before the id is set, or before the
   * INSERT is complete?  Like, what if you want to delete it?  Or
   * set it as the value of some other object?  Neither of those will
   * work well before having an id, I expect.
   */
  async addAsync (data, target, proxy) {
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

    const id = await this.dispenser.next()
    this.debug('id has been assigned', id)
    target.id = id
    this.proxiesById.set(id, proxy)
    this.debug('PROXIES SET id=', JSON.stringify(id), this.proxiesById)

    // probably not needed, but still, might be nice...
    this.debug('USING this.rowEE', this.rowEE)
    let ee = this.rowEE.get(target)
    if (ee) {
      if (this.state < CLOSING) ee.emit('id-assigned', id)
    } else {
      this.debug('no ee to get id-assigned event')
    }

    props.push('id')
    dollars.push('$' + counter++)
    values.push(id)

    const res = await this.query(
      `INSERT INTO ${this.table} (${props.join(', ')}) 
       VALUES (${dollars.join(', ')}) RETURNING *`, values)
    if (res === undefined) throw Error('view closed before INSERT could run')
    this.debug('creation INSERT returned row', res.rows[0])

    // Whenever the NOTIFY arrives, which migth be before or
    // after this point where the INSERT returns, it will
    // ignore the event because the id is already in proxiesById.
    Object.assign(target, res.rows[0])

    // we could look for conflicts and warn someone... Emit
    // a 'change-refused' event or something.
    this.newLocalValue.delete(target)

    ee = this.rowEE.get(target)
    if (ee) {
      this.debug('emit change', {}, proxy)
      if (this.state < CLOSING) ee.emit('change', {}, proxy)
    }
    this.debug('emitting appear')
    if (this.state < CLOSING) this._ee.emit('appear', proxy)
    if (this.state < CLOSING) this._ee.emit('stable')
  }

  async close () {
    this.debug('.close() called for ', this.table)
    if (this.state < CONNECTED) {
      this.debug('cant close until fully connected')
      await this.connect()
    }
    if (this.state === CLOSED) {
      this.debug('was already closed')
      return
    }
    if (this.state === CLOSING) {
      this.debug('already closing; we shall wait for other thread')
      await eventOccurs(this._ee, 'closed')
      this.debug('other thread completed closing, we can now resolve')
      return
    }
    this.setState(CLOSING)

    // some day maybe we'll bring back 'this.pleaseClose' as a flag
    // telling connect() to skip some of its work.  But that got
    // messy.

    // remove triggers/function?   Nah.

    if (this.closeDispenser) {
      this.dispenser.close()
    }

    if (this.releaseClient) {
      this.debug('releasing', this.table)
      this.releaseClient()
    }

    if (this.endPoolOnClose) {
      this.debug('calling pool.end')
      await this.pool.end()
      this.debug('pool end resolved')
    }

    this.setState(CLOSED)
    this._ee.emit('closed')
  }

  // I like to hide the eventemitter interface, for now at least, to
  // keep users to only using on/off, and catch for more errors.

  // we might want onWithReplay and/or onWithoutReplay to control whether
  // you see old stuff
  on (eventName, callback) {
    if (eventName in {appear: 1, stable: 1}) {
      this._ee.on(eventName, callback)
      this.connect()
        .then(() => {
          this.startQueryForPriorValues() // ignore returned promie
        })
    } else {
      throw Error('unknown event name: ' + JSON.stringify(eventName))
    }
  }

  off (...args) {
    return this._ee.off(...args)
  }

  async query (text, data) {
    this.debug('QUERY', text, data)
    if (this.state >= CLOSING) {
      this.debug('QUERY WHILE CLOSING')
      return undefined
    }
    await this.connect()
    const res = await this.pool.query(text, data)
    return res
  }

  handleUpdateEvent (newdata) {
    this.debug('update on', newdata.id, JSON.stringify(newdata))
    const id = newdata.id // in one version we overwrote .id later
    const proxy = this.proxiesById.get(id)
    const target = proxy._targetBehindProxy
    const ee = this.rowEE.get(target)
    let old
    if (ee) {
    // only save a "Before" copy if we have an ee
      old = Object.assign({}, target)
    }

    this.debug('database update from', target)
    this.debug('database update to  ', newdata)

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

    this.debug('database update left target as', target)
    this.debug('database returning old as     ', old)

    if (ee) {
      this.debug('emit change', old, proxy)
      if (this.state < CLOSING) ee.emit('change', old, proxy)
    } else {
      this.debug('no event emitter')
    }
  }

  /**
   * Resolves when connected, like connect, but doesn't try to make
   * the connection happen.  Passive.
   */
  async connected () {
    if (this.state === CONNECTED) return
    if (this.state <= CONNECTING) {
      this.debug('waiting to be connected')
      await eventOccurs(this._ee, 'connected')
      return
    }
    if (this.state > CONNECTED) {
      throw Error('already closing or closed')
    }
  }

  /**
   * Start the process of "connecting", which mostly means making sure
   * the database is set up so we can listen to table changes
   */
  async connect () {
    this.debug('.connect', this.table, 'state=', this.state)
    if (this.state >= CONNECTING) {
      await this.connected()
      return
    }
    if (this.state !== INITIALIZED) throw Error('internal error')
    this.setState(CONNECTING)
    this.debug('.connect CONNECTING', this.table, 'state=', this.state)

    // Pull one client out of the pool.  Use it in a transaction with
    // advisory locking, so we can cleanly set up the table and
    // trigger without interference, then use it for LISTEN, which
    // also needs a private client.

    const client = await this.pool.connect()
    this.releaseClient = () => {
      if (client.release) {
        client.release()
        this.debug('listen-client .release called')
      }
    }

    try {
      // not sure why, but we need to make the trigger function before
      // the transaction; otherwise, it's not visible in the transaction
      //
      //  BUT this is what we wanted the lock for!!!!

      await client.query('BEGIN')
      this.debug('transaction started, locking')
      //
      // for some reason this has stopped working, and now says
      //
      //   detail: 'Key (proname, proargtypes, pronamespace)=(live_view_notify, , 2200) already exists.',
      // duplicate key value violates unique constraint "pg_proc_proname_args_nsp_index"
      //
      await client.query(`SELECT pg_advisory_xact_lock(${myLockId})`)
      this.debug('got lock')

      await this.createTriggerFunction(client)
      console.log('**********************************   created tf')
      this.debug('did create trigger function')

      //
      //  Maybe we can do a process-wide lock?
      //
      // await sleep(5000)
      // await processLock()
      // this.debug('got process lock')
      // await sleep(5000)
      await this.createTableIfNeeded(client)
      // await sleep(5000)
      this.debug('did table')
      // await sleep(5000)
      // this.debug('did sleep')
      await this.createTrigger(client)
      this.debug('did trigger')
      await client.query('COMMIT')
      this.debug('did commit')
    } catch (e) {
      await client.query('ROLLBACK')
      throw e
    } finally {
      // processUnlock()
      // do not "client.release()" because we use it for LISTEN
    }

    client.on('notification', msg => this.notification(msg))
    const sql = `LISTEN ${this.table}_notify`
    await client.query(sql)
    this.debug('listening via', sql)

    if (this.state !== CONNECTING) throw Error('internal error')
    this.setState(CONNECTED)
    this._ee.emit('connected')
  }

  notification (msg) {
    this.debug('postgres notification received:', msg)
    const [op, data] = JSON.parse(msg.payload)
    if (op === 'INSERT') {
      this.appear(data)
    } else if (op === 'DELETE') {
      const proxy = this.proxiesById.get(data.id)
      if (proxy) {
        const target = proxy._targetBehindProxy
        const ee = this.rowEE.get(target)
        if (ee) {
          if (this.state < CLOSING) ee.emit('disappear', data)
        }
        this.rowEE.delete(target)
        this.proxiesById.delete(data.id)
      } else {
        console.error('DELETE for unknown id', data.id)
      }
    } else if (op === 'UPDATE') {
      this.handleUpdateEvent(data)
    } else {
      throw new Error('unexpected database change code')
    }
    // alas, we don't have a way to cluster a bunch of inserts, so
    // we need to do this after each one:
    if (this.state < CLOSING) this._ee.emit('stable')
  }

  async createTriggerFunction (conn) {
    const sql = `
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
    $$ LANGUAGE plpgsql`
    try {
      this.debug('creating live_view_notify', sql)
      await conn.query(sql)
      this.debug('created live_view_notify')
    } catch (e) {
      this.debug('error in creating function:', e)
    }
  }

  // In theory we could check to make sure the columns are right, but
  // maybe better to catch that in looking at the results rows anyway.
  async createTableIfNeeded (conn) {
    this.debug('createTableIfNeeded', this.createUsingSQL)
    if (this.createUsingSQL) {
      // do NOT sql-quote this, it's machine generated usually
      const query = (
        `CREATE TABLE IF NOT EXISTS ${this.table} (
           id serial primary key,
           ${this.createUsingSQL}
         )`)
      this.debug('trying to create table', query)
      const result = await conn.query(query)
      this.debug('creation result', result)
    }
  }

  async createTrigger (conn) {
    /* Actually, let's drop it first, just in case we changed how we
     * defined it.  If we didn't have our advisory lock, this would
     * open up a window to missing events in a second view of the same
     * table, BUT that's one of the reasons we do have the advisory
     * lock.
     *
     * Everyone using triggers named *_live_view needs to be using the
     * same advisory lock! */

    // do NOT sql-quote the table name.
    await conn.query(`DROP TRIGGER IF EXISTS ${this.table}_live_view
                      ON ${this.table} CASCADE`)

    await conn.query(`CREATE TRIGGER ${this.table}_live_view
                      AFTER INSERT OR UPDATE OR DELETE ON ${this.table}
                      FOR EACH ROW EXECUTE PROCEDURE live_view_notify(); `)

    /* if we're not going to drop it, then do this:

    catch (e) {
      if (e.code === '42710' && e.routine === 'CreateTrigger') {
        // ignore error "Trigger Already Exists"
      } else {
        throw e
      }

      */
  }

  async startQueryForPriorValues () {
    if (this.state !== CONNECTED) throw Error('internal error')

    // only run once
    if (this.startedQuery) return
    this.startedQuery = true

    // don't use this.query because we don't want this to invoke connect()
    const res = await this.pool.query(`SELECT * FROM ${this.table}`)
    if (res) {
      for (let row of res.rows) {
        this.appear(row)
      }
      if (this.state < CLOSING) this._ee.emit('stable')
    }
  }

  async lookup (id) {
    if (this.state >= CLOSING) {
      throw Error('lookup after close')
    }
    await this.connect()
    if (typeof id === 'string') {
      id = parseInt(id)
    }
    // we've probably already got it cached
    let proxy = this.proxiesById.get(id)
    // but if not, let's do a separate query, to be sure
    if (!proxy) {
      this.debug('LOOKUP NEEDS id=', JSON.stringify(id), proxy, this.proxiesById)
      const res = await this.query(
        `SELECT * FROM ${this.table} WHERE id = $1`, [id])
      if (res.rowCount === 1) {
        const row = res.rows[0]
        proxy = this.appear(row)
        // should we: ?? this._ee.emit('stable')
      } else {
        if (res.rowCount === 0) {
          return undefined
        }
        throw Error('multiple results from id query, id=' + id)
      }
    }
    this.debug('LOOKUP RETURNING FOR ID', id, proxy, this.proxiesById)
    return proxy
  }

/*  not using this for the moment,
    in trying to debug...
    startQuery () {
    this.pool.connect((err, client, done) => {
    if (err) throw err
    const q = new QueryStream(`SELECT * FROM ${this.table}`)
    const stream = client.query(q)
    //release the client when the stream is finished
    stream.on('end', () => {
    this.debug('***query stream ended')
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
    this.debug('generating APPEAR', data)
    let proxy = this.proxiesById.get(data.id)
    this.debug('PROXIES GET ID=', data.id, this.proxiesById, 'PROXY=', proxy)
    if (!proxy) {
      this.debug('new proxy needed for', data.id)
      proxy = new Proxy(data, this.handler)
      this.proxiesById.set(data.id, proxy)
      this.debug('generating APPEAR on proxy', proxy)
      if (this.state < CLOSING) this._ee.emit('appear', proxy)
      return proxy
    } else {
      this.debug('duplicate appear for ' + data.id)
      return proxy
    }
  }
}

module.exports = View

/*

These were used for some debugging...

function sleep (millis) {
  debug('sleeping', millis)
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      debug('...sleep done')
      resolve()
    }, millis)
  })
}

// simple semaphore, to workaround postgres problem

let locked = false
const processQueue = []

function processLock () {
  debug('trying to lock')
  if (locked) {
    debug('... on queue')
    return new Promise((resolve, reject) => {
      processQueue.push(resolve)
    })
  } else {
    debug('... locked')
    locked = true
    return Promise.resolve()
  }
}

function processUnlock () {
  debug('unlocking...')
  const head = processQueue.shift()
  if (head) {
    debug('new lock given to head of queue')
    head()
  } else {
    debug('unlocked')
    locked = false
  }
}

*/
