'use strict'

const pg = require('pg')
const debug = require('debug')('IdDispenser')

class Dispenser {
  constructor (opts) {
    Object.assign(this, opts)
    if (!this.pool) {
      this.pool = new pg.Pool({database: this.database, max: 3})
      this.endPoolOnClose = true
    }
    if (!this.seqName) {
      this.seqName = 'client_assigned_id_seq'
    }
    if (!this.blockSize) {
      this.blockSize = 10000
    }
    if (!this.innerBlockSize) {
      this.innerBlockSize = this.blockSize
    }

    this.nextId = 0
    this.maxId = -1
    this.postponed = []

    this.create = `CREATE SEQUENCE ${this.seqName} 
                   INCREMENT BY ${this.blockSize} START ${this.blockSize}`
    this.fetch = `SELECT nextval('${this.seqName}')`
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
   *
   * call this pg-serial ?  pg-fast-serial ?
   */
  next () {
    debug(`next()  nextid=${this.nextId}, maxId=${this.maxId}`)
    return new Promise((resolve, reject) => {
      if (this.nextId <= this.maxId) {
        const id = this.nextId
        this.nextId++
        resolve(id)
        return
      }
      debug('need to wait for a fetch')
      this.postponed.push([resolve, reject])
      if (this.fetching) {
        debug('already being done')
      } else {
        this.fetching = true
        debug('doing it!')
        this.getBlock()
      }
    })
  }

  getBlock () {
    debug('getblock')
    this.pool.query(this.fetch)
      .then(res => {
        debug('fetch result', res)
        const id = parseInt(res.rows[0].nextval)
        this.nextId = id
        this.maxId = id + this.innerBlockSize - 1
        debug('postponed:', this.postponed.length)
        while (this.postponed.length) {
          debug(`a postponed, nextid=${this.nextId}, maxId=${this.maxId}`)
          if (this.nextId <= this.maxId) {
            const [resolve] = this.postponed.shift()
            const id = this.nextId
            this.nextId++
            debug('resolving as', id)
            resolve(id)
          } else {
            debug('we still have postponed requests, but the block is empty')
            process.nextTick(this.getBlock.bind(this))
            return
          }
        }
      })
      .catch(e => {
        debug('fetch error', e)
        if (e.code === '42P01') {
          if (this.triedCreatingSequence) {
            debug('second failure, aboring')
            throw e
          }
          this.triedCreatingSequence = true
          debug('no such sequence; lets try creating it')
          this.pool.query(this.create)
            .then(debug.bind('creation result:'))
            .then(this.getBlock.bind(this))
        } else {
          console.log('err', JSON.stringify(e))
          throw e
        }
      })
  }

  close () {
    if (this.endPoolOnClose) {
      debug('calling pool.end', this.pool.end)
      return (this.pool.end()
              .then(() => {
                debug('pool end resolved')
              })
      )
    } else {
      return Promise.resolve()
    }
  }
}

let main
function obtain () {
  if (!main) {
    main = new Dispenser()
  }
  return main
}
Dispenser.obtain = obtain

module.exports = Dispenser
