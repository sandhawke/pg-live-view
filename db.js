'use strict'

const pg = require('pg')
const debug = require('debug')('View')
const EventEmitter = require('eventemitter3')
const setdefault = require('./setdefault')
const IdDispenser = require('./pg-id-dispenser')
const View = require('./view')
const pgtemp = require('pg-temp-db')

class DB extends EventEmitter {
  constructor (opts) {
    super()
    Object.assign(this, opts)

    if (!this.pool) {
      if (this.useTempDB) {
        this.pool = new pgtemp.Pool()
      } else {
        this.pool = new pg.Pool({database: this.database})
      }
      this.endPoolOnClose = true
    }
    if (!this.dispenser) {
      this.dispenser = new IdDispenser({pool: this.pool})
    }
    if (!this.views) {
      this.views = {}
    }
  }

  view (...args) {
    let name
    if (typeof args[0] === 'string') {
      name = args.shift()
    } else {
      name = 'anonview' + this.views.length
    }
    let spec = args.shift() // run through Normalize
    let optOverride = args.shift()

    const opts = {
      db: this,
      pool: this.pool,
      dispenser: this.dispenser}
    Object.assign(opts, optOverride)

    const v = new View(name, spec, opts)
    this.views[name] = v

    return v
  }

  async close () {
    await Promise.all(this.views.map(v => v.close()))
    if (this.endPoolOnClose) {
      await this.pool.end()
    }
  }
}


module.exports = DB
