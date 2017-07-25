'use strict'

const pg = require('pg')
const debug = require('debug')('db')
const EventEmitter = require('eventemitter3')
// const setdefault = require('./setdefault')
const IdDispenser = require('./pg-id-dispenser')
const View = require('./view')
const pgtemp = require('pg-scratch')

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
      this.views = new Set()
    }
    this.anonCounter = 0
  }

  static scratch () {
    return new DB({useTempDB: true})
  }

  view (...args) {
    debug('.view called', args)
    let name
    if (typeof args[0] === 'string') {
      name = args.shift()
    } else {
      name = 'anonview_' + (++this.anonCounter)
    }
    let spec = args.shift() // run through Normalize
    let optOverride = args.shift()

    const opts = {
      db: this,
      pool: this.pool,
      dispenser: this.dispenser}
    Object.assign(opts, optOverride)

    // TEMP HACK until we're doing spec right
    if (typeof spec === 'string') {
      opts.createUsingSQL = spec
    }

    debug('.view normalized to', [name, spec, opts])
    const v = new View(name, spec, opts)
    this.views.add(v)
    debug('views now', this.views)

    return v
  }

  async close () {
    for (let v of this.views.values()) {
      await v.close()
    }
    if (this.endPoolOnClose) {
      await this.pool.end()
    }
  }
}

module.exports = DB
