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
      this.views = {}
    }
    this.anonCounter = 0
  }

  static scratch () {
    return new DB({useTempDB: true})
  }

  view (arg) {
    debug('.view called', arg)
    if (!arg.name) {
      arg.name = 'anonview_' + (++this.anonCounter)
    }

    const opts = {
      // db: this,    not needed these days
      pool: this.pool,
      dispenser: this.dispenser}
    Object.assign(opts, arg)

    // TEMP HACK until we're doing spec right
    if (typeof this.spec === 'string') {
      opts.createUsingSQL = this.spec
    }

    // debug('.view normalized to', opts)
    const v = new View(opts)
    if (this.views[v.name]) {
      throw Error('view name duplication: ' + JSON.stringify(v.name))
    }
    this.views[v.name] = v
    debug('views now', this.views)

    return v
  }

  async close () {
    await Promise.all(Object.values(this.views).map(v => v.close()))
    /*
    for (let v of Object.values(this.views)) {
      await v.close()
    }
    */
    if (this.endPoolOnClose) {
      await this.pool.end()
    }
  }
}

module.exports = DB
