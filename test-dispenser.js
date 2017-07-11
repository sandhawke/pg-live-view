'use strict'

const test = require('tape')
const Dispenser = require('./pg-id-dispenser')
const pg = require('pg')

const seqName = 'dispenser_test_id_seq'

function dbReset () {
  const pool = new pg.Pool({max: 1})
  return (
    pool.query('DROP SEQUENCE ' + seqName)
      .then(() => {
        return pool.end()
      })
      .catch(e => {})
  )
}

test('one item', t => {
  dbReset().then(() => {
    const d = new Dispenser({seqName})

    d.next().then(id => {
      t.equal(id, 10000)
      d.close()
      t.end()
    })
  })
})

test('one item in each of two dispenser', t => {
  dbReset().then(() => {
    const d = new Dispenser({seqName})

    d.next().then(id => {
      const d2 = new Dispenser({seqName})

      d2.next().then(id2 => {
        t.equal(id + 10000, id2)
        d.close()
        d2.close()
        t.end()
      })
    })
  })
})

test('12 in seq with innerblocksize 3', t => {
  dbReset().then(() => {
    const d = new Dispenser({seqName, innerBlockSize: 3})

    const all = []
    for (let i = 0; i < 12; i++) {
      all.push(d.next())
    }
    Promise.all(all).then(res => {
      t.deepEqual(res, [ 10000,
        10001,
        10002,
        20000,
        20001,
        20002,
        30000,
        30001,
        30002,
        40000,
        40001,
        40002 ])
      d.close()
      t.end()
    })
  })
})

test('50k in sequence', t => {
  dbReset().then(() => {
    const d = new Dispenser({seqName})

    const all = []
    for (let i = 0; i < 50000; i++) {
      all.push(d.next())
    }
    Promise.all(all).then(res => {
      t.equal(res.length, 50000)
      let n = 10000
      for (let i = 0; i < 50000; i++) {
        if (res[i] !== n) t.fail()
        n++
      }
      t.pass()
      d.close()
      t.end()
    })
  })
})
