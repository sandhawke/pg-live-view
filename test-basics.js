'use strict'

const test = require('tape')
const DB = require('.')
const debug = require('debug')('testing')

process.on('unhandledRejection', (reason, p) => {
  console.error(process.argv[1], 'Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit()
})
if (!process.env.PGPASSWORD) {
  console.error('\nno PGPASSWORD in environement\n\n')
  process.exit(1)
}

function dbv (sql) {
  const db = new DB({useTempDB: true})
  const v = db.view('testing_table_live_view_1', [], { createUsingSQL: `a text` })
  return [db, v]
}

test('start and stop', t => {
  t.plan(1)
  const [db] = dbv('a text')

  db.close().then(() => {
    t.ok(true)
    t.end()
  })
})

test('simple add and appear', t => {
  t.plan(1)
  const [db, v] = dbv('a text')

  v.on('appear', obj => {
    t.equal(obj.a, 'Hello')
    db.close().then(() => t.end())
  })

  v.add({a: 'Hello'})
})

test(t => {
  t.plan(6)
  const [db, v] = dbv('a text')

  v.on('appear', obj => {
    t.equal(obj.a, 'Hello')

    obj.on('change', (from, to) => {
      t.equal(from.id, obj.id)
      t.equal(to.id, obj.id)
      t.equal(from.a, 'Hello')
      t.equal(to.a, 'Goodbye!')

      // obj.delete()
      v.query('DELETE FROM testing_table_live_view_1')
    })

    obj.on('disappear', partial => {
      t.equal(partial.id, obj.id)
      db.close().then(() => t.end())
    })

    // obj.a = 'Goodbye!'
    v.query("UPDATE testing_table_live_view_1 SET a='Goodbye!'")
  })

  v.add({a: 'Hello'})
})

test('watch between different views', t => {
  // t.plan(6)
  const [db, v] = dbv('a text')

  v.add({a: 'Hello'})

  v._ee.on('connected', () => { // make sure table is created, then
    const v2 = db.view({}, {table: 'testing_table_live_view_1'})
    debug('v2', v2)

    v2.on('appear', obj => {
      t.equal(obj.a, 'Hello')   // WTF???   SOMETIMES THIS GETS 'Hello SECOND'

      obj.on('change', (from, to) => {
        t.equal(from.id, obj.id)
        t.equal(to.id, obj.id)
        t.equal(from.a, 'Hello')
        t.equal(to.a, 'Goodbye!')

        // obj.delete()
        v.query('DELETE FROM testing_table_live_view_1')
      })

      obj.on('disappear', partial => {
        t.equal(partial.id, obj.id)
        db.close().then(() => t.end())
      })

      // obj.a = 'Goodbye!'
      v.query("UPDATE testing_table_live_view_1 SET a='Goodbye!'")
    })
  })
})

test('sleep between tests', t => {
  setTimeout(t.end.bind(t), 0)
})

test('second view after some adds', t => {
  debug('9000')
  t.plan(2)
  const [db, v] = dbv('a text')

  v.add({a: 'Hello'})
  v.add({a: 'Hello SECOND'})
    .on('change', () => {
      const v2 = db.view({}, {table: 'testing_table_live_view_1'})

      let counter = 0
      v2.on('appear', obj => {
        counter++
        if (obj.a === 'Hello' || obj.a === 'Hello SECOND') {
          t.pass()
        } else {
          t.fail()
        }
        if (counter >= 2) {
          debug('closing db')
          db.close().then(() => t.end())
        }
      })
    })
})

test('set', t => {
  debug('9100')
  t.plan(7)
  const [db, v] = dbv('a text')

  v.on('appear', obj => {
    t.equal(obj.a, 'Hello')

    obj.on('change', (from, to) => {
      t.equal(from.id, obj.id)
      t.equal(to.id, obj.id)
      t.equal(from.a, 'Hello')
      t.equal(to.a, 'Goodbye!')

      // obj.delete()
      v.query('DELETE FROM testing_table_live_view_1')
    })

    obj.on('disappear', partial => {
      t.equal(partial.id, obj.id)
      db.close().then(() => t.end())
    })

    obj.a = 'Goodbye!'
    t.equal(obj.a, 'Hello')  // still
    // v.query("UPDATE testing_table_live_view_1 SET a='Goodbye!'")
  })

  v.add({a: 'Hello'})
})

test('set with changeNow and delete', t => {
  t.plan(7)
  const db = new DB({useTempDB: true})
  const v = db.view('testing_table_live_view_1', {},
    { createUsingSQL: `a text`,
      changeNow: true })

  v.on('appear', obj => {
    t.equal(obj.a, 'Hello')

    obj.on('change', (from, to) => {
      t.equal(from.id, obj.id)
      t.equal(to.id, obj.id)
      t.equal(from.a, 'Hello')
      t.equal(to.a, 'Goodbye!')

      // obj.delete()
      v.delete(obj.id)
      // v.query('DELETE FROM testing_table_live_view_1')
    })

    obj.on('disappear', partial => {
      t.equal(partial.id, obj.id)
      db.close().then(() => t.end())
    })

    obj.a = 'Goodbye!'
    t.equal(obj.a, 'Goodbye!')  // because of changeNow
    // v.query("UPDATE testing_table_live_view_1 SET a='Goodbye!'")
  })

  v.add({a: 'Hello'})
})

test('add', t => {
  t.plan(3)
  const [db, v] = dbv('a text')

  v.on('appear', obj => {
    debug('appear', obj)
    t.equal(obj.a, 'Hello')
    db.close().then(() => t.end())
  })

  v.add({a: 'Hello'})
    .on('id-assigned', id => {
      t.assert(true)
      debug('id assigned', id)
    })
    .on('change', (before, after) => {
      t.assert(true)
      debug('changed', before, after)
    })
})

test('lookup something in mem', t => {
  t.plan(2)
  const [db, v] = dbv('a text')

  v.on('appear', obj => {
    debug('appear', obj)
    t.equal(obj.a, 'Hello')

    v.lookup(obj.id)
      .then(obj2 => {
        t.equal(obj, obj2)
        db.close().then(() => t.end())
      })
  })
  v.add({a: 'Hello'})
})

test('lookup non in mem', t => {
  t.plan(3)
  const [db, v] = dbv('a text')

  v.on('appear', obj => {
    const v2 = db.view({}, {table: 'testing_table_live_view_1'})
    debug('v2', v2)

    v2.lookup(obj.id)
      .then(obj2 => {
        t.notEqual(obj, obj2)  // different views, different copies
        t.equal(obj.id, obj2.id)
        t.equal(obj.a, obj2.a)
        db.close().then(() => t.end())
      })
  })

  v.add({a: 'Hello'})
})

test('lookup not found', t => {
  t.plan(1)
  const [db, v] = dbv('a text')

  v.lookup(10000)
    .then(obj => {
      t.equal(obj, undefined)
      db.close().then(() => t.end())
    })
})
