'use strict'

const test = require('tape')
const View = require('.')
const debug = require('debug')('testing')

test('simple add and appear', t => {
  t.plan(1)
  const v = new View('testing_table_live_view_1',
                     { dropTableFirst: true, createUsingSQL: `a text` })

  v.on('appear', obj => {
    t.equal(obj.a, 'Hello')
    v.close()
    t.end()
  })

  v.add({a: 'Hello'})
})

test(t => {
  t.plan(6)
  const v = new View('testing_table_live_view_1',
                     { dropTableFirst: true, createUsingSQL: `a text` })

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
      v.close()
      t.end()
    })

    // obj.a = 'Goodbye!'
    v.query("UPDATE testing_table_live_view_1 SET a='Goodbye!'")
  })

  v.add({a: 'Hello'})
})

test('watch between different views', t => {
  // t.plan(6)
  const v = new View('testing_table_live_view_1',
                     { dropTableFirst: true, createUsingSQL: `a text` })
  v.add({a: 'Hello'})

  v._ee.on('ready', () => { // make sure table is created, then
    const v2 = new View('testing_table_live_view_1')
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
        Promise.all([
          v.close(),
          v2.close()
        ]).then(() => {
          console.log('READY TO CALL t.end()')
          setTimeout(t.end.bind(t), 300)
          // t.end()
        })
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
  t.plan(2)
  const v = new View('testing_table_live_view_1',
                     { dropTableFirst: true, createUsingSQL: `a text` })
  v.add({a: 'Hello'})
  v.add({a: 'Hello SECOND'})
    .on('change', () => {
      const v2 = new View('testing_table_live_view_1')

      let counter = 0
      v2.on('appear', obj => {
        counter++
        if (obj.a === 'Hello' || obj.a === 'Hello SECOND') {
          t.pass()
        } else {
          t.fail()
        }
        if (counter >= 2) {
          v.close()
          v2.close()
          t.end()
        }
      })
    })
})

test('set', t => {
  t.plan(7)
  const v = new View('testing_table_live_view_1',
                     { dropTableFirst: true, createUsingSQL: `a text` })

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
      v.close()
      t.end()
    })

    obj.a = 'Goodbye!'
    t.equal(obj.a, 'Hello')  // still
    // v.query("UPDATE testing_table_live_view_1 SET a='Goodbye!'")
  })

  v.add({a: 'Hello'})
})

test('set with changeNow and delete', t => {
  t.plan(7)
  const v = new View('testing_table_live_view_1',
    { dropTableFirst: true,
      createUsingSQL: `a text`,
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
      v.close()
      t.end()
    })

    obj.a = 'Goodbye!'
    t.equal(obj.a, 'Goodbye!')  // because of changeNow
    // v.query("UPDATE testing_table_live_view_1 SET a='Goodbye!'")
  })

  v.add({a: 'Hello'})
})

test('add', t => {
  t.plan(3)
  const v = new View('testing_table_live_view_1',
                     { dropTableFirst: true, createUsingSQL: `a text` })

  v.on('appear', obj => {
    debug('appear', obj)
    t.equal(obj.a, 'Hello')
    v.close()
    t.end()
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
