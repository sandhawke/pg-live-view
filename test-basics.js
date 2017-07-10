'use strict'

const test = require('tape')
const View = require('.')

test(t => {
  t.plan(1)
  const v = new View('testing_table_live_view_1',
                     { dropTableFirst: true, createUsingSQL: `a text` })

  v.on('appear', obj => {
    t.equal(obj.a, 'Hello')
    v.close()
    t.end()
  })
  setTimeout(() => {
    v.add({a: 'Hello'})
  }, 100)
})
