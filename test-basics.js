'use strict'

const test = require('tape')
const View = require('.')

test(t => {
  t.plan(1)
  const v = new View('testing_table_live_view_1',
                     { dropAndCreateUsingSQL: `a test`} )

  v.on('appear', obj => {
    t.equal(obj.a, 'Hello')
    t.end()
  })
  v.add({a: 'Hello'})
})

