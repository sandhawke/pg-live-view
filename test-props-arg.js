'use strict'

const test = require('tape')
const cpa = require('./props-arg')

test(t => {
  const a = 'a,b'
  const b = [['a', [['required', true]]],
             ['b', [['required', true]]]]
  t.deepEqual(cpa(a), b.entries())
  t.end()
})
