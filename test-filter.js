'use strict'

const test = require('tape')
const f = require('./filter')

test(t => {
  t.deepEqual(f.toList({name: {type: 'string'}}), [
    { op: 'type', property: 'name', type: 'string' }
  ])
  t.deepEqual(f.toList({
    name: {type: 'string'},
    age: {type: 'number'}
  }), [
    { op: 'type', property: 'age', type: 'number' },
    { op: 'type', property: 'name', type: 'string' }
  ])
  t.end()
})

test(t => {
  const l = [
    { op: 'type', property: 'age', type: 'number' },
    { op: 'type', property: 'name', type: 'string' }
  ]
  t.equal(f.toSQLCreate(l), 'age float, name text')
  t.end()
})
