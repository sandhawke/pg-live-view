'use strict'

const test = require('tape')
const setdefault = require('./setdefault')

test(t => {
  const a = {}
  setdefault(a, 'p1', 1)
  t.equal(a.p1, 1)
  t.end()
})

test(t => {
  const a = {}
  t.deepEqual(a.p1, undefined)
  setdefault(a, 'p1', []).push(10)
  t.deepEqual(a.p1, [10])
  setdefault(a, 'p1', []).push(20)
  t.deepEqual(a.p1, [10, 20])
  t.end()
})

test(t => {
  const a = new Map()
  setdefault(a, 'p1', 1)
  t.equal(a.get('p1'), 1)
  t.end()
})

test(t => {
  const a = new Map()
  t.deepEqual(a.get('p1'), undefined)
  setdefault(a, 'p1', []).push(10)
  t.deepEqual(a.get('p1'), [10])
  setdefault(a, 'p1', []).push(20)
  t.deepEqual(a.get('p1'), [10, 20])
  t.end()
})
