'use strict'

/*

  filter:
  {
    name: {type: 'string'},
    age: {type: 'number'}
  }

  filterList:

  [
    { op: 'type', property: 'age', type: 'number' },
    { op: 'type', property: 'name', type: 'string' }
  ]

  MAYBE filterList should be
  [
    ['type', 'age', 'number'],
    ['type', 'name', 'string']
  ]

*/

function toList (filter) {
  const out = []

  for (let key of Object.keys(filter).sort()) {
    const val = filter[key]
    if (val.type) {
      out.push({op: 'type', property: key, type: val.type})
    /* } else if (val.lessthan) {

     */
    } else {
      throw Error('unknown filter value for ' + JSON.string(key) +
                  ' => ' + JSON.string(val))
    }
  }

  return out
}

function toSQLCreate (list) {
  const parts = []
  for (let f of list) {
    if (f.op === 'type') {
      parts.push(f.property + ' ' + sqlType(f.type))
    }
  }
  return parts.join(', ')
}

function sqlType (type) {
  switch (type) {
    case 'string':
      return 'text'
    case 'number':
      return 'float'
    default:
      throw Error('no SQL type to match type: ' + JSON.stringify(type))
  }
}

module.exports.toList = toList
module.exports.toSQLCreate = toSQLCreate
