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
    } else if (val === true) {
      out.push({op: 'type', property: key, type: 'boolean'})
      out.push({op: 'eq', property: key, type: true})
    } else {
      throw Error('unknown filter value for ' + JSON.stringify(key) +
                  ' => ' + JSON.stringify(val))
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
  const joined = parts.join(', ')
  return joined
}

function sqlType (type) {
  switch (type) {
    case 'string':
      return 'text'
    case 'number':
      return 'float'
    case 'date':
      return 'timestamp with time zone'
    case 'integer':
      return 'integer'
    case 'boolean':
      return 'boolean'
    case 'id':
      return 'bigint'
    default:
      throw Error('no implemented SQL type to match type: ' + JSON.stringify(type))
  }
}

module.exports.toList = toList
module.exports.toSQLCreate = toSQLCreate
