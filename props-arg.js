'use strict'

/**
 * Turn a 'props' argument into a Map from property names
 * to objects which characterize and constrain what we're supposed to
 * do with those properties.
 *
 * Canonical form is like:
 *    props.set('age', [['lt', 37], ['gt', 0]])
 *
*/
function canonicalizePropertiesArgument (props) {
  let p
  if (props instanceof Map) {
    // make a copy because we're going to mess around with it
    p = new Map(props)
  }
  if (typeof props === 'string') {
    p = new Map()
    for (let key of props.split(/[, ]+/)) {
      p.set(key, {required: true})
    }
  } else if (typeof props === 'object' && !(props instanceof Map)) {
    p = new Map()
    for (let key of Object.keys(props)) {
      p.set(key, props[key])
    }
  }

  for (let [propname, ops] of p) {
    if (typeof ops === 'string' ||
        typeof ops === 'number' ||
        typeof ops === 'boolean') {
      ops = ['eq', ops]
    }
    if (typeof ops === 'object' && !Array.isArray(ops)) {
      const arrayForm = []
      for (let key of Object.keys(ops).sort()) {
        arrayForm.push([key, ops[key]])
      }
    }

    if (Array.isArray(ops)) {
      for (let [op, arg, ...extra] of ops) {
        const len = expectedLen => {
          if (expectedLen !== extra.length + 1) {
            throw Error(
              'incorrect number of args, ' + propname + ': ' + op)
          }
        }
        const typ = (item, type) => {
          //
        }
        switch (op) {
          case 'eq':
            len(1)
            typ(arg, 'scalar')
            break
          // some day we'll allow more than 'eq' !
          default:
            throw Error(
              'unrecognized property operator:' + propname + ': ' + op)
        }
      }
    } else {
      throw Error(
        'malformed properties operation set, ' + propname + ': ' + ops)
    }
  }
}

module.exports = canonicalizePropertiesArgument
