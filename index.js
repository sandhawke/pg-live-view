'use strict'

const canonicalizePropertiesArgument = require('./props-arg')

function view (props, tablename, opts) {
  props = canonicalizePropertiesArgument(props)
}

module.exports = view
