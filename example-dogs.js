'use strict'

const pgView = require('pg-live-view')

console.log('Watching for records of dogs')

const v = pgView('name, age, weight', 'my_dogs')

v.on('appear', dog => {
  console.log('Found record of a dog,', dog)
  dog.on('disappear', dog => {
    console.log('Dog record removed,', dog)
  })
  dog.on('change', (before, after) => {
    console.log('Dog record changed:\n', before, '\n => \n', after)
  })
})

setTimeout(() => {
  console.log('Okay, that was long enough.')
  v.close()
}, 5000)
