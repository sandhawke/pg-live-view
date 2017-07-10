'use strict'

const View = require('pg-live-view')

console.log(`Watching my_dogs table...  (ctl-C to stop)`)

const v = new View('my_dogs')

v.on('appear', dog => {
  console.log('Found record of a dog,', dog)

  dog.on('disappear', dog => {
    console.log('Dog record removed,', dog)
  })
  dog.on('change', (old, current) => {
    console.log('Dog record changed:\n', old, '\n => \n', current)
  })

})

v.on('stable', () => {
  console.log('Results stable, waiting for more changes')
})
