'use strict'

process.on('unhandledRejection', (reason, p) => {
  console.error(process.argv[1], 'Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit()
})


const View = require('pg-live-view')

console.log(`Watching my_dogs table..`)

const v = new View('name, age, weight', 'my_dogs')

setTimeout( () => {
  const f = v.add({name: 'Fluffy', age:3000, weight: 6000})
  console.log('add returned', f)
  
  f.on('saved', () => {
    console.log('saved', f)
  })
  
  v.on('appear', dog => {
    console.log('Found record of a dog,', dog)
    
    dog.on('disappear', dog => {
      console.log('Dog record removed,', dog)
    })
    dog.on('change', (old, current) => {
      console.log('Dog record changed:\n', old, '\n => \n', current)
    })
    
  })
}, 1000)

