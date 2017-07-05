## Live PostgreSQL View

A convenient way to use postgres data from JavaScript.

Example assumes you've set up postgres and the right environment
variables, and a suitable table called my_dogs.

```js
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
```

The `appear` event will occur on the view whenever a suitable row is
found, either because the initial query is proceeding, or because the
database changed.

The `change` and `disappear` events will occur when those things
happen to the row.

