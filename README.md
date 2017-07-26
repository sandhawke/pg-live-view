## Live PostgreSQL View

A convenient way to use postgres data from JavaScript.

Example assumes you've set up postgres and the right environment
variables, and a suitable table called my_dogs.

'''OBSOLETE INTERFACE; SEE TESTS'''

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

## Current Limitations

In the interest of simplicity, we have some limitations at the moment:

* every viewed table must have a column named 'id' which is a unique index. This is to make it very clear what's meant by a particular row being updated or deleted, steering clear of Ship of Theseus issues.  Obviously the name could be made a parameter and other generalization made, some day.
* every row must be serializable in JSON in under 8000 bytes.  This is so the row can be transmitted as the PostgreSQL Notification payload, avoiding the need for an extra round trip to get the payload in a subsequent query.  Could be an options some day.

## Benchmark

Todo: compare to in-memory message passing (or https://redis.io/topics/pubsub?)  over a restful or websockets api?


