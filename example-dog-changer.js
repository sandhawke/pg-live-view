'use strict'

process.on('unhandledRejection', (reason, p) => {
  console.error(process.argv[1], 'Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit()
})


const View = require('pg-live-view')

const v = new View('name, age, weight', 'my_dogs')

const all = []
for (let x = 1; x < 10000; x++) {
  all.push(
    v.add({name: 'Fluffy #' +x , age: 10 * x, weight: 60})
      .then(f => {
        // console.log('Added', f)
      })
  )
}
Promise.all(all)
  .then(() => {
    v.close()
  })

// seems to run at about 300 per second.

// 3.3s for 1k, 34 seconds for 10k
// node is running about 10% CPU, postgress is running 1.7% per thread
// not clear what the bottleneck would be.  Maybe the LISTEN client needs
// a round-trip?
//
// The watcher can catch up via SELECT *much* faster.
//
// And if not running a watcher? ...
// takes just as long


// Done as
//  psql << COPY my_dogs (id, name, age, weight) FROM stdin;
// it takes   real	0m0.077s  for 10k lines !!!

// cray cray

/*
time psql < my-dogs-no-id.sql
COPY 999

real	0m0.070s
user	0m0.028s
sys	0m0.004s

time psql < my-dogs-no-id.sql
real	0m0.082s

And watcher catches up very fast.  So it's something about the insert.
*/

// TRY var pg = require('pg').native

// https://github.com/brianc/node-pg-copy-streams
//

