'use strict'

process.on('unhandledRejection', (reason, p) => {
  console.error(process.argv[1], 'Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit()
})

const View = require('pg-live-view')

const seconds = 5
console.log(`Watching my_dogs table ${seconds} seconds`)

setupDatabaseForExample().then(() => {
  const v = new View('name, age, weight', 'my_dogs')

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
  }, seconds * 1000)
})

function setupDatabaseForExample () {
  const pg = require('pg')
  let pool = new pg.Pool({})

  setTimeout(() => {
    console.log('dog 2')
    pool.query("INSERT INTO my_dogs VALUES (2, 'Tsuzumi', 2, 61)")
  }, 0.1 * seconds * 1000)

  setTimeout(() => {
    console.log('dog 3')
    pool.query("INSERT INTO my_dogs VALUES (4, 'Mako', 3, 400)")
  }, 0.2 * seconds * 1000)

  setTimeout(() => {
    console.log('dog 4')
    pool.query('UPDATE my_dogs SET age=3 WHERE id=2')
  }, 0.3 * seconds * 1000)

  setTimeout(() => {
    console.log('dog 5')
    pool.query('DELETE FROM my_dogs WHERE id=1')
  }, 0.4 * seconds * 1000)

  setTimeout(() => {
    console.log('CLOSING setup pool.')
    pool.end()
    pool = {
      query: () => null
    }
  }, seconds * 1000)

  return (
    pool.query(`DROP TABLE IF EXISTS my_dogs`)
      .then(() => {
        return pool.query(`
          CREATE TABLE IF NOT EXISTS my_dogs (
            id serial primary key, 
            name varchar, 
            age int, 
            weight float
          )`)
      })
      .then(() => {
        return pool.query("INSERT INTO my_dogs (name, age, weight) VALUES ('Taiko', 3, 87)")
      })
  )
}
