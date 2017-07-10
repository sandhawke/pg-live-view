'use strict'

const pg = require('pg')
let pool = new pg.Pool({})

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
    pool.query("INSERT INTO my_dogs (name, age, weight) VALUES ('Tsuzumi', 2, 61)")
    pool.query("INSERT INTO my_dogs (name, age, weight) VALUES ('Mako', 3, 400)")
    pool.query("INSERT INTO my_dogs (name, age, weight) VALUES ('Taiko', 3, 87) RETURNING ID")
      .then(res => {
        console.log('Taiko assigned id=' + res.rows[0].id)
      })
    pool.end()
    console.log('table my_dogs dropped and recreated, with a few rows')
  })
