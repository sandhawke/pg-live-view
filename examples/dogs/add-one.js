'use strict'

const View = require('pg-live-view')

const v = new View('my_dogs')

v.add({name: 'Fluffy', age: 10, weight: 60})
  .then(f => {
    console.log('Added', f)
    v.close()
  })
  .catch(e => {
    console.error(e)
    process.exit(1)
  })

/*   WHY DOES THIS SOMETIMES GET A DUPLICATE KEY ERROR?!?!   */
