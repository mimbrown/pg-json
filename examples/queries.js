const Connection = require('../lib/sql-commands')
const definitions = require('./definitions.json')
let connection

definitions.forEach((definition, outerIndex) => {
  connection = new Connection(definition.database)
  definition.examples.forEach((example, innerIndex) => {
    console.log(`\n------- Example ${outerIndex+1}.${innerIndex+1}`)
    let query = connection.createQuery(example.query, 'select')
    let context = new connection.Context(example.incoming)
    let created = context.$create(query)
    if (query instanceof connection.Each) {
      // console.log(created)
      created[0].forEach((executable, index) => {
        console.log(`---------- Query ${index+1}`)
        console.log(`QUERY:  ${executable[0]}`)
        console.log(`VALUES: ${executable[1]}`)
      })
    } else {
      console.log(`QUERY:  ${created[0]}`)
      console.log(`VALUES: ${created[1]}`)
    }
  })
})
console.log()