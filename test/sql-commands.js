/* eslint-env node, mocha */

const path = require('path')
require('dotenv').config({path: path.join(__dirname, '.env')})
const {createQuery, Connection} = require('../index')
const assert = require('assert')
const tests = require('./tests.json')

const connection = new Connection({
  database: path.join(__dirname, 'database.json'),
  hooks: {
    clientInit: (client, query, manager) => manager.setSearchPath('temp_json_test_schema')
  }
})

after(() => {
  connection.end()
})

describe('Setup', () => {
  it('should setup correctly', async () => {
    try {
      let message = await connection.execute(createQuery({
        qt: 'file',
        file: path.join(__dirname, 'queries/setup.sql')
      }))
      assert.equal(message, 'BEGIN\nCREATE SCHEMA\nSET\nCREATE TABLE\nCREATE TABLE\nCOMMIT\n')
    } catch (err) {
      assert.fail(err.message)
    }
  })
})

tests.forEach(({name, tests}) => {
  describe(name, () => {
    tests.forEach(({description, query, data, returns}, index) => {
      it(description, async () => {
        try {
          let response = await connection.execute(createQuery(query), data)
          assert.deepEqual(response, returns)
        } catch (err) {
          assert.fail(err.message)
        }
      })
    })
  })
})

describe('Teardown', () => {
  it('should teardown correctly', async () => {
    try {
      let message = await connection.execute(createQuery({
        qt: 'file',
        file: path.join(__dirname, 'queries/teardown.sql')
      }))
      assert.equal(message, 'BEGIN\nDROP SCHEMA\nCOMMIT\n')
    } catch (err) {
      assert.fail(err.message)
    }
  })
})