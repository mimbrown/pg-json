// 'use strict'

// const {Context, Select, Raw} = require('./lib/sql-commands')
// const Execute = require('./lib/query')

// const createMiddleware = (query, options = {}) => async function (req, res, next) {
//   let context = createContext(req)
//   try {
//     let response = await execute(query.resolve(context), context.values)
//     if (options.next) {
//       req.sql = {response}
//       next()
//     } else {
//       res.json(response.rows)
//     }
//   } catch (err) {
//     if (options.next) {
//       next(err)
//     } else {
//       res.status(400).json(err)
//     }
//   }
// }

// const createContext = (req = {}) => new Context({
//   query: req.query,
//   path: req.params,
//   headers: req.headers,
//   body: req.body
// })

// module.exports = {
//   execute, createContext,
//   //defineTables: tables => Promise.all(tables.map(resolveRelation)),
//   get: (definition, options) => createMiddleware(new Select(definition), options),
//   raw: (definition, options) => createMiddleware(new Raw(definition), options)
// }

const fs = require('fs')
const queries = require('./lib/sql-commands')

const tableRegEx = /^(?:(?:(\w+)|"(.+)")\.)?(?:(\w+)|"(.+)")/i

class Relation {
  constructor (definition) {
    Object.assign(this, definition)
  }
}

class Database {
  constructor (definitions) {
    Object.assign(this, definitions)
    let schema, schemaName, table
    for (schemaName in definitions) {
      schema = definitions[schemaName]
      for (table in schema) {
        schema[table] = new Relation(schema[table])
      }
    }
  }
  getRelation (table, schema) {
    return this[schema][table]
  }
}

class Connection {
  constructor (definitions) {
    if (typeof definitions === 'string') {
      definitions = fs.readFileSync(definitions)
    }
    const database = this.database = new Database(definitions)

    this.Manager = class Manager {
      constructor (data) {
        data = Object.assign({}, data)
        data.$manager = this
        this.data = data
      }
      value (value) {
        let values = this.values
        values.push(value)
        return `$${values.length}`
      }
      create (query) {
        if (query instanceof queries.block) {
          return [query.resolve(this), this]
        } else if (query instanceof queries.multi) {
          return [query.resolve(this), query]
        } else {
          this.values = []
          return [query.resolve(this), this.values]
        }
      }
      begin () {
        this.count = this.values.length
      }
      rollback () {
        let {count, values} = this
        let len = values.length
        if (len > count) {
          values.splice(count, len - count)
        }
      }
      createSubManager (data) {
        let manager = new Manager(data)
        manager.data.parent = this.data
        return manager
      }
      getRelation (table) {
        let result = tableRegEx.exec(table.trim())
        if (result) {
          let [, schema1, schema2, table1, table2] = result
          return database.getRelation(table1 || table2, schema1 || schema2)
        } else {
          throw new Error(`Malformed table '${table}'`)
        }
      }
    }
  }
}

module.exports = Connection