'use strict'

const {Pool} = require('pg')
const Manager = require('./manager')

class Relation {
  constructor (definition) {
    Object.assign(this, definition)
  }
}

class Database {
  constructor (definitions = {}) {
    Object.assign(this, definitions)
    let schema, schemaName, table
    for (schemaName in definitions) {
      schema = definitions[schemaName]
      for (table in schema) {
        schema[table] = new Relation(schema[table])
      }
    }
  }
  findRelation (table, schema) {
    if (!(schema in this)) {
      throw new Error(`Couldn't find schema '${schema}'`)
    }
    schema = this[schema]
    if (!(table in schema)) {
      throw new Error(`Couldn't find table '${table}' in schema '${schema}'`)
    }
    return schema[table]
  }
  getRelation (table, schema, searchPath = ['public']) {
    if (schema) {
      return this.findRelation(table, schema)
    } else {
      let relation
      let i = 0, len = searchPath.length
      for (; i < len; i++) {
        relation = this.findRelation(table, searchPath[i])
        if (relation) {
          return relation
        }
      }
      throw new Error(`Unable to find table '${table}'`)
    }
  }
}

/**
 * A connection class for connecting to the database.
 *
 * @class Connection
 */
class Connection {
  /**
   * Creates an instance of Connection.
   * @param {Object} [options={}] Specifies the options for this connection.
   * @param {(Object|string)} [options.database] A definition of a database or a path pointing to one
   * @param {Object} [options.connectionOptions] The connection options to pass on to the `pg` pool
   * @param {Object} [options.hooks] Functions to hook into key times in the query process
   * @memberof Connection
   * @constructor
   */
  constructor ({connectionOptions, database, hooks = {}} = {}) {
    if (typeof database === 'string') {
      database = require(database)
    }
    // this.connectionOptions = connectionOptions
    this.hooks = hooks
    this.pool = new Pool(connectionOptions)
    this.database = new Database(database)
    // this.execute = new Execute(poolOptions)
  }

  async getClient (query, manager) {
    let {clientInit} = this.hooks
    let client = manager.client = await this.pool.connect()
    if (clientInit) {
      try {
        await clientInit(client, query, manager)
      } catch (err) {
        delete manager.client
        client.release()
        throw err
      }
    }
    return client
  }

  /**
   * Creates a Manager instance associated with this connection.
   * @param {Object} [data] The data this manager ought to contain
   * @returns {Manager} The manager instance
   * @memberof Connection
   */
  createManager (data) {
    return new Manager(data, this)
  }

  /**
   * Ends the pool associated with this connection.
   * @param {*} [options] The options to pass to the Pool.end() function
   * @returns
   * @memberof Connection
   */
  end (options) {
    return this.pool.end(options)
  }

  /**
   * Executes a query with a manager.
   * @param {Query} query The query instance
   * @param {(Manager|Object)} manager The manager instance or a set of data to create one with
   * @returns {Promise}
   * @memberof Connection
   */
  execute (query, manager) {
    if (!(manager instanceof Manager)) {
      manager = this.createManager(manager)
    }
    return manager.execute(query)
  }
}

module.exports = Connection