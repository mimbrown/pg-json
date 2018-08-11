'use strict'

const tableRegEx = /^(?:(?:(\w+)|"(.+)")\.)?(?:(\w+)|"(.+)")/i

module.exports = class Manager {
  constructor (data, connection) {
    data = Object.assign({}, data)
    data.$manager = this
    this.data = data
    this.connection = connection
  }
  safe (value) {
    let values = this.values
    values.push(value)
    return `$${values.length}`
  }
  // create (query) {
  //   // this.values = []
  //   // const ret = {
  //   //   sql: query.resolve(this),
  //   //   values: this.values
  //   // }
  //   // delete this.values
  //   // return ret
  //   // if (query instanceof queries.block) {
  //   //   return [query.resolve(this), this]
  //   // } else if (query instanceof queries.multi) {
  //   //   return [query.resolve(this), query]
  //   // } else {
  //   //   this.values = []
  //   //   return [query.resolve(this), this.values]
  //   // }
  // }
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
  clone (data) {
    let manager = new Manager(data || this.data, this.connection)
    if (this.client) {
      manager.client = this.client
    }
    return manager
  }
  createSubManager (data) {
    let manager = this.clone(data)
    manager.data.$parent = this.data
    return manager
  }
  async execute (query) {
    let {client} = this
    if (!client && !query.ignoreClient) {
      client = await this.connection.getClient(query, this)
      try {
        return await query.execute(this)
      } finally {
        delete this.client
        client.release()
      }
    } else {
      return query.execute(this)
    }
  }
  getRelation (table) {
    let result = tableRegEx.exec(table.trim())
    if (result) {
      let [, schema1, schema2, table1, table2] = result
      return this.connection.database.getRelation(table1 || table2, schema1 || schema2, this.searchPath)
      // let table = table1 || table2
      // let schema = schema1 || schema2
      // if (table && schema) {
      //   return this.connection.database.getRelation(table, schema)
      // } else if (table) {
      //   let searchPath = this.searchPath || ['public']
      //   let relation
      //   for (let i = 0, len = searchPath.length; i < len; i++) {
      //     relation = this.connection.database.getRelation(table, searchPath[i])
      //     if (relation) {
      //       return relation
      //     }
      //   }
      //   throw new Error(`Couldn't find table '${table}'`)
      // }
    } else {
      throw new Error(`Malformed table '${table}'`)
    }
  }
  setSearchPath (...searchPath) {
    this.searchPath = searchPath
    return this.client.query(`SET search_path TO ${searchPath.map(schema => `"${schema}"`)}`)
  }
}