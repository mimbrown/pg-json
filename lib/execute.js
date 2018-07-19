'use strict'

const _ = require('lodash')
const objectPath = require('object-path')
const { Pool } = require('pg')

async function doExecute (client, query, values, options = {}) {
  let response = await client.query(query, values)
  let {metadata, singleKey, singleRow} = options
  if (metadata) {
    return response
  } else {
    let rows = response.rows
    if (singleKey) {
      rows = rows.map(row => row[singleKey])
    }
    return singleRow ? rows[0] : rows
  }
}

class Execute {
  constructor (poolOptions) {
    const pool = new Pool(poolOptions)

    async function executeSimple (query, values, options) {
      const client = await pool.connect()
      try {
        return doExecute(client, query, values, options)
      } finally {
        client.release()
      }
    }
    
    async function executeParallel (executables) {
      return Promise.all(executables.map(executable => execute.apply(null, executable)))
    }
    
    async function executeSeries (query, context) {
      const client = await pool.connect()
      try {
        let {queries, format} = query.definition
        let item, executable, response, name, toReturn, i, len
        format = format ? _.cloneDeep(format) : {}
        for (i = 0, len = queries.length; i < len; i++) {
          item = queries[i]
          if (typeof item === 'string') {
            await client.query(item)
          } else {
            executable = context.$create(item.query)
            executable.unshift(client)
            executable.push(item.options)
            response = await doExecute.apply(null, executable)
            name = item.name || `q${i+1}`
            toReturn = item.return
            context.returned[name] = response
            if (toReturn) {
              objectPath.set(format, typeof toReturn === 'string' ? toReturn : name, response)
            }
          }
        }
        return format
      } catch (err) {
        client.query('ROLLBACK')
        throw err
      } finally {
        client.release()
      }
    }
    
    async function execute (first, second, third) {
      if (typeof first === 'string') { // Simple query
        return executeSimple(first, second, third)
      } else if (Array.isArray(first)) { // Parallel queries
        let response = await executeParallel(first)
        return second.format ? second.format(response) : response
      } else { // Series of queries
        return executeSeries(first, second)
      }
    }

    this.executeSimple = executeSimple
    this.executeParallel = executeParallel
    this.executeSeries = executeSeries
    this.execute = execute
    this.pool = pool
  }
}

module.exports = Execute