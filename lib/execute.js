'use strict'

const _ = require('lodash')
const objectPath = require('object-path')
const { Pool } = require('pg')
const pool = new Pool()

function configureReturn (response, options = {}) {
  if (options.metadata) {
    return response
  } else {
    let rows = response.rows
    if (options.singleValue) {
      return rows[0] && rows[0][options.singleValue]
    } else if (options.singleRow) {
      return rows[0]
    }
    return rows
  }
}

async function executeSimple (query, values, options) {
  const client = await pool.connect()
  try {
    let response = await client.query(query, values)
    return configureReturn(response, options)
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
        response = await client.query.apply(client, executable)
        name = item.name || `q${i+1}`
        toReturn = item.return
        response = configureReturn(response, item.options)
        context.returned[name] = response
        if (toReturn) {
          objectPath.set(format, typeof toReturn === 'string' ? toReturn : name, response)
        }
      }
    }
    return format
  } catch (e) {
    client.query('ROLLBACK')
    throw e
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

module.exports = { execute }