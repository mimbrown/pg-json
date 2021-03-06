'use strict'

const path = require('path')
const fs = require('fs')
const os = require('os')
const {execSync} = require('child_process')
const _ = require('lodash')
const uuid = require('uuid/v4')
const Handlebars = require('handlebars')
const objectPath = require('object-path')

const tmpDir = os.tmpdir()
const wordRegEx = /^\w+$/i
const joinRegEx = /^(?:natural\s+)?(?:(?:left|right|full|cross)\s+)?(?:(?:inner|outer)\s+)?join/i

const ensure = (definition, Class) => definition instanceof Class ? definition : new Class(definition)

const stringOrFn = item => {
  let type = typeof item
  return type === 'string' || type === 'function'
}

const compileStrings = item => {
  if (typeof item === 'string') {
    return item.includes('{{') ? Handlebars.compile(item, {
      noEscape: true,
      strict: true
    }) : item
  } else if (typeof item === 'object' && !(item instanceof Query)) {
    for (let k in item) {
      item[k] = compileStrings(item[k])
    }
  }
  return item
}

const resolve = (item, manager, join) => {
  if (!item) {
    return item
  } else if (typeof item === 'function') {
    manager.begin()
    try {
      item = item(manager.data)
      if (!item) {
        manager.rollback()
      }
      return item
    } catch (e) {
      manager.rollback()
      return null
    }
  } else if (typeof item.resolve === 'function') {
    return item.resolve(manager)
  } else if (Array.isArray(item)) {
    return resolveArray(item, manager, join)
  } else {
    return item
  }
}

const resolveArray = (array, manager, join = ',') => {
  let i = 0, len = array.length
  let str = '', toAdd
  for (; i < len; i++) {
    toAdd = resolve(array[i], manager)
    if (toAdd) {
      if (str) str += join
      str += toAdd
    }
  }
  return str
}

const resolveObject = (object, manager) => {
  let newObject = {}
  for (let k in object) {
    newObject[k] = resolve(object[k], manager)
  }
  return newObject
}

const createValues = (values, columns, defaults, manager) => `(${columns.map(column => column in values ? manager.safe(values[column]) : column in defaults ? defaults[column] : 'DEFAULT')})`

const createQuery = (definition, defaultType) => {
  if (definition instanceof Query) {
    return definition
  }
  if (stringOrFn(definition)) {
    return new Raw(definition)
  }
  let type = definition.qt || defaultType
  delete definition.qt
  type = type.toLowerCase()
  if (type in queries) {
    return new queries[type](definition)
  } else {
    throw new Error(`No query of type '${type}' detected.`)
  }
}

Handlebars.registerHelper('safe', function (value) {
  if (value === undefined) throw new Error('Missing value detected')
  return this.$manager.safe(value)
})

/**
 * A base class for all query-related classes
 */
class Query {
  constructor (definition) {
    if (!definition) {
      throw new Error('Queries require a definition to be instantiated')
    }
    this.definition = compileStrings(this.prepare ? this.prepare(definition) : definition)
  }
}

/**
 * For things like the WHERE and HAVING clauses
 */
class Logic extends Query {
  prepare (definition) {
    if (Array.isArray(definition)) {
      definition = {array: definition}
    } else if (!definition.array) {
      definition = {array: [definition]}
    }
    if (!definition.join) {
      definition.join = 'AND'
    }
    let join = definition.join.toLowerCase()
    definition.array.map(item => {
      if (typeof item === 'string') {
        return item
      } else if (Array.isArray(item)) {
        return {
          array: item,
          join: join === 'and' ? 'OR' : 'AND'
        }
      }
    })
    return definition
  }
  resolve (manager) {
    let { array, join } = this.definition
    return resolveArray(array, manager, ` ${join} `)
  }
}

/**
 * For defining tables (with schemas and aliases)
 */
class Table extends Query {
  resolve (manager) {
    let definition = this.definition
    if (stringOrFn(definition)) {
      return resolve(definition, manager)
    }
    let { table, schema, alias } = definition
    let sql = resolve(table, manager)
    if (schema) {
      sql = `${resolve(schema, manager)}.${sql}`
    }
    if (alias) {
      sql += ` AS ${alias}`
    }
    return sql
  }
}

/**
 * Like the Table class, but supports queries in the place of a table name
 */
class From extends Query {
  prepare (definition) {
    let table = definition.table
    if (typeof table === 'object') {
      definition.table = createQuery(table, 'select')
    }
    return definition
  }
  resolve (manager) {
    let definition = this.definition
    if (stringOrFn(definition)) {
      return resolve(definition, manager)
    }
    let { table, schema, alias } = definition
    let sql
    if (table) {
      table = resolve(table, manager)
      sql = wordRegEx.test(table) ? table : `(${table})`
      if (schema) {
        sql = `${resolve(schema, manager)}.${sql}`
      }
      if (alias) {
        sql += ` AS ${alias}`
      }
      return sql
    } else {
      throw new Error('Expected a table in the from statement, but no table was provided')
    }
  }
}

/**
 * Like the From class, but includes join options as well
 */
class Join extends From {
  resolve (manager) {
    let sql = super.resolve(manager)
    let definition = this.definition
    if (typeof definition === 'object') {
      let { type, on } = definition
      if (type === ',') {
        return `,${sql}`
      }
      if (on) {
        sql += ` ON ${on}`
      }
      return ` ${type} ${sql}`
    } else {
      sql = sql.trim()
      if (joinRegEx.test(sql)) {
        return ` ${sql}`
      } else {
        return `,${sql}`
      }
    }
  }
}

/**
 * For defining WITH clauses
 */
class WithClause extends Query {
  prepare (definition) {
    if (typeof definition === 'object') {
      if (!('queries' in definition)) {
        definition = {queries: definition}
      }
      let queries = definition.queries
      if (!Array.isArray(queries)) {
        definition.queries = queries = [queries]
      }
      definition.queries = queries.map(query => ensure(query, WithQuery))
    }
    return definition
  }
  resolve (manager) {
    let definition = this.definition
    if (stringOrFn(definition)) {
      return resolve(definition, manager)
    }
    let {queries, recursive} = definition
    let sql = recursive ? 'RECURSIVE ' : ''
    sql += resolveArray(queries, manager)
    return sql
  }
}

/**
 * A specific subquery of a WITH clause
 */
class WithQuery extends Query {
  prepare (definition) {
    if (typeof definition === 'object') {
      let query = definition.query
      if (query && typeof query === 'object') {
        definition.query = createQuery(query, 'select')
      }
    }
    return definition
  }
  resolve (manager) {
    let definition = this.definition
    if (stringOrFn(definition)) {
      return resolve(definition, manager)
    }
    let {alias, columns, query} = definition
    let sql = alias
    if (columns) {
      sql += ` (${resolve(columns, manager)})`
    }
    sql += ` AS (${resolve(query, manager)})`
    return sql
  }
}

/**
 * This is the base class for all normal queries
 */
class SingleQuery extends Query {
  constructor (def) {
    super(def)
    let {definition} = this
    this.returnOptions = definition.returnOptions
    delete definition.returnOptions
  }
  async execute (manager) {
    let {returnOptions = {}} = this
    let {client} = manager
    let values = manager.values = []
    let query = this.resolve(manager)
    delete manager.values
    let response = await client.query(query, values)
    let {metadata, singleKey, singleRow} = returnOptions
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
}

class ParallelQuery extends Query {
  constructor (definition) {
    super(definition)
    this.ignoreClient = true
  }
}

class FileQuery extends Query {
  constructor (definition) {
    super(definition)
    this.ignoreClient = true
  }
  async execute (manager) {
    let {template, file, transaction} = this.definition
    // file = path.join(workingDir, file)
    let dir = path.dirname(file)
    let command = 'psql'
    let {host, port, database, user} = manager.connection.pool.options
    let tempFile
    if (template) {
      tempFile = path.join(tmpDir, `temp-${uuid()}.sql`)
      fs.writeFileSync(tempFile, Handlebars.compile(fs.readFileSync(file).toString())(manager.data))
    }
    if (transaction) {
      command += ' -1'
    }
    if (host) {
      command += ` -h ${host}`
    }
    if (port) {
      command += ` -p ${port}`
    }
    if (database) {
      command += ` -d ${database}`
    }
    if (user) {
      command += ` -U ${user}`
    }
    command += ` -f ${tempFile || file}`
    try {
      return execSync(command, {
        cwd: dir
      }).toString('utf8')
    } finally {
      if (tempFile) {
        execSync(`rm ${tempFile}`)
      }
    }
  }
}

/**
 * Handles a single (maybe compilable) string definition of a query
 */
class Raw extends SingleQuery {
  resolve (manager) {
    return resolve(this.definition, manager)
  }
}

class Select extends SingleQuery {
  prepare (definition) {
    if (stringOrFn(definition)) {
      definition = {from: definition}
    }
    let {from, with: withClause, where, having} = definition
    if (withClause) {
      definition.with = ensure(withClause, WithClause)
    }
    if (from) {
      from = Array.isArray(from) ? from : [from]
      if (from.length === 0) {
        delete definition.from
      } else {
        definition.from = from.map((fromInstance, index) => ensure(fromInstance, index ? Join : From))
      }
    }
    if (where) {
      definition.where = ensure(where, Logic)
    }
    if (having) {
      definition.having = ensure(having, Logic)
    }
    return definition
  }
  resolve (manager) {
    let {from, with: withClause, distinct, fields, where, groupBy, having, orderBy, limit, offset} = this.definition
    let sql = 'SELECT'
    if (withClause) {
      sql = `WITH ${withClause.resolve(manager)} ${sql}`
    }
    if (distinct) {
      sql += ' DISTINCT'
      if (distinct !== true) {
        sql += ` ON (${distinct})`
      }
    }
    sql += ` ${resolve(fields, manager) || '*'}`
    if (from) {
      sql += ` FROM ${resolveArray(from, manager, '')}`
    }
    if (where) {
      where = where.resolve(manager)
      if (where) sql += ` WHERE ${where}`
    }
    if (groupBy) {
      groupBy = resolve(groupBy, manager)
      sql += ` GROUP BY ${groupBy}`
    }
    if (having) {
      having = having.resolve(manager)
      if (having) sql += ` HAVING ${having}`
    }
    if (orderBy) {
      orderBy = resolve(orderBy, manager)
      if (orderBy) sql += ` ORDER BY ${orderBy}`
    }
    if (limit) {
      limit = resolve(limit, manager)
      if (limit) sql += ` LIMIT ${limit}`
    }
    if (offset) {
      offset = resolve(offset, manager)
      if (offset) sql += ` OFFSET ${offset}`
    }
    return sql
  }
}

class Insert extends SingleQuery {
  prepare (definition) {
    if (stringOrFn(definition)) {
      definition = {into: definition}
    }
    let {into, with: withClause, values} = definition
    if (into) {
      definition.into = ensure(into, Table)
    }
    if (withClause) {
      definition.with = ensure(withClause, WithClause)
    }
    if (values && typeof values === 'object') {
      definition.values = createQuery(values, 'select')
    }
    return definition
  }
  resolve (manager) {
    let {into, columns, with: withClause, values = [], valuesPath, defaultValues = {}, onConflict, upsert, returning} = this.definition
    into = resolve(into, manager)
    let relation, valuesArray
    if (!columns || upsert) {
      relation = manager.getRelation(into)
    }
    let sql = `INSERT INTO ${into}`
    if (withClause) {
      sql = `WITH ${withClause.resolve(manager)} ${sql}`
    }
    if (!columns) {
      columns = _.keys(relation.columns)
    }
    sql += ` (${resolve(columns, manager)})`
    if (Array.isArray(values)) {
      values = values.map(value => `(${resolve(value, manager)})`)
    } else {
      values = [resolve(values, manager)]
    }
    if (typeof valuesPath === 'string') {
      valuesArray = objectPath.get(manager.data, valuesPath)
      if (typeof valuesArray !== 'object') throw new Error('Values must be an object')
      if (!Array.isArray(valuesArray)) {
        valuesArray = [valuesArray]
      }
      if (defaultValues) {
        defaultValues = resolveObject(defaultValues, manager)
      }
      values.push(valuesArray.map(values => createValues(values, columns, defaultValues, manager)).join())
    }
    values = values.join()
    if (values) {
      sql += ` VALUES ${values}`
    }
    if (onConflict) {
      onConflict = resolve(onConflict, manager)
      if (onConflict) {
        sql += ` ON CONFLICT ${onConflict}`
      }
    } else if (upsert && valuesArray) {
      if (valuesArray.length > 1) {
        throw new Error('Cannot upsert more than one row in a single query')
      }
      let primaryColumns = relation.primaryKey.columns
      let actualValues = valuesArray[0]
      sql += ` ON CONFLICT ON CONSTRAINT ${relation.primaryKey.name} DO UPDATE SET ${columns.filter(column => !primaryColumns.includes(column) && column in actualValues).map(column => `${column} = EXCLUDED.${column}`)}`
    }
    if (returning) {
      returning = resolve(returning, manager)
      if (returning) {
        sql += ` RETURNING ${returning}`
      }
    }
    return sql
  }
}

class Update extends SingleQuery {
  prepare (definition) {
    if (stringOrFn(definition)) {
      definition = {table: definition}
    }
    let {table, with: withClause, from, where} = definition
    if (table) {
      definition.table = ensure(table, Table)
    }
    if (withClause) {
      definition.with = ensure(withClause, WithClause)
    }
    if (from) {
      from = Array.isArray(from) ? from : [from]
      if (from.length === 0) {
        delete definition.from
      } else {
        definition.from = from.map((fromInstance, index) => ensure(fromInstance, index ? Join : From))
      }
    }
    if (where) {
      definition.where = ensure(where, Logic)
    }
    return definition
  }
  resolve (manager) {
    let {table, only, columns, columnMap = {}, with: withClause, valuesPath, defaultValues = {}, from, where, returning} = this.definition
    table = resolve(table, manager)
    let values
    let sql = 'UPDATE'
    if (only) {
      sql += ' ONLY'
    }
    sql += ` ${table}`
    if (withClause) {
      sql = `WITH ${withClause.resolve(manager)} ${sql}`
    }
    if (!columns) {
      columns = _.keys(manager.getRelation(table).columns)
    }
    if (typeof valuesPath === 'string') {
      values = objectPath.get(manager, valuesPath)
      if (typeof values !== 'object') throw new Error('Values must be an object')
      let validatedValues = {}
      let k, mapped
      for (k in values) {
        if (k in columnMap) {
          mapped = columnMap[k]
          if (columns.includes(mapped)) {
            validatedValues[mapped] = values[k]
          }
        } else if (columns.includes(k)) {
          validatedValues[k] = values[k]
        }
      }
      validatedValues = Object.assign({}, defaultValues, validatedValues)
      values = []
      for (k in validatedValues) {
        values.push(`${k} = ${manager.safe(validatedValues[k])}`)
      }
      sql += ` SET ${values}`
    }
    if (from) {
      sql += ` FROM ${resolveArray(from, manager, '')}`
    }
    if (where) {
      where = where.resolve(manager)
      if (where) sql += ` WHERE ${where}`
    }
    if (returning) {
      returning = resolve(returning, manager)
      if (returning) {
        sql += ` RETURNING ${returning}`
      }
    }
    return sql
  }
}

class Delete extends SingleQuery {
  prepare (definition) {
    if (stringOrFn(definition)) {
      definition = {from: definition}
    }
    let {from, with: withClause, using, where} = definition
    if (from) {
      definition.from = ensure(from, Table)
    }
    if (withClause) {
      definition.with = ensure(withClause, WithClause)
    }
    if (using) {
      using = Array.isArray(using) ? using : [using]
      if (using.length === 0) {
        delete definition.using
      } else {
        definition.using = using.map((usingInstance, index) => ensure(usingInstance, index ? Join : From))
      }
    }
    if (where) {
      definition.where = ensure(where, Logic)
    }
    return definition
  }
  resolve (manager) {
    let {from, only, with: withClause, using, where, returning} = this.definition
    let sql = 'DELETE FROM'
    if (only) {
      sql += ' ONLY'
    }
    sql += ` ${resolve(from, manager)}`
    if (withClause) {
      sql = `WITH ${withClause.resolve(manager)} ${sql}`
    }
    if (using) {
      sql += ` USING ${resolveArray(using, manager, '')}`
    }
    if (where) {
      where = where.resolve(manager)
      if (where) sql += ` WHERE ${where}`
    }
    if (returning) {
      returning = resolve(returning, manager)
      if (returning) {
        sql += ` RETURNING ${returning}`
      }
    }
    return sql
  }
}

class Multiple extends SingleQuery {
  prepare (definition) {
    if (Array.isArray(definition)) {
      definition = {queries: definition}
    }
    let queries = definition.queries
    if (queries) {
      definition.queries = queries.map(query => createQuery(query, 'select'))
    } else {
      throw new Error('\'queries\' is required for class Multiple')
    }
    return definition
  }
  resolve (manager) {
    let {type = 'UNION', queries} = this.definition
    return `(${resolveArray(queries, manager, `) ${type} (`)})`
  }
}

class Each extends ParallelQuery {
  prepare (definition) {
    definition.query = createQuery(definition.query, 'select')
    return definition
  }
  execute (manager) {
    let {path, query} = this.definition
    let array = objectPath.get(manager.data, path)
    if (!Array.isArray(array)) {
      throw new Error(`Object at path '${path}' is not an array`)
    }
    return Promise.all(array.map(item => {
      let subManager = manager.createSubManager(item)
      return subManager.execute(query)
    }))
  }
}

class Formatted extends ParallelQuery {
  prepare (definition) {
    this.queries = []
    return this.createShell(definition)
  }
  createShell (definition, path = []) {
    let temp
    if (typeof definition === 'object') {
      definition = _.clone(definition)
      if ('$query' in definition) {
        definition.query = createQuery(definition.$query, 'select')
        delete definition.$query
        definition.path = path.join('.')
        this.queries.push(definition)
        return undefined
      } else {
        for (let k in definition) {
          path.push(k)
          temp = this.createShell(definition[k], path)
          if (temp === undefined) {
            delete definition[k]
          } else {
            definition[k] = temp
          }
          path.pop()
        }
      }
    }
    return definition
  }
  async execute (manager) {
    let {queries, definition} = this
    definition = _.cloneDeep(definition)
    await Promise.all(queries.map(async def => {
      let {query, path} = def
      objectPath.set(definition, path, await manager.clone().execute(query))
    }))
    return definition
  }
}

class Series extends Query {
  prepare (definition) {
    definition.queries = definition.queries.map(item => {
      item = _.clone(item)
      item.query = createQuery(item.query, 'select')
      return item
    })
    return definition
  }
  async execute (manager) {
    let {queries, format, transaction} = this.definition
    let returned = manager.data.$returned = {}
    let {client} = manager
    let response, i, len
    try {
      format = format ? _.cloneDeep(format) : {}
      if (transaction) {
        await client.query('BEGIN')
      }
      for (i = 0, len = queries.length; i < len; i++) {
        let {name = `q${i+1}`, query, return: toReturn} = queries[i]
        response = await query.execute(manager)
        returned[name] = response
        if (toReturn) {
          objectPath.set(format, typeof toReturn === 'string' ? toReturn : name, response)
        }
      }
      if (transaction) {
        await client.query('COMMIT')
      }
      return format
    } catch (err) {
      if (transaction) {
        await client.query('ROLLBACK')
      }
      throw err
    } finally {
      delete manager.data.$returned
    }
  }
}

const queries = {
  createQuery,
  query: Query,
  select: Select,
  insert: Insert,
  update: Update,
  delete: Delete,
  multiple: Multiple,
  each: Each,
  formatted: Formatted,
  series: Series,
  file: FileQuery,
  single: SingleQuery
}

module.exports = queries