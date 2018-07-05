'use strict'

const {Context, Select, Raw} = require('./lib/sql-commands')
const {execute} = require('./lib/query')

const createMiddleware = (query, options = {}) => async function (req, res, next) {
  let context = createContext(req)
  try {
    let response = await execute(query.resolve(context), context.values)
    if (options.next) {
      req.sql = {response}
      next()
    } else {
      res.json(response.rows)
    }
  } catch (err) {
    if (options.next) {
      next(err)
    } else {
      res.status(400).json(err)
    }
  }
}

const createContext = (req = {}) => new Context({
  query: req.query,
  path: req.params,
  headers: req.headers,
  body: req.body
})

module.exports = {
  execute, createContext,
  //defineTables: tables => Promise.all(tables.map(resolveRelation)),
  get: (definition, options) => createMiddleware(new Select(definition), options),
  raw: (definition, options) => createMiddleware(new Raw(definition), options)
}