'use strict'

const _ = require('lodash')
const { execute } = require('./query.js')

const tables = {}

// class Relation {
//   constructor (name, columns) {
//     this.name = name
//     this.define(columns)
//     // execute(tableQuery, [name]).then(res => this.define(res.rows))
//   }
//   define (columns) {
//     if (!columns.length) {
//       throw `Relation '${this.name}' does not exist or does not contain any columns.`
//     }
//     let pkeys = [],
//       cols = {},
//       i, len, col
//     for (i = 0, len = columns.length; i < len; i++) {
//       col = columns[i]
//       if (col.primary_key) {
//         pkeys.push(col.name)
//       }
//       cols[col.name] = col.type
//     }
//     this.primary_key = pkeys
//     this.columns = cols
//   }
//   resolve () {return this.name}
//   getFields () {
//     return _.keys(this.columns)
//     // let fields = _.keys(this.columns)
//     // return fields.length ? `${alias}.${fields.join(`${alias}.`)}` : null
//   }
// }

class Relation {
  constructor (definition) {
    Object.assign(this, definition)
  }
}

const resolveRelation = table => {
  let currentRelation = tables[table]
  if (currentRelation) {
    if (currentRelation instanceof Promise) {
      return currentRelation
    } else {
      return Promise.resolve(currentRelation)
    }
  } else {
    return tables[table] = execute(tableQuery, [table]).then(res => {
      if (!res.rows.length) {
        return Promise.reject(`Relation '${table}' does not exist or does not contain any columns.`)
      }
      tables[table] = new Relation(table, res.rows)
      return Promise.resolve(tables[table])
    })
  }
}

module.exports = {
  Relation, resolveRelation
}