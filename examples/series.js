require('dotenv').config()
const Connection = require('../lib/sql-commands')
const {execute} = require('../lib/execute')
let connection = new Connection({
  public: {
    client: {
      columns: {
        id: 'integer',
        string_col: 'text',
        date_col: 'timestamp(0) with time zone',
        json_col: 'json',
        array_col: 'text[]',
        int_col: 'integer',
        the_geom: 'geometry'
      },
      privateKey: {
        name: 'test_pkey',
        columns: [
          'id'
        ]
      }
    }
  }
})

let query = connection.createQuery({
  qt: 'series',
  transaction: true,
  format: {
    test: 17
  },
  queries: [
    {
      query: {
        fields: 'FLOOR(COUNT(*)/2) AS cnt',
        from: 'test'
      },
      options: {
        singleValue: 'cnt'
      },
      name: 'half',
      return: 'metadata.numRows'
    },
    {
      query: {
        from: 'test',
        limit: '{{returned.half}}'
      },
      name: 'rows',
      return: true
    }
  ]
})

let context = new connection.Context()

execute.apply(null, context.$create(query))
  .then(response => console.log(response))
  .catch(err => console.error(err))