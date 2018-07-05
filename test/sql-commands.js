const {Select, Incoming} = require('../lib/sql-commands')
const assert = require('assert')

describe('Queries', () => {
  describe('Select', () => {
    it('should match basic query', () => {
      let query = new Select({
        fields: ['id', 'string_col'],
        from: 'my_table'
      })
      let incoming = new Incoming({name: 'Bob'})
      assert.equal(query.resolve(incoming), 'SELECT id,string_col FROM my_table AS _t1')
    })
    it('should match where clause', () => {
      let query = new Select({
        fields: ['id', 'string_col'],
        from: 'my_table',
        where: ['name = {{value name}}']
      })
      let incoming = new Incoming({name: 'Bob'})
      assert.equal(query.resolve(incoming), 'SELECT id,string_col FROM my_table AS _t1 WHERE name = $1')
      assert.equal(incoming.values.length, 1)
      assert.equal(incoming.values[0], 'Bob')
    })
    it('should match no where clause', () => {
      let query = new Select({
        fields: ['id', 'string_col'],
        from: 'my_table',
        where: ['name = {{value name}}']
      })
      let incoming = new Incoming({nam: 'Bob'})
      assert.equal(query.resolve(incoming), 'SELECT id,string_col FROM my_table AS _t1')
      assert.equal(incoming.values.length, 0)
    })
    it('should match comma-join', () => {
      let query = new Select({
        fields: ['id', 'string_col'],
        from: ['my_table', 'your_table']
      })
      let incoming = new Incoming({name: 'Bob'})
      assert.equal(query.resolve(incoming), 'SELECT id,string_col FROM my_table AS _t1, your_table AS _t2')
    })
    it('should match join', () => {
      let query = new Select({
        from: [{
          table: 'my_table',
          alias: 'm_t',
          fields: ['id', 'my_name']
        }, {
          table: 'your_table',
          alias: 'y_t',
          fields: 'your_name',
          on: 'm_t.id = y_t.my_id'
        }]
      })
      let incoming = new Incoming({name: 'Bob'})
      assert.equal(query.resolve(incoming), 'SELECT m_t.id,m_t.my_name,y_t.your_name FROM my_table AS m_t JOIN your_table AS y_t ON m_t.id = y_t.my_id')
    })
  })
})