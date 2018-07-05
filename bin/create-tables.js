const Program = require('commander')
const {execute} = require('../lib/execute')

Program
  .version('0.0.1')
  .option('-s, --specs [JSON string]', 'a JSON specification of which tables from which schemas should be included')
  .option('-f, --specs-file [path]', 'a path to a JSON specification file')

Program.parse(process.argv)

let {specs, specsFile} = Program

if (specs) {
  specs = JSON.parse(specs)
} else if (specsFile) {
  specs = require(specsFile)
} else {
  throw new Error('You must specify either the --specs or --specs-file option')
}

// const tableQuery = 
// `SELECT DISTINCT
// a.attname as name,
// format_type(a.atttypid, a.atttypmod) as type,
// -- a.attnotnull as notnull,
// coalesce(i.indisprimary,false) as primary_key,
// def.adsrc as default
// FROM pg_attribute a
// JOIN pg_class pgc ON pgc.oid = a.attrelid
// LEFT JOIN pg_index i ON
// (pgc.oid = i.indrelid AND i.indkey[0] = a.attnum)
// LEFT JOIN pg_description com on
// (pgc.oid = com.objoid AND a.attnum = com.objsubid)
// LEFT JOIN pg_attrdef def ON
// (a.attrelid = def.adrelid AND a.attnum = def.adnum)
// WHERE a.attnum > 0 AND pgc.oid = a.attrelid
// AND pg_table_is_visible(pgc.oid)
// AND NOT a.attisdropped
// AND pgc.relname = $1`

const tableQuery = 
`SELECT json_build_object(
  'columns', (
    SELECT json_object_agg(column_name, data_type)
    FROM information_schema.columns
    WHERE table_name = $1 AND table_schema = $2
  ),
  'primaryKey', json_build_object(
    'name', (
      SELECT constraint_name
      FROM information_schema.table_constraints
      WHERE table_name = $1 AND table_schema = $2 AND constraint_type = 'PRIMARY KEY'
    ),
    'columns', (
      SELECT json_agg(ccu.column_name)
      FROM information_schema.table_constraints tc
      JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name 
      WHERE tc.table_name = $1 AND tc.table_schema = $2 
    )
  )
) AS definition`