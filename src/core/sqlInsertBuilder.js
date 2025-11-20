const fs = require('fs');
const path = require('path');
const { normalizeValueForColumn, escapeSqlValue } = require('./valueUtils');

function buildInsertStatements(tableName, schema, rows, options = {}) {
  const chunkSize = options.chunkSize || 250;
  const bulk = options.bulk !== false;
  const insertableColumns = schema.columns.filter((col) => !col.autoIncrement);
  const columnList = insertableColumns.map((c) => `\`${c.name}\``).join(', ');
  const statements = [];
  let total = 0;

  if (bulk) {
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      const valuesClause = chunk
        .map((row) => {
          const values = insertableColumns.map((col) =>
            escapeSqlValue(normalizeValueForColumn(row[col.rawName], col))
          );
          return `(${values.join(', ')})`;
        })
        .join(',\n');
      statements.push(`INSERT INTO \`${tableName}\` (${columnList}) VALUES\n${valuesClause};`);
      total += chunk.length;
    }
  } else {
    rows.forEach((row) => {
      const values = insertableColumns.map((col) =>
        escapeSqlValue(normalizeValueForColumn(row[col.rawName], col))
      );
      statements.push(`INSERT INTO \`${tableName}\` (${columnList}) VALUES (${values.join(', ')});`);
      total += 1;
    });
  }

  const sql = statements.join('\n\n');
  if (options.insertOutputPath) {
    const outDir = path.dirname(options.insertOutputPath);
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(options.insertOutputPath, sql, 'utf-8');
  }

  return { sql, rowsWritten: total };
}

module.exports = { buildInsertStatements };
