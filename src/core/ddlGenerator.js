const fs = require('fs');
const path = require('path');
const { deriveTableName } = require('./utils');

function buildColumnDefinition(column) {
  let line = `  \`${column.name}\` ${column.sqlType}`;
  if (column.autoIncrement) {
    line += ' AUTO_INCREMENT';
  }
  line += column.nullable ? '' : ' NOT NULL';
  return line;
}

function generateDDL(tableName, schema, options = {}) {
  const effectiveTable = deriveTableName('', tableName);
  const columnLines = schema.columns.map(buildColumnDefinition);

  const uniqueCols = schema.columns.filter((col) => col.unique && !col.isPrimaryKey);
  const uniqueLines = uniqueCols.map(
    (col) => `  UNIQUE KEY \`uk_${col.name}\` (\`${col.name}\`)`
  );
  const pkLine = `  PRIMARY KEY (${schema.primaryKeys.map((name) => `\`${name}\``).join(', ')})`;

  const ddl = [
    `-- Auto-generado por Database Migration Toolkit`,
    `CREATE TABLE IF NOT EXISTS \`${effectiveTable}\` (`,
    [...columnLines, pkLine, ...uniqueLines].join(',\n'),
    `) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
  ].join('\n');

  if (options.ddlOutputPath) {
    const outputDir = path.dirname(options.ddlOutputPath);
    fs.mkdirSync(outputDir, { recursive: true });
    fs.writeFileSync(options.ddlOutputPath, ddl, 'utf-8');
  }

  return { tableName: effectiveTable, ddl };
}

module.exports = { generateDDL };
