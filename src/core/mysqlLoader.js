const mysql = require('mysql2/promise');
const { normalizeValueForColumn } = require('./valueUtils');

async function insertChunk({
  connection,
  tableName,
  columns,
  chunk,
  offset,
  continueOnError,
  onProgress,
  errors,
}) {
  const placeholders = `(${columns.map(() => '?').join(', ')})`;
  const sql = `INSERT INTO \`${tableName}\` (${columns.map((c) => `\`${c.name}\``).join(', ')}) VALUES ${new Array(chunk.length)
    .fill(placeholders)
    .join(', ')}`;

  const params = [];
  chunk.forEach((row) => {
    columns.forEach((col) => {
      params.push(normalizeValueForColumn(row[col.rawName], col));
    });
  });

  try {
    await connection.query(sql, params);
    if (onProgress) onProgress(chunk.length);
    return chunk.length;
  } catch (err) {
    if (!continueOnError) throw err;
    // Fallback a inserción fila por fila para aislar errores
    let success = 0;
    for (let idx = 0; idx < chunk.length; idx += 1) {
      const row = chunk[idx];
      const singleSql = `INSERT INTO \`${tableName}\` (${columns.map((c) => `\`${c.name}\``).join(', ')}) VALUES (${placeholders})`;
      const rowParams = columns.map((col) => normalizeValueForColumn(row[col.rawName], col));
      try {
        await connection.query(singleSql, rowParams);
        success += 1;
        if (onProgress) onProgress(1);
      } catch (rowErr) {
        errors.push({ index: offset + idx, message: rowErr.message, row });
      }
    }
    return success;
  }
}

async function loadIntoMySQL({
  connectionConfig,
  ddl,
  tableName,
  schema,
  rows,
  bulk = true,
  chunkSize = 250,
  dryRun = false,
  continueOnError = true,
  onProgress,
}) {
  const insertableColumns = schema.columns.filter((col) => !col.autoIncrement);
  const errors = [];

  if (dryRun) {
    return { inserted: 0, errors };
  }

  const connection = await mysql.createConnection({
    multipleStatements: true,
    ...connectionConfig,
  });

  try {
    await connection.query(ddl);
    await connection.beginTransaction();

    let inserted = 0;
    if (bulk) {
      for (let i = 0; i < rows.length; i += chunkSize) {
        const chunk = rows.slice(i, i + chunkSize);
        const success = await insertChunk({
          connection,
          tableName,
          columns: insertableColumns,
          chunk,
          offset: i,
          continueOnError,
          onProgress,
          errors,
        });
        inserted += success;
      }
    } else {
      for (let i = 0; i < rows.length; i += 1) {
        const row = rows[i];
        try {
          await insertChunk({
            connection,
            tableName,
            columns: insertableColumns,
            chunk: [row],
            offset: i,
            continueOnError,
            onProgress,
            errors,
          });
          inserted += 1;
        } catch (err) {
          if (!continueOnError) throw err;
          errors.push({ index: i, message: err.message, row });
        }
      }
    }

    if (!continueOnError && errors.length) {
      await connection.rollback();
      return { inserted: 0, errors };
    }

    await connection.commit();
    return { inserted, errors };
  } catch (err) {
    await connection.rollback();
    throw err;
  } finally {
    await connection.end();
  }
}

module.exports = { loadIntoMySQL };
