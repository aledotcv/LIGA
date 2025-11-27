const fs = require('fs');
const path = require('path');

/**
 genera stored procedure para inserción de datos
 */
function generateStoredProcedure(tableName, schema, options = {}) {
  const { outputPath } = options;
  
  const insertableColumns = schema.columns.filter((col) => !col.autoIncrement);
  const params = insertableColumns.map((col) => {
    const sqlType = col.sqlType || 'VARCHAR(255)';
    return `  IN p_${col.name} ${sqlType}`;
  }).join(',\n');

  const columnList = insertableColumns.map((c) => `\`${c.name}\``).join(', ');
  const valueList = insertableColumns.map((c) => `p_${c.name}`).join(', ');

  const procedureName = `sp_insert_${tableName}`;

  const sql = `-- Stored Procedure para inserción en ${tableName}
DELIMITER $$

DROP PROCEDURE IF EXISTS \`${procedureName}\`$$

CREATE PROCEDURE \`${procedureName}\`(
${params}
)
BEGIN
  INSERT INTO \`${tableName}\` (${columnList})
  VALUES (${valueList});
  
  -- Retornar ID del nuevo registro si existe auto_increment
  SELECT LAST_INSERT_ID() as id;
END$$

DELIMITER ;

-- Ejemplo de uso:
-- CALL ${procedureName}(${insertableColumns.map((c, i) => `'valor${i + 1}'`).join(', ')});
`;

  if (outputPath) {
    const outDir = path.dirname(outputPath);
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(outputPath, sql, 'utf-8');
  }

  return { procedureName, sql };
}

/**
 * genera stored procedure para actualización de datos
 */
function generateUpdateProcedure(tableName, schema, options = {}) {
  const { outputPath } = options;
  
  const pkColumns = schema.columns.filter((col) => col.isPrimaryKey);
  const updateableColumns = schema.columns.filter((col) => !col.isPrimaryKey && !col.autoIncrement);

  if (!pkColumns.length) {
    throw new Error(`No se puede generar UPDATE procedure sin PRIMARY KEY en tabla ${tableName}`);
  }

  const params = [
    ...pkColumns.map((col) => `  IN p_${col.name} ${col.sqlType}`),
    ...updateableColumns.map((col) => `  IN p_${col.name} ${col.sqlType}`),
  ].join(',\n');

  const setClause = updateableColumns
    .map((col) => `    \`${col.name}\` = p_${col.name}`)
    .join(',\n');

  const whereClause = pkColumns
    .map((col) => `\`${col.name}\` = p_${col.name}`)
    .join(' AND ');

  const procedureName = `sp_update_${tableName}`;

  const sql = `-- Stored Procedure para actualización en ${tableName}
DELIMITER $$

DROP PROCEDURE IF EXISTS \`${procedureName}\`$$

CREATE PROCEDURE \`${procedureName}\`(
${params}
)
BEGIN
  UPDATE \`${tableName}\`
  SET
${setClause}
  WHERE ${whereClause};
  
  -- Retornar número de filas afectadas
  SELECT ROW_COUNT() as affected_rows;
END$$

DELIMITER ;
`;

  if (outputPath) {
    const outDir = path.dirname(outputPath);
    fs.mkdirSync(outDir, { recursive: true });
    fs.appendFileSync(outputPath, '\n' + sql, 'utf-8');
  }

  return { procedureName, sql };
}

/**
 *genera stored procedure para eliminación de datos
 */
function generateDeleteProcedure(tableName, schema, options = {}) {
  const { outputPath } = options;
  
  const pkColumns = schema.columns.filter((col) => col.isPrimaryKey);

  if (!pkColumns.length) {
    throw new Error(`No se puede generar DELETE procedure sin PRIMARY KEY en tabla ${tableName}`);
  }

  const params = pkColumns.map((col) => `  IN p_${col.name} ${col.sqlType}`).join(',\n');
  const whereClause = pkColumns.map((col) => `\`${col.name}\` = p_${col.name}`).join(' AND ');

  const procedureName = `sp_delete_${tableName}`;

  const sql = `-- Stored Procedure para eliminación en ${tableName}
DELIMITER $$

DROP PROCEDURE IF EXISTS \`${procedureName}\`$$

CREATE PROCEDURE \`${procedureName}\`(
${params}
)
BEGIN
  DELETE FROM \`${tableName}\`
  WHERE ${whereClause};
  
  -- Retornar número de filas afectadas
  SELECT ROW_COUNT() as affected_rows;
END$$

DELIMITER ;
`;

  if (outputPath) {
    const outDir = path.dirname(outputPath);
    fs.mkdirSync(outDir, { recursive: true });
    fs.appendFileSync(outputPath, '\n' + sql, 'utf-8');
  }

  return { procedureName, sql };
}

/**
 *genera stored procedure para selección de datos
 */
function generateSelectProcedure(tableName, schema, options = {}) {
  const { outputPath } = options;
  
  const pkColumns = schema.columns.filter((col) => col.isPrimaryKey);
  const params = pkColumns.length
    ? pkColumns.map((col) => `  IN p_${col.name} ${col.sqlType}`).join(',\n')
    : '  -- Sin parámetros';

  const whereClause = pkColumns.length
    ? `  WHERE ${pkColumns.map((col) => `\`${col.name}\` = p_${col.name}`).join(' AND ')}`
    : '';

  const procedureName = `sp_select_${tableName}`;

  const sql = `-- Stored Procedure para selección en ${tableName}
DELIMITER $$

DROP PROCEDURE IF EXISTS \`${procedureName}\`$$

CREATE PROCEDURE \`${procedureName}\`(
${params}
)
BEGIN
  SELECT * FROM \`${tableName}\`
${whereClause};
END$$

DELIMITER ;
`;

  if (outputPath) {
    const outDir = path.dirname(outputPath);
    fs.mkdirSync(outDir, { recursive: true });
    fs.appendFileSync(outputPath, '\n' + sql, 'utf-8');
  }

  return { procedureName, sql };
}

/**
 *genera conjunto completo de stored procedures CRUD
 */
function generateCRUDProcedures(tableName, schema, options = {}) {
  const { outputPath } = options;
  
  const procedures = [];

  // INSERT
  const insertProc = generateStoredProcedure(tableName, schema, { outputPath });
  procedures.push(insertProc);

  // SELECT
  const selectProc = generateSelectProcedure(tableName, schema, { outputPath });
  procedures.push(selectProc);

  // UPDATE (si hay PK)
  const pkColumns = schema.columns.filter((col) => col.isPrimaryKey);
  if (pkColumns.length > 0) {
    const updateProc = generateUpdateProcedure(tableName, schema, { outputPath });
    procedures.push(updateProc);

    const deleteProc = generateDeleteProcedure(tableName, schema, { outputPath });
    procedures.push(deleteProc);
  }

  return procedures;
}

module.exports = {
  generateStoredProcedure,
  generateUpdateProcedure,
  generateDeleteProcedure,
  generateSelectProcedure,
  generateCRUDProcedures,
};
