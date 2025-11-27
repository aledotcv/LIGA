const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const archiver = require('archiver');

async function writeCSV(rows, outputPath, options = {}) {
  const { compress = false } = options;
  
  if (!rows || !rows.length) {
    throw new Error('No hay datos para exportar a CSV');
  }

  const columns = Object.keys(rows[0]);
  const lines = [];
  
  lines.push(columns.map(col => escapeCSVField(col)).join(','));
  
  rows.forEach(row => {
    const values = columns.map(col => {
      const value = row[col];
      return escapeCSVField(value);
    });
    lines.push(values.join(','));
  });

  const csvContent = lines.join('\r\n') + '\r\n';

  const { dataPath, zipPath, entryName } = compress
    ? resolveCompressionPaths(outputPath, '.csv')
    : { dataPath: outputPath, zipPath: null, entryName: null };
  const actualPath = dataPath;
  const outDir = path.dirname(actualPath);
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(actualPath, csvContent, 'utf-8');

  if (compress) {
    await compressFile(actualPath, zipPath, entryName);
    fs.unlinkSync(actualPath);
    return zipPath;
  }

  return actualPath;
}

function escapeCSVField(value) {
  if (value === null || value === undefined) {
    return '';
  }
  
  let str = String(value);
  
  str = str.replace(/\r\n/g, ' ').replace(/\r/g, ' ').replace(/\n/g, ' ');
  
  if (str.includes('"') || str.includes(',') || str.includes('\r') || str.includes('\n')) {
    str = '"' + str.replace(/"/g, '""') + '"';
  } else {
    str = '"' + str + '"';
  }
  
  return str;
}

/**
 escribe filas a un archivo JSON
 */
async function writeJSON(rows, outputPath, options = {}) {
  const { compress = false, pretty = true } = options;

  const jsonContent = pretty
    ? JSON.stringify(rows, null, 2)
    : JSON.stringify(rows);

  const { dataPath, zipPath, entryName } = compress
    ? resolveCompressionPaths(outputPath, '.json')
    : { dataPath: outputPath, zipPath: null, entryName: null };
  const actualPath = dataPath;
  const outDir = path.dirname(actualPath);
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(actualPath, jsonContent, 'utf-8');

  if (compress) {
    await compressFile(actualPath, zipPath, entryName);
    fs.unlinkSync(actualPath);
    return zipPath;
  }

  return actualPath;
}

/**
 escribe filas a un archivo XML
 */
async function writeXML(rows, rootElementName, outputPath, options = {}) {
  const { compress = false } = options;

  const xmlLines = ['<?xml version="1.0" encoding="UTF-8"?>'];
  xmlLines.push(`<${rootElementName}>`);

  rows.forEach((row) => {
    xmlLines.push('  <record>');
    Object.entries(row).forEach(([key, value]) => {
      const escapedValue = escapeXML(value);
      xmlLines.push(`    <${key}>${escapedValue}</${key}>`);
    });
    xmlLines.push('  </record>');
  });

  xmlLines.push(`</${rootElementName}>`);
  const xmlContent = xmlLines.join('\n');

  const { dataPath, zipPath, entryName } = compress
    ? resolveCompressionPaths(outputPath, '.xml')
    : { dataPath: outputPath, zipPath: null, entryName: null };
  const actualPath = dataPath;
  const outDir = path.dirname(actualPath);
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(actualPath, xmlContent, 'utf-8');

  if (compress) {
    await compressFile(actualPath, zipPath, entryName);
    fs.unlinkSync(actualPath);
    return zipPath;
  }

  return actualPath;
}

/**
 comprime un archivo a ZIP
 */
async function compressFile(inputPath, outputPath, entryName) {
  return new Promise((resolve, reject) => {
    const outDir = path.dirname(outputPath);
    fs.mkdirSync(outDir, { recursive: true });
    const output = fs.createWriteStream(outputPath);
    const archive = archiver('zip', { zlib: { level: 9 } });

    output.on('close', () => resolve(outputPath));
    archive.on('error', (err) => reject(err));

    archive.pipe(output);
    archive.file(inputPath, { name: entryName });
    archive.finalize();
  });
}

function resolveCompressionPaths(outputPath, defaultExt) {
  const normalizedExt = defaultExt.startsWith('.') ? defaultExt : `.${defaultExt}`;
  const parsed = path.parse(outputPath);
  const isZipTarget = parsed.ext.toLowerCase() === '.zip';

  const entryName =
    !parsed.ext || isZipTarget
      ? `${parsed.name}${normalizedExt}`
      : parsed.base;

  const zipPath = isZipTarget
    ? outputPath
    : path.join(parsed.dir, `${parsed.name || parsed.base}.zip`);

  const baseForData =
    !parsed.ext || isZipTarget
      ? parsed.name || parsed.base
      : parsed.name;

  const dataPath = path.join(parsed.dir, `${baseForData}_temp${normalizedExt}`);

  return { dataPath, zipPath, entryName };
}

/**
 escapa caracteres especiales para XML
 */
function escapeXML(value) {
  if (value === null || value === undefined) return '';
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

/**
 exporta tabla de MySQL al formato especificado
 */
async function exportFromMySQL(options, logger = console) {
  const {
    dbConfig,
    sourceType = 'table',
    tableName,
    sqlPath,
    format = 'csv',
    outputPath,
    compress = false,
  } = options;

  let connection;
  try {
    connection = await mysql.createConnection({
      ...dbConfig,
      multipleStatements: sourceType === 'sql',
    });
  } catch (connErr) {
    if (sourceType === 'sql') {
      logger.warn(`No se pudo conectar a la base de datos (${connErr.message}). Intentando procesar el archivo SQL localmente...`);
    } else {
      throw connErr;
    }
  }

  try {
    let rows = [];
    let sourceName;

    if (sourceType === 'sql') {
      if (!sqlPath) throw new Error('Ruta de archivo SQL no proporcionada');
      if (!fs.existsSync(sqlPath)) throw new Error(`Archivo SQL no encontrado: ${sqlPath}`);

      logger.info(`Leyendo consulta de: ${sqlPath}`);
      const sqlContent = fs.readFileSync(sqlPath, 'utf-8');
      const targetTable = tableName ? tableName.trim() : '';

      if (connection) {
        logger.info('Ejecutando consulta SQL...');
        
        let result;
        try {
          [result] = await connection.query(sqlContent);
        } catch (execErr) {
          logger.warn(`La ejecución SQL falló: ${execErr.message}. Intentando analizar como dump...`);
          result = [];
        }

        if (Array.isArray(result)) {
          if (result.length > 0 && Array.isArray(result[0])) {
            const candidate = result.find((r) => Array.isArray(r) && r.length > 0);
            rows = candidate || result.find((r) => Array.isArray(r)) || [];
          } else {
            rows = [];
          }
        }
      }

      if (rows.length === 0) {
        logger.info('No se obtuvieron resultados de la ejecución (o no hay conexión). Analizando archivo en busca de INSERTs...');
        if (!targetTable) {
          throw new Error('Especifica la tabla que se debe exportar del archivo SQL.');
        }
        rows = parseInsertStatements(sqlContent, targetTable);
        if (rows.length > 0) {
          logger.info(`Se extrajeron ${rows.length} registros de la tabla ${targetTable}.`);
        }
      }
      
      sourceName = (tableName && tableName.trim()) || path.basename(sqlPath, '.sql') || 'export';
    } else {
      // tabla
      if (!connection) throw new Error('No hay conexión a la base de datos.');
      if (!tableName) throw new Error('Nombre de tabla no proporcionado');

      const [tables] = await connection.query(
        'SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?',
        [dbConfig.database, tableName]
      );

      if (!tables.length) {
        throw new Error(`Tabla '${tableName}' no encontrada en la base de datos '${dbConfig.database}'`);
      }

      logger.info(`Exportando tabla '${tableName}'...`);
      const [result] = await connection.query(`SELECT * FROM \`${tableName}\``);
      rows = result;
      sourceName = tableName;
    }

    if (!rows || !rows.length) {
      throw new Error(`El origen de datos (${sourceName}) está vacío o no devolvió registros.`);
    }

    const preparedRows = rows.map(normalizeRowForExport);

    logger.info(`Procesando ${preparedRows.length} registros a formato ${format.toUpperCase()}...`);

    let resultPath;
    const formatLower = format.toLowerCase();

    switch (formatLower) {
      case 'csv':
        resultPath = await writeCSV(preparedRows, outputPath, { compress });
        break;
      case 'json':
        resultPath = await writeJSON(preparedRows, outputPath, { compress, pretty: true });
        break;
      case 'xml':
        resultPath = await writeXML(preparedRows, sourceName, outputPath, { compress });
        break;
      default:
        throw new Error(`Formato de exportación no soportado: ${format}`);
    }

    logger.info(`Exportación completada: ${resultPath}`);
    return resultPath;
  } finally {
    if (connection) await connection.end();
  }
}

/**
 lista todas las tablas en la base de datos
 */
async function listMySQLTables(dbConfig) {
  const connection = await mysql.createConnection(dbConfig);
  
  try {
    const [tables] = await connection.query(
      'SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?',
      [dbConfig.database]
    );
    
    return tables.map(t => t.TABLE_NAME);
  } finally {
    await connection.end();
  }
}

async function listTablesFromSqlFile(sqlPath) {
  const fs = require('fs');
  if (!fs.existsSync(sqlPath)) {
    throw new Error(`Archivo SQL no encontrado: ${sqlPath}`);
  }
  
  const sqlContent = fs.readFileSync(sqlPath, 'utf-8');
  const tables = new Set();
  
  const createRegex = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:`[^`]+`|"[^"]+"|\w+)\.)?(`[^`]+`|"[^"]+"|\w+)/gi;
  let match;
  while ((match = createRegex.exec(sqlContent)) !== null) {
    const tableName = stripIdentifier(match[1]);
    if (tableName) {
      tables.add(tableName);
    }
  }
  
  const insertRegex = /INSERT\s+INTO\s+(?:(?:`[^`]+`|"[^"]+"|\w+)\.)?(`[^`]+`|"[^"]+"|\w+)/gi;
  while ((match = insertRegex.exec(sqlContent)) !== null) {
    const tableName = stripIdentifier(match[1]);
    if (tableName) {
      tables.add(tableName);
    }
  }
  
  return Array.from(tables).sort();
}

function extractTableDefinitions(sqlContent) {
  const tableColumns = new Map();
  const createRegex = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(`[^`]+`|"[^"]+"|\w+)\.)?(`[^`]+`|"[^"]+"|\w+)\s*\(([\s\S]*?)\)\s*(?:ENGINE|COMMENT|PARTITION|DEFAULT|;)/gi;

  let match;
  while ((match = createRegex.exec(sqlContent)) !== null) {
    const schema = stripIdentifier(match[1]);
    const table = stripIdentifier(match[2]);
    const body = match[3];
    const key = buildTableKey(schema, table);

    const columns = [];
    const lines = body.split(/,\s*\n/);
    lines.forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || /^(PRIMARY|UNIQUE|KEY|CONSTRAINT|FOREIGN|INDEX)\b/i.test(trimmed)) {
        return;
      }
      const colMatch = trimmed.match(/^[`"]?([\w$]+)[`"]?\s+/);
      if (colMatch) {
        columns.push(colMatch[1]);
      }
    });

    if (columns.length) {
      tableColumns.set(key, columns);
      const bareKey = buildTableKey(null, table);
      if (!tableColumns.has(bareKey)) {
        tableColumns.set(bareKey, columns);
      }
    }
  }

  return tableColumns;
}

function stripIdentifier(identifier) {
  if (!identifier) return null;
  return identifier.replace(/^`|`$/g, '').replace(/^"|"$/g, '');
}

function buildTableKey(schema, table) {
  const tablePart = (table || '').toLowerCase();
  if (schema) {
    return `${schema.toLowerCase()}.${tablePart}`;
  }
  return tablePart;
}

function normalizeTableTarget(name) {
  if (!name) return null;
  const trimmed = name.trim();
  if (!trimmed) return null;
  const parts = trimmed.split('.');
  if (parts.length === 2) {
    const schema = stripIdentifier(parts[0]).toLowerCase();
    const table = stripIdentifier(parts[1]).toLowerCase();
    return { full: `${schema}.${table}`, simple: table };
  }
  const tableOnly = stripIdentifier(trimmed).toLowerCase();
  return { full: tableOnly, simple: tableOnly };
}

function tableMatchesTarget(key, tableName, target) {
  if (!target) return true;
  const tableLower = (tableName || '').toLowerCase();
  if (target.full.includes('.')) {
    if (key === target.full) {
      return true;
    }
  }
  return tableLower === target.simple;
}

function parseColumnList(rawList) {
  if (!rawList) return null;
  return rawList
    .replace(/^\(/, '')
    .replace(/\)$/, '')
    .split(',')
    .map((col) => stripIdentifier(col.trim()))
    .filter(Boolean);
}

function splitValueTuples(valuesPart) {
  const tuples = [];
  let current = '';
  let parenDepth = 0;
  let inQuote = false;
  let escape = false;

  for (let i = 0; i < valuesPart.length; i++) {
    const char = valuesPart[i];
    current += char;

    if (escape) {
      escape = false;
      continue;
    }

    if (char === '\\') {
      escape = true;
      continue;
    }

    if (char === "'") {
      inQuote = !inQuote;
      continue;
    }

    if (!inQuote) {
      if (char === '(') {
        parenDepth += 1;
        continue;
      }
      if (char === ')') {
        parenDepth -= 1;
        if (parenDepth === 0) {
          tuples.push(current.trim());
          current = '';
        }
        continue;
      }
      if (char === ',' && parenDepth === 0) {
        current = '';
        continue;
      }
    }
  }

  if (current.trim()) {
    tuples.push(current.trim());
  }

  return tuples;
}

/**
 parsea sentencias INSERT INTO ... VALUES ... de un contenido SQL
best-effort
 */
function parseInsertStatements(sqlContent, targetTableName) {
  const rows = [];
  const tableColumns = extractTableDefinitions(sqlContent);
  const target = normalizeTableTarget(targetTableName);
  const insertRegex = /INSERT\s+INTO\s+(?:[`"']?(\w+)[`"']?\.)?[`"']?(\w+)[`"']?\s*(\((?:[^)(]|\([^)(]*\))*\))?\s*VALUES\s*([\s\S]+?);/gi;

  let match;
  while ((match = insertRegex.exec(sqlContent)) !== null) {
    const schema = match[1];
    const table = match[2];
    const columnsRaw = match[3];
    const valuesPart = match[4];

    const key = buildTableKey(schema, table);
    if (!tableMatchesTarget(key, table, target)) {
      continue;
    }

    let columnNames = parseColumnList(columnsRaw);
    if (columnNames && columnNames.length) {
      if (!tableColumns.has(key)) {
        tableColumns.set(key, columnNames);
      }
      const bareKey = buildTableKey(null, table);
      if (!tableColumns.has(bareKey)) {
        tableColumns.set(bareKey, columnNames);
      }
    } else {
      columnNames = tableColumns.get(key) || tableColumns.get(buildTableKey(null, table)) || null;
    }

    const records = splitValueTuples(valuesPart);

    records.forEach((record) => {
      const cleanRecord = record.replace(/^\s*\(/, '').replace(/\)\s*$/, '');
      const values = parseSQLValues(cleanRecord);
      if (!values.length) {
        return;
      }
      const row = {};
      values.forEach((val, i) => {
        const colName = columnNames && columnNames[i] ? columnNames[i] : `col_${i + 1}`;
        row[colName] = val;
      });
      rows.push(row);
    });
  }
  return rows;
}

function parseSQLValues(recordStr) {
  const values = [];
  let currentVal = '';
  let inQuote = false;
  let escape = false;
  
  for (let i = 0; i < recordStr.length; i++) {
    const char = recordStr[i];
    
    if (escape) {
      currentVal += char;
      escape = false;
      continue;
    }
    
    if (char === '\\') {
      escape = true;
      continue;
    }
    
    if (char === "'" && !escape) {
      inQuote = !inQuote;
      continue; 
    }
    
    if (char === ',' && !inQuote) {
      values.push(cleanValue(currentVal));
      currentVal = '';
    } else {
      currentVal += char;
    }
  }
  values.push(cleanValue(currentVal));
  return values;
}

function cleanValue(val) {
  const v = val.trim();
  if (v.toUpperCase() === 'NULL') return null;
  // hex
  if (v.startsWith('0x')) return v; 
  // numeros
  if (!isNaN(v) && v !== '') return Number(v);
  return v;
}

function normalizeRowForExport(row) {
  const normalized = {};
  Object.entries(row).forEach(([key, value]) => {
    normalized[key] = normalizeValueForExport(value);
  });
  return normalized;
}

function normalizeValueForExport(value) {
  if (Buffer.isBuffer(value)) {
    return value.length ? `0x${value.toString('hex')}` : '';
  }
  if (typeof value === 'bigint') {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value && typeof value === 'object' && value.type === 'Buffer' && Array.isArray(value.data)) {
    return value.data.length ? `0x${Buffer.from(value.data).toString('hex')}` : '';
  }
  return value;
}

module.exports = {
  exportFromMySQL,
  writeCSV,
  writeJSON,
  writeXML,
  compressFile,
  listMySQLTables,
  listTablesFromSqlFile,
};
