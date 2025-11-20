const path = require('path');
const { parseFile, inferFormat } = require('./parsers');
const { inferSchema } = require('./schemaInference');
const { generateDDL } = require('./ddlGenerator');
const { buildInsertStatements } = require('./sqlInsertBuilder');
const { loadIntoMySQL } = require('./mysqlLoader');
const { buildReport, writeReport } = require('./reporting');
const { deriveTableName } = require('./utils');

const defaultLogger = {
  info: () => {},
  warn: () => {},
  error: () => {},
  progress: () => {},
  start: () => {},
};

async function runMigration(options, logger = defaultLogger) {
  const {
    inputPath,
    format,
    tableName,
    ddlOutputPath,
    insertOutputPath,
    reportOutputPath,
    dbConfig,
    bulkInsert = true,
    chunkSize = 250,
    dryRun = false,
    continueOnError = true,
    skipLoad = false,
    encoding,
  } = options;

  const startedAt = Date.now();
  const inferredFormat = format || inferFormat(inputPath);
  logger.info(`Leyendo archivo ${inputPath} (${inferredFormat || 'desconocido'})...`);
  const { rows, meta } = await parseFile(inputPath, inferredFormat, { encoding });

  if (!rows.length) {
    throw new Error('No se encontraron registros para migrar.');
  }

  logger.start(rows.length);
  logger.info(`Inferiendo esquema sobre ${rows.length} registros...`);
  const schema = inferSchema(rows);
  const effectiveTable = deriveTableName(inputPath, tableName);
  const ddlResult = generateDDL(effectiveTable, schema, { ddlOutputPath });

  let insertScriptPath = null;
  if (dryRun || insertOutputPath) {
    const res = buildInsertStatements(ddlResult.tableName, schema, rows, {
      insertOutputPath: insertOutputPath || path.join('output', `${ddlResult.tableName}_inserts.sql`),
      bulk: bulkInsert,
      chunkSize,
    });
    insertScriptPath = insertOutputPath || path.join('output', `${ddlResult.tableName}_inserts.sql`);
    logger.info(`Script de INSERT generado (${res.rowsWritten} filas).`);
  }

  let totals = { inserted: 0, errors: [] };
  if (!dryRun && !skipLoad) {
    logger.info('Insertando datos en MySQL...');
    totals = await loadIntoMySQL({
      connectionConfig: dbConfig,
      ddl: ddlResult.ddl,
      tableName: ddlResult.tableName,
      schema,
      rows,
      bulk: bulkInsert,
      chunkSize,
      dryRun,
      continueOnError,
      onProgress: (count) => logger.progress(count),
    });
  } else {
    logger.info('Dry-run habilitado: no se ejecutaron inserciones en MySQL.');
    logger.progress(rows.length);
  }

  const finishedAt = Date.now();
  const report = buildReport({
    tableName: ddlResult.tableName,
    meta: { ...meta },
    schema,
    ddlPath: ddlOutputPath,
    insertScriptPath,
    reportPath: reportOutputPath,
    totals,
    startedAt,
    finishedAt,
  });
  writeReport(report, reportOutputPath);

  const errorList = totals.errors || [];
  return {
    ...report,
    errorCount: errorList.length,
    errors: errorList,
  };
}

module.exports = { runMigration };
