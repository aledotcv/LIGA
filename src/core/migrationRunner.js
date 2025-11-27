const path = require('path');
const fs = require('fs');
const archiver = require('archiver');
const { parseFile, inferFormat } = require('./parsers');
const { inferSchema } = require('./schemaInference');
const { generateDDL } = require('./ddlGenerator');
const { buildInsertStatements } = require('./sqlInsertBuilder');
const { loadIntoMySQL } = require('./mysqlLoader');
const { buildReport, writeReport } = require('./reporting');
const { deriveTableName } = require('./utils');
const { validateData, validateReferentialIntegrity, generateValidationReport } = require('./validation');
const { loadTransformConfig, applyTransformations } = require('./transformations');
const { processBatchFiles, sortTablesForInsertion, generateForeignKeyConstraints } = require('./batchProcessor');
const { parseAccessFile } = require('./accessParser');
const { generateCRUDProcedures } = require('./storedProcedureGenerator');

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
    enableValidation = false,
    checkDuplicates = true,
    checkInvalidValues = true,
    validationReportPath,
    transformConfigPath,
    batchMode = false,
    detectFKs = false,
    sortByDependencies = false,
    isAccessFile = false,
    accessTables = null,
    generateProcedures = false,
    proceduresOutputPath,
    compress = false,
  } = options;

  const startedAt = Date.now();
  
  // detectar si es un archivo Access
  const ext = path.extname(inputPath).toLowerCase();
  const isAccess = isAccessFile || ['.mdb', '.accdb'].includes(ext);
  
  // lote o Access múltiples tablas
  if (batchMode || isAccess) {
    return await runBatchMigration(options, logger);
  }

  const inferredFormat = format || inferFormat(inputPath);
  logger.info(`Leyendo archivo ${inputPath} (${inferredFormat || 'desconocido'})...`);
  let { rows, meta } = await parseFile(inputPath, inferredFormat, { encoding });

  if (!rows.length) {
    throw new Error('No se encontraron registros para migrar.');
  }

  // aplicar transformaciones
  if (transformConfigPath) {
    logger.info('Aplicando transformaciones...');
    const transformConfig = loadTransformConfig(transformConfigPath);
    if (transformConfig) {
      rows = applyTransformations(rows, transformConfig);
      logger.info('Transformaciones aplicadas.');
    }
  }

  logger.start(rows.length);
  logger.info(`Inferiendo esquema sobre ${rows.length} registros...`);
  const schema = inferSchema(rows);
  const effectiveTable = deriveTableName(inputPath, tableName);

  // validación de datos 
  let validationIssues = [];
  let validationReportFile = null;
  if (enableValidation) {
    logger.info('Validando datos...');
    validationIssues = validateData(rows, schema, { checkDuplicates, checkInvalidValues });
    
    if (validationIssues.length > 0) {
      validationReportFile = validationReportPath || path.join('output', 'validation_report.json');
      const validationReport = generateValidationReport(
        validationIssues,
        validationReportFile
      );
      logger.warn(`Encontrados ${validationReport.errorCount} errores y ${validationReport.warningCount} advertencias`);
      
      const criticalErrors = validationIssues.filter(i => i.severity === 'error');
      if (criticalErrors.length > 0 && !continueOnError) {
        throw new Error(`Validación falló con ${criticalErrors.length} errores críticos. Revise ${validationReportPath}`);
      }
    } else {
      logger.info('Validación completada sin problemas.');
    }
  }

  const ddlResult = generateDDL(effectiveTable, schema, { ddlOutputPath });
  const artifactPaths = [];
  if (ddlOutputPath) {
    artifactPaths.push(ddlOutputPath);
  }
  if (validationReportFile) {
    artifactPaths.push(validationReportFile);
  }

  //benerar stored procedures si está habilitado
  if (generateProcedures) {
    logger.info('Generando stored procedures CRUD...');
    const procPath = proceduresOutputPath || path.join('output', `${ddlResult.tableName}_procedures.sql`);
    const procedures = generateCRUDProcedures(ddlResult.tableName, schema, { outputPath: procPath });
    logger.info(`${procedures.length} stored procedures generados en: ${procPath}`);
    artifactPaths.push(procPath);
  }

  let insertScriptPath = null;
  if (dryRun || insertOutputPath) {
    const res = buildInsertStatements(ddlResult.tableName, schema, rows, {
      insertOutputPath: insertOutputPath || path.join('output', `${ddlResult.tableName}_inserts.sql`),
      bulk: bulkInsert,
      chunkSize,
    });
    insertScriptPath = insertOutputPath || path.join('output', `${ddlResult.tableName}_inserts.sql`);
    logger.info(`Script de INSERT generado (${res.rowsWritten} filas).`);
    artifactPaths.push(insertScriptPath);
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
    validationIssues: validationIssues.length,
  });
  writeReport(report, reportOutputPath);
  if (reportOutputPath) {
    artifactPaths.push(reportOutputPath);
  }

  const errorList = totals.errors || [];
  let compressedOutputsPath = null;
  if (compress) {
    const bundleBase = ddlResult.tableName || path.basename(inputPath);
    const zipTarget = resolveBundlePath(bundleBase, reportOutputPath);
    compressedOutputsPath = await bundleOutputs(artifactPaths, zipTarget);
    if (compressedOutputsPath) {
      logger.info(`Archivos de salida comprimidos en: ${compressedOutputsPath}`);
    } else {
      logger.warn('No se encontraron archivos para comprimir.');
    }
  }

  return {
    ...report,
    errorCount: errorList.length,
    errors: errorList,
    validationIssues,
    compressedOutputs: compressedOutputsPath,
  };
}

/**
  migración en modo lote (múltiples archivos/tablas)
 */
async function runBatchMigration(options, logger = defaultLogger) {
  const {
    inputPath,
    encoding,
    transformConfigPath,
    detectFKs = false,
    sortByDependencies = false,
    enableValidation = false,
    checkDuplicates = true,
    checkInvalidValues = true,
    validationReportPath,
    ddlOutputPath,
    dbConfig,
    bulkInsert = true,
    chunkSize = 250,
    dryRun = false,
    continueOnError = true,
    skipLoad = false,
    isAccessFile = false,
    accessTables = null,
    compress = false,
    reportOutputPath,
  } = options;

  const startedAt = Date.now();
  let validationReportFile = null;
  let tables = [];

  // Determinar si Access o directorio
  const ext = path.extname(inputPath).toLowerCase();
  const isAccess = isAccessFile || ['.mdb', '.accdb'].includes(ext);

  if (isAccess) {
    logger.info(`Leyendo archivo Access: ${inputPath}...`);
    const accessData = await parseAccessFile(inputPath, { selectedTables: accessTables });
    logger.info(`Tablas disponibles: ${accessData.availableTables.join(', ')}`);
    
    tables = accessData.tables.map((t) => ({
      tableName: t.tableName,
      rows: t.rows,
      schema: inferSchema(t.rows),
      meta: t.meta,
    }));
  } else {
    logger.info(`Procesando archivos en lote desde: ${inputPath}...`);
    const batchData = await processBatchFiles(inputPath, { encoding });
    
    tables = batchData.map((file) => ({
      tableName: deriveTableName(file.filePath, file.tableName),
      rows: file.rows,
      schema: null,
      meta: file.meta,
      filePath: file.filePath,
    }));
  }

  logger.info(`${tables.length} tabla(s) encontrada(s).`);

  // Aplicar transformaciones si están configuradas
  if (transformConfigPath) {
    logger.info('Aplicando transformaciones...');
    const transformConfig = loadTransformConfig(transformConfigPath);
    if (transformConfig) {
      tables.forEach((table) => {
        table.rows = applyTransformations(table.rows, transformConfig);
      });
      logger.info('Transformaciones aplicadas a todas las tablas.');
    }
  }

  // inferir esquemas
  tables.forEach((table) => {
    if (!table.schema) {
      table.schema = inferSchema(table.rows);
    }
  });

  // detectar FKs y ordenar por dependencias
  let foreignKeys = [];
  if (detectFKs) {
    logger.info('Detectando foreign keys implícitas...');
    const { tables: sortedTables, foreignKeys: detectedFKs, insertionOrder } = sortTablesForInsertion(tables);
    foreignKeys = detectedFKs;
    logger.info(`${foreignKeys.length} foreign key(s) detectada(s).`);
    
    if (sortByDependencies && sortedTables.length === tables.length) {
      tables = sortedTables;
      logger.info(`Orden de inserción: ${insertionOrder.join(' → ')}`);
    }
  }

  // validación de integridad referencial
  let validationIssues = [];
  if (enableValidation) {
    logger.info('Validando datos...');
    
    // validar cada tabla individualmente
    tables.forEach((table) => {
      const issues = validateData(table.rows, table.schema, { checkDuplicates, checkInvalidValues });
      issues.forEach(issue => {
        issue.table = table.tableName;
        validationIssues.push(issue);
      });
    });

    // validar integridad referencial si hay FKs
    if (foreignKeys.length > 0) {
      const tablesWithFKs = tables.map(t => ({ ...t, foreignKeys: foreignKeys.filter(fk => fk.table === t.tableName) }));
      const refIssues = validateReferentialIntegrity(tablesWithFKs);
      validationIssues.push(...refIssues);
    }

    if (validationIssues.length > 0) {
      validationReportFile = validationReportPath || path.join('output', 'validation_report.json');
      const validationReport = generateValidationReport(
        validationIssues,
        validationReportFile
      );
      logger.warn(`Encontrados ${validationReport.errorCount} errores y ${validationReport.warningCount} advertencias`);
      
      const criticalErrors = validationIssues.filter(i => i.severity === 'error');
      if (criticalErrors.length > 0 && !continueOnError) {
        throw new Error(`Validación falló con ${criticalErrors.length} errores críticos.`);
      }
    } else {
      logger.info('Validación completada sin problemas.');
    }
  }

  // generar DDLs
  const ddls = [];
  const artifactPaths = [];
  const outputDir = path.dirname(ddlOutputPath || 'output/schema.sql');
  fs.mkdirSync(outputDir, { recursive: true });

  tables.forEach((table) => {
    const schemaPath = path.join(outputDir, `${table.tableName}_schema.sql`);
    const ddlResult = generateDDL(table.tableName, table.schema, {
      ddlOutputPath: schemaPath,
    });
    ddls.push(ddlResult.ddl);
    artifactPaths.push(schemaPath);
  });

  // generar FKs si se detectaron
  if (foreignKeys.length > 0) {
    const fkDDL = generateForeignKeyConstraints(foreignKeys);
    const fkPath = path.join(outputDir, 'foreign_keys.sql');
    fs.writeFileSync(fkPath, fkDDL, 'utf-8');
    logger.info(`Foreign keys guardadas en: ${fkPath}`);
    artifactPaths.push(fkPath);
  }

  // insertar datos
  const results = [];
  let totalInserted = 0;
  let totalErrors = 0;

  if (!dryRun && !skipLoad) {
    const mysql = require('mysql2/promise');
    const connection = await mysql.createConnection({
      multipleStatements: true,
      ...dbConfig,
    });

    try {
      for (const [index, table] of tables.entries()) {
        logger.info(`[${index + 1}/${tables.length}] Procesando tabla: ${table.tableName}...`);
        
        const ddl = ddls[index];
        await connection.query(ddl);

        const totals = await loadIntoMySQL({
          connectionConfig: dbConfig,
          ddl,
          tableName: table.tableName,
          schema: table.schema,
          rows: table.rows,
          bulk: bulkInsert,
          chunkSize,
          dryRun: false,
          continueOnError,
          onProgress: (count) => logger.progress(count),
        });

        totalInserted += totals.inserted;
        totalErrors += totals.errors.length;
        
        results.push({
          table: table.tableName,
          inserted: totals.inserted,
          errors: totals.errors.length,
        });
      }
    } finally {
      await connection.end();
    }
  } else {
    logger.info('Dry-run habilitado: no se ejecutaron inserciones.');
  }

  const finishedAt = Date.now();
  const report = {
    mode: 'batch',
    totalTables: tables.length,
    totalRows: tables.reduce((sum, t) => sum + t.rows.length, 0),
    totalInserted,
    totalErrors,
    tables: results,
    foreignKeys: foreignKeys.length,
    validationIssues: validationIssues.length,
    executionMs: finishedAt - startedAt,
  };

  const reportPath = reportOutputPath || path.join(outputDir, 'batch_report.json');
  writeReport(report, reportPath);
  artifactPaths.push(reportPath);
  if (validationReportFile) {
    artifactPaths.push(validationReportFile);
  }

  if (compress) {
    const bundleBase = path.basename(inputPath) || 'batch';
    const zipTarget = resolveBundlePath(bundleBase, reportPath);
    const compressedOutputsPath = await bundleOutputs(artifactPaths, zipTarget);
    if (compressedOutputsPath) {
      logger.info(`Archivos de salida comprimidos en: ${compressedOutputsPath}`);
      report.compressedOutputs = compressedOutputsPath;
    } else {
      logger.warn('No se encontraron archivos para comprimir.');
    }
  }

  return report;
}

async function bundleOutputs(files, zipPath) {
  const entries =
    (files || [])
      .filter(Boolean)
      .filter((file, index, self) => self.indexOf(file) === index && fs.existsSync(file));

  if (!entries.length) return null;

  fs.mkdirSync(path.dirname(zipPath), { recursive: true });

  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver('zip', { zlib: { level: 9 } });

    output.on('close', () => resolve(zipPath));
    archive.on('error', (err) => reject(err));

    archive.pipe(output);
    entries.forEach((file) => {
      archive.file(file, { name: path.basename(file) });
    });
    archive.finalize();
  });
}

function resolveBundlePath(baseName, reportPath) {
  const dir = reportPath ? path.dirname(reportPath) : path.join('output');
  const safeBase = (baseName || 'migration').replace(/[^a-z0-9_-]+/gi, '_') || 'migration';
  return path.join(dir, `${safeBase}_outputs.zip`);
}

module.exports = { runMigration };
