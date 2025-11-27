#!/usr/bin/env node
const path = require('path');
const { program } = require('commander');
const cliProgress = require('cli-progress');
const { runMigration } = require('./src/core/migrationRunner');
const { exportFromMySQL, listMySQLTables } = require('./src/core/mysqlExporter');

program
  .name('db-migration-toolkit')
  .description('Migra datos CSV/JSON/XML hacia MySQL con inferencia automática de esquema.')
  .requiredOption('-i, --input <path>', 'Archivo de entrada (CSV, JSON, XML, Access) o directorio para modo lote')
  .option('-f, --format <format>', 'Formato de entrada: csv|json|xml|access (auto si se omite)')
  .option('-t, --table <name>', 'Nombre de tabla destino (se sanea automáticamente)')
  .option('-H, --host <host>', 'Host MySQL', 'localhost')
  .option('-P, --port <port>', 'Puerto MySQL', '3306')
  .option('-u, --user <user>', 'Usuario MySQL', 'root')
  .option('-p, --password <password>', 'Password MySQL', '')
  .option('-d, --database <database>', 'Base de datos destino')
  .option('--bulk', 'Activar bulk insert', true)
  .option('--no-bulk', 'Desactivar bulk insert')
  .option('--chunk-size <n>', 'Tamaño de lote para bulk insert', '250')
  .option('--ddl-out <path>', 'Ruta para guardar el DDL generado', path.join('output', 'schema.sql'))
  .option('--insert-out <path>', 'Ruta para guardar script de INSERT (se activa en dry-run)', path.join('output', 'inserts.sql'))
  .option('--report-out <path>', 'Ruta para guardar reporte JSON', path.join('output', 'report.json'))
  .option('--dry-run', 'Generar DDL/INSERT sin ejecutar en MySQL', false)
  .option('--continue-on-error', 'Continuar ante errores de inserción', true)
  .option('--stop-on-error', 'Abortar ante el primer error de inserción')
  .option('--encoding <enc>', 'Forzar encoding (utf-8, latin1, etc.)')
  .option('--skip-load', 'No ejecutar cargas en MySQL, solo generación de scripts', false)
  .option('--enable-validation', 'Activar validación de datos antes de insertar', false)
  .option('--check-duplicates', 'Detectar valores duplicados en columnas únicas', true)
  .option('--no-check-duplicates', 'No verificar duplicados')
  .option('--check-invalid-values', 'Validar valores fuera de rango o inválidos', true)
  .option('--no-check-invalid-values', 'No validar valores')
  .option('--validation-report <path>', 'Ruta para reporte de validación', path.join('output', 'validation_report.json'))
  .option('--transform-config <path>', 'Archivo JSON de configuración de transformaciones')
  .option('--batch-mode', 'Procesar múltiples archivos desde un directorio', false)
  .option('--detect-fks', 'Detectar foreign keys implícitas', false)
  .option('--sort-by-dependencies', 'Ordenar inserción por dependencias', false)
  .option('--access-tables <tables>', 'Tablas específicas a procesar de Access (separadas por coma)')
  .option('--generate-procedures', 'Generar stored procedures CRUD en lugar de INSERTs directos', false)
  .option('--procedures-out <path>', 'Ruta para guardar stored procedures', path.join('output', 'procedures.sql'))
  .option('--compress', 'Comprimir archivos de salida a .zip', false)
  .showHelpAfterError();

program.action(async (opts) => {
  const continueOnError = opts.stopOnError ? false : opts.continueOnError;
  const progress = new cliProgress.SingleBar(
    {
      format: 'Progreso [{bar}] {percentage}% | {value}/{total} filas',
      hideCursor: true,
    },
    cliProgress.Presets.shades_classic
  );

  let totalRows = 0;
  let processed = 0;

  const logger = {
    info: (msg) => console.log(msg),
    warn: (msg) => console.warn(msg),
    error: (msg) => console.error(msg),
    start: (rows) => {
      totalRows = rows;
      progress.start(rows, 0);
    },
    progress: (count) => {
      processed += count;
      progress.update(processed);
    },
  };

  try {
    const result = await runMigration(
      {
        inputPath: opts.input,
        format: opts.format,
        tableName: opts.table,
        ddlOutputPath: opts.ddlOut,
        insertOutputPath: opts.insertOut,
        reportOutputPath: opts.reportOut,
        bulkInsert: opts.bulk,
        chunkSize: Number(opts.chunkSize) || 250,
        dryRun: opts.dryRun || opts.skipLoad,
        continueOnError,
        skipLoad: opts.skipLoad,
        encoding: opts.encoding,
        enableValidation: opts.enableValidation,
        checkDuplicates: opts.checkDuplicates,
        checkInvalidValues: opts.checkInvalidValues,
        validationReportPath: opts.validationReport,
        transformConfigPath: opts.transformConfig,
        batchMode: opts.batchMode,
        detectFKs: opts.detectFks,
        sortByDependencies: opts.sortByDependencies,
        isAccessFile: opts.format === 'access',
        accessTables: opts.accessTables ? opts.accessTables.split(',').map(t => t.trim()) : null,
        dbConfig: {
          host: opts.host,
          port: Number(opts.port) || 3306,
          user: opts.user,
          password: opts.password,
          database: opts.database,
        },
      },
      logger
    );
    progress.stop();
    console.log('\nMigración completada.');
    console.log(JSON.stringify(result, null, 2));
  } catch (err) {
    progress.stop();
    console.error(`Error: ${err.message}`);
    process.exit(1);
  }
});

// comando para exportar desde MySQL
program
  .command('export')
  .description('Exporta tabla de MySQL a CSV/JSON/XML')
  .requiredOption('-t, --table <name>', 'Nombre de la tabla a exportar')
  .requiredOption('-o, --output <path>', 'Archivo de salida')
  .option('-f, --format <format>', 'Formato de salida: csv|json|xml', 'csv')
  .option('-H, --host <host>', 'Host MySQL', 'localhost')
  .option('-P, --port <port>', 'Puerto MySQL', '3306')
  .option('-u, --user <user>', 'Usuario MySQL', 'root')
  .option('-p, --password <password>', 'Password MySQL', '')
  .option('-d, --database <database>', 'Base de datos origen')
  .option('--compress', 'Comprimir salida a .zip', false)
  .action(async (opts) => {
    if (!opts.database) {
      console.error('Error: --database es requerido');
      process.exit(1);
    }

    try {
      console.log(`Exportando tabla '${opts.table}' de '${opts.database}' a ${opts.format.toUpperCase()}...`);
      
      const result = await exportFromMySQL(
        {
          dbConfig: {
            host: opts.host,
            port: Number(opts.port) || 3306,
            user: opts.user,
            password: opts.password,
            database: opts.database,
          },
          tableName: opts.table,
          format: opts.format,
          outputPath: opts.output,
          compress: opts.compress,
        },
        console
      );

      console.log(`✓ Exportación exitosa: ${result}`);
    } catch (err) {
      console.error(`Error: ${err.message}`);
      process.exit(1);
    }
  });

// comando para listar tablas de MySQL
program
  .command('list-tables')
  .description('Lista todas las tablas en una base de datos MySQL')
  .option('-H, --host <host>', 'Host MySQL', 'localhost')
  .option('-P, --port <port>', 'Puerto MySQL', '3306')
  .option('-u, --user <user>', 'Usuario MySQL', 'root')
  .option('-p, --password <password>', 'Password MySQL', '')
  .requiredOption('-d, --database <database>', 'Base de datos')
  .action(async (opts) => {
    try {
      const tables = await listMySQLTables({
        host: opts.host,
        port: Number(opts.port) || 3306,
        user: opts.user,
        password: opts.password,
        database: opts.database,
      });

      console.log(`\nTablas en '${opts.database}':`);
      tables.forEach((table, index) => {
        console.log(`  ${index + 1}. ${table}`);
      });
      console.log(`\nTotal: ${tables.length} tabla(s)`);
    } catch (err) {
      console.error(`Error: ${err.message}`);
      process.exit(1);
    }
  });

program.parse();
