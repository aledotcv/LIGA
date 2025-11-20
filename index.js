#!/usr/bin/env node
const path = require('path');
const { program } = require('commander');
const cliProgress = require('cli-progress');
const { runMigration } = require('./src/core/migrationRunner');

program
  .name('db-migration-toolkit')
  .description('Migra datos CSV/JSON/XML hacia MySQL con inferencia automática de esquema.')
  .requiredOption('-i, --input <path>', 'Archivo de entrada (CSV, JSON, XML)')
  .option('-f, --format <format>', 'Formato de entrada: csv|json|xml (auto si se omite)')
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

program.parse();
