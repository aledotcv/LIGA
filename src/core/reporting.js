const fs = require('fs');
const path = require('path');

function buildReport({
  tableName,
  meta = {},
  schema,
  ddlPath,
  insertScriptPath,
  reportPath,
  totals,
  startedAt,
  finishedAt,
}) {
  const report = {
    table: tableName,
    sourceFormat: meta.format,
    encoding: meta.encoding,
    delimiter: meta.delimiter,
    rowCount: meta.rowCount,
    inserted: totals?.inserted || 0,
    errors: totals?.errors?.length || 0,
    ddlPath,
    insertScriptPath,
    reportPath,
    executionMs: startedAt && finishedAt ? finishedAt - startedAt : undefined,
    primaryKeys: schema?.primaryKeys || [],
  };
  return report;
}

function writeReport(report, outputPath) {
  if (!outputPath) return;
  const outDir = path.dirname(outputPath);
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outputPath, JSON.stringify(report, null, 2), 'utf-8');
}

module.exports = {
  buildReport,
  writeReport,
};
