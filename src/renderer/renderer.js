const selectInputBtn = document.getElementById('selectInputBtn');
const selectedInput = document.getElementById('selectedInput');
const runBtn = document.getElementById('runBtn');
const runExportBtn = document.getElementById('runExportBtn');
const listTablesBtn = document.getElementById('listTablesBtn');
const tablesList = document.getElementById('tablesList');
const logPanel = document.getElementById('logPanel');
const progressBar = document.getElementById('progressBar');
const progressLabel = document.getElementById('progressLabel');
const statusPill = document.getElementById('statusPill');
const summary = document.getElementById('summary');
const importActions = document.getElementById('importActions');
const exportCard = document.getElementById('exportCard');
const modeRadios = document.querySelectorAll('input[name="operationMode"]');
const batchModeCheckbox = document.getElementById('batchMode');
const formatSelect = document.getElementById('format');
const accessTablesRow = document.getElementById('accessTablesRow');
const exportFormatSelect = document.getElementById('exportFormat');
const exportOutputInput = document.getElementById('exportOutput');
const exportCompressCheckbox = document.getElementById('exportCompress');
const exportTableContainer = document.getElementById('exportTableContainer');
const exportTableLabel = document.getElementById('exportTableLabel');
const exportSqlContainer = document.getElementById('exportSqlContainer');
const exportSqlPathInput = document.getElementById('exportSqlPath');
const selectExportSqlBtn = document.getElementById('selectExportSqlBtn');
const exportSourceRadios = document.querySelectorAll('input[name="exportSourceType"]');

let inputPath = '';
let totalRows = 0;
let progressed = 0;
let currentMode = 'import';

function appendLog(message) {
  const ts = new Date().toLocaleTimeString();
  logPanel.textContent += `[${ts}] ${message}\n`;
  logPanel.scrollTop = logPanel.scrollHeight;
}

function updateStatus(text, intent = 'muted') {
  statusPill.textContent = text;
  const palettes = {
    muted: 'rgba(255,255,255,0.08)',
    running: '#793200FF',
    ok: '#00551FFF',
    error: '#800000FF',
  };
  statusPill.style.background = palettes[intent] || palettes.muted;
}

function updateProgress(value, total) {
  const pct = total > 0 ? Math.min(100, Math.round((value / total) * 100)) : 0;
  progressBar.style.width = `${pct}%`;
  progressLabel.textContent = `${pct}%`;
}

function getDBConfig() {
  return {
    host: document.getElementById('dbHost').value || 'localhost',
    port: Number(document.getElementById('dbPort').value) || 3306,
    user: document.getElementById('dbUser').value || 'root',
    password: document.getElementById('dbPass').value || '',
    database: document.getElementById('dbName').value || '',
  };
}

function resetProgress() {
  totalRows = 0;
  progressed = 0;
  updateProgress(0, 1);
}

function toggleOperationMode(mode) {
  currentMode = mode;
  document.querySelectorAll('[data-mode="import"]').forEach((section) => {
    section.classList.toggle('hidden', mode !== 'import');
  });
  importActions.classList.toggle('hidden', mode !== 'import');
  exportCard.classList.toggle('hidden', mode !== 'export');
  summary.classList.add('hidden');
  updateStatus('En espera');
  appendLog(`Modo seleccionado: ${mode === 'import' ? 'Importar' : 'Exportar'}.`);
  updateInputButtonLabel();
}

function updateInputButtonLabel() {
  const isBatch = batchModeCheckbox.checked;
  selectInputBtn.textContent = isBatch ? 'Seleccionar directorio' : 'Seleccionar archivo';
  selectInputBtn.disabled = currentMode !== 'import';
}

function refreshAccessFields() {
  const isAccess = formatSelect.value === 'access';
  accessTablesRow.classList.toggle('hidden', !isAccess);
}

function ensureExportOutputExtension() {
  if (!exportOutputInput) return;
  const compress = exportCompressCheckbox.checked;
  const format = exportFormatSelect.value || 'csv';
  let current = exportOutputInput.value.trim();

  if (!current) {
    exportOutputInput.value = `output/export.${compress ? 'zip' : format}`;
    return;
  }

  const stripKnownExt = (value) => value.replace(/\.(zip|csv|json|xml)$/i, '');

  if (compress) {
    if (!current.toLowerCase().endsWith('.zip')) {
      exportOutputInput.value = `${stripKnownExt(current)}.zip`;
    }
  } else {
    const desiredExt = `.${format}`;
    const lower = current.toLowerCase();
    if (lower.endsWith('.zip') || !lower.endsWith(desiredExt)) {
      exportOutputInput.value = `${stripKnownExt(current)}${desiredExt}`;
    }
  }
}

function buildMigrationPayload() {
  const dryRun = document.getElementById('dryRun').checked;
  return {
    inputPath,
    format: formatSelect.value || null,
    tableName: document.getElementById('tableName').value || null,
    ddlOutputPath: document.getElementById('ddlOut').value,
    insertOutputPath: document.getElementById('insertOut').value,
    reportOutputPath: document.getElementById('reportOut').value,
    bulkInsert: document.getElementById('bulk').checked,
    chunkSize: Number(document.getElementById('chunkSize').value) || 250,
    dryRun,
    continueOnError: !document.getElementById('stopOnError').checked,
    skipLoad: dryRun,
    encoding: document.getElementById('encoding').value || undefined,
    enableValidation: document.getElementById('enableValidation').checked,
    checkDuplicates: document.getElementById('checkDuplicates').checked,
    checkInvalidValues: document.getElementById('checkInvalidValues').checked,
    validationReportPath: document.getElementById('validationReport').value || undefined,
    transformConfigPath: document.getElementById('transformConfig').value || undefined,
    batchMode: batchModeCheckbox.checked,
    detectFKs: document.getElementById('detectFKs').checked,
    sortByDependencies: document.getElementById('sortByDependencies').checked,
    generateProcedures: document.getElementById('generateProcedures').checked,
    proceduresOutputPath: document.getElementById('proceduresOut').value || undefined,
    compress: document.getElementById('compressOutput').checked,
    isAccessFile: formatSelect.value === 'access',
    accessTables: document.getElementById('accessTables').value
      ? document
          .getElementById('accessTables')
          .value.split(',')
          .map((t) => t.trim())
          .filter(Boolean)
      : null,
    dbConfig: getDBConfig(),
  };
}

function showImportSummary(response, payload) {
  summary.classList.remove('hidden');
  if (response.result.mode === 'batch') {
    summary.innerHTML = `
      <div><strong>Modo:</strong> Lote (${response.result.totalTables} tablas)</div>
      <div><strong>Registros totales:</strong> ${response.result.totalRows}</div>
      <div><strong>Insertados:</strong> ${response.result.totalInserted}</div>
      <div><strong>Errores:</strong> ${response.result.totalErrors}</div>
      <div><strong>Foreign Keys:</strong> ${response.result.foreignKeys}</div>
      <div><strong>Problemas validación:</strong> ${response.result.validationIssues || 0}</div>
    `;
  } else {
    summary.innerHTML = `
      <div><strong>Tabla:</strong> ${response.result.table}</div>
      <div><strong>Registros origen:</strong> ${response.result.rowCount || totalRows}</div>
      <div><strong>Insertados:</strong> ${response.result.inserted}</div>
      <div><strong>Errores:</strong> ${response.result.errorCount || 0}</div>
      <div><strong>Problemas validación:</strong> ${response.result.validationIssues?.length || 0}</div>
      <div><strong>DDL:</strong> ${response.result.ddlPath || payload.ddlOutputPath}</div>
      <div><strong>Report:</strong> ${response.result.reportPath || payload.reportOutputPath}</div>
    `;
  }
}

function showExportSummary(resultPath, payload) {
  summary.classList.remove('hidden');
  const sourceInfo = payload.sourceType === 'sql'
    ? `<div><strong>Origen:</strong> Archivo SQL (${payload.tableName})</div>`
    : `<div><strong>Tabla:</strong> ${payload.tableName}</div>`;

  summary.innerHTML = `
    <div><strong>Acción:</strong> Exportación MySQL → ${payload.format.toUpperCase()}</div>
    ${sourceInfo}
    <div><strong>Archivo generado:</strong> ${resultPath}</div>
    <div><strong>Comprimido:</strong> ${payload.compress ? 'Sí' : 'No'}</div>
  `;
}

modeRadios.forEach((radio) =>
  radio.addEventListener('change', (event) => {
    if (event.target.checked) {
      toggleOperationMode(event.target.value);
    }
  })
);

exportSourceRadios.forEach((radio) => {
  radio.addEventListener('change', (event) => {
    const isSql = event.target.value === 'sql';
    exportSqlContainer.classList.toggle('hidden', !isSql);
    if (exportTableLabel) {
      exportTableLabel.textContent = isSql ? 'Tabla destino (archivo SQL)' : 'Tabla de MySQL';
    }
    listTablesBtn.disabled = isSql;
  });
});

selectExportSqlBtn.addEventListener('click', async () => {
  const chosen = await window.migrationAPI.chooseInput({
    allowDirectories: false,
    filters: [{ name: 'SQL Files', extensions: ['sql'] }],
  });
  if (chosen) {
    exportSqlPathInput.value = chosen;
    appendLog(`Archivo SQL seleccionado: ${chosen}`);
    await populateTablesFromSqlFile(chosen);
  }
});

async function populateTablesFromSqlFile(sqlPath) {
  try {
    appendLog('Extrayendo tablas del archivo SQL...');
    const response = await window.migrationAPI.listTablesFromSql(sqlPath);
    if (response.ok && response.tables.length > 0) {
      tablesList.innerHTML = '';
      response.tables.forEach((table) => {
        const option = document.createElement('option');
        option.value = table;
        tablesList.appendChild(option);
      });
      appendLog(`Tablas encontradas: ${response.tables.join(', ')}`);
    } else if (response.ok) {
      appendLog('No se encontraron tablas en el archivo SQL.');
    } else {
      appendLog(`Error al leer tablas: ${response.error}`);
    }
  } catch (err) {
    appendLog(`Error al extraer tablas: ${err.message}`);
  }
}

batchModeCheckbox.addEventListener('change', () => {
  updateInputButtonLabel();
  if (batchModeCheckbox.checked) {
    selectedInput.textContent = 'En espera de directorio...';
    inputPath = '';
  }
});

formatSelect.addEventListener('change', refreshAccessFields);
exportFormatSelect.addEventListener('change', ensureExportOutputExtension);
exportCompressCheckbox.addEventListener('change', ensureExportOutputExtension);

selectInputBtn.addEventListener('click', async () => {
  const allowDirectories = batchModeCheckbox.checked;
  const chosen = await window.migrationAPI.chooseInput({
    allowDirectories,
    filters: [
      { name: 'Datos', extensions: ['csv', 'json', 'xml', 'mdb', 'accdb'] },
      { name: 'Todos', extensions: ['*'] },
    ],
  });

  if (chosen) {
    inputPath = chosen;
    selectedInput.textContent = chosen;
    appendLog(`Origen seleccionado: ${chosen}`);
  }
});

runBtn.addEventListener('click', async () => {
  if (currentMode !== 'import') {
    return;
  }

  if (!inputPath) {
    appendLog('Selecciona un archivo o directorio antes de ejecutar.');
    updateStatus('Falta origen', 'error');
    return;
  }

  runBtn.disabled = true;
  runExportBtn.disabled = true;
  summary.classList.add('hidden');
  updateStatus('Ejecutando import', 'running');
  appendLog('Iniciando migración...');
  resetProgress();

  const payload = buildMigrationPayload();
  const response = await window.migrationAPI.runMigration(payload);

  runBtn.disabled = false;
  runExportBtn.disabled = false;

  if (response.ok) {
    updateStatus('Completado', 'ok');
    appendLog('Migración finalizada.');
    showImportSummary(response, payload);
  } else {
    updateStatus('Error', 'error');
    appendLog(`Error: ${response.error}`);
  }
});

runExportBtn.addEventListener('click', async () => {
  if (currentMode !== 'export') {
    return;
  }

  const sourceType = document.querySelector('input[name="exportSourceType"]:checked').value;
  const tableName = document.getElementById('exportTable').value.trim();
  const sqlPath = exportSqlPathInput.value.trim();
  const outputPath = document.getElementById('exportOutput').value.trim();

  if (!tableName) {
    appendLog('Nombre de tabla requerido para exportar.');
    updateStatus('Faltan datos', 'error');
    return;
  }
  if (sourceType === 'sql' && !sqlPath) {
    appendLog('Archivo SQL es requerido para exportar.');
    updateStatus('Faltan datos', 'error');
    return;
  }
  if (!outputPath) {
    appendLog('Ruta de salida es requerida para exportar.');
    updateStatus('Faltan datos', 'error');
    return;
  }

  runBtn.disabled = true;
  runExportBtn.disabled = true;
  updateStatus('Exportando', 'running');

  if (sourceType === 'table') {
    appendLog(`Exportando tabla ${tableName}...`);
  } else {
    appendLog(`Exportando tabla ${tableName} desde archivo SQL ${sqlPath}...`);
  }

  summary.classList.add('hidden');
  updateProgress(0, 1);

  const payload = {
    dbConfig: getDBConfig(),
    sourceType,
    tableName,
    sqlPath: sourceType === 'sql' ? sqlPath : undefined,
    format: document.getElementById('exportFormat').value,
    outputPath,
    compress: document.getElementById('exportCompress').checked,
  };

  const response = await window.migrationAPI.runExport(payload);
  runBtn.disabled = false;
  runExportBtn.disabled = false;

  if (response.ok) {
    updateStatus('Exportación lista', 'ok');
    appendLog(`Exportación completada: ${response.result}`);
    showExportSummary(response.result, payload);
  } else {
    updateStatus('Error', 'error');
    appendLog(`Error: ${response.error}`);
  }
});

listTablesBtn.addEventListener('click', async () => {
  if (listTablesBtn.disabled) {
    return;
  }
  const config = getDBConfig();
  if (!config.database) {
    appendLog('Especifica la base de datos para listar tablas.');
    updateStatus('Falta base de datos', 'error');
    return;
  }

  appendLog(`Consultando tablas en ${config.database}...`);
  const response = await window.migrationAPI.listTables(config);
  if (response.ok) {
    tablesList.innerHTML = '';
    response.tables.forEach((table) => {
      const option = document.createElement('option');
      option.value = table;
      tablesList.appendChild(option);
    });
    appendLog(`Tablas disponibles: ${response.tables.length}`);
  } else {
    appendLog(`Error al listar tablas: ${response.error}`);
    updateStatus('Error', 'error');
  }
});

window.migrationAPI.onLog((msg) => appendLog(msg));
window.migrationAPI.onError((msg) => appendLog(`Error: ${msg}`));
window.migrationAPI.onStart((rows) => {
  totalRows = rows;
  progressed = 0;
  updateProgress(0, totalRows);
});
window.migrationAPI.onProgress((count) => {
  progressed += count;
  updateProgress(progressed, totalRows);
});

setTimeout(() => {
  const intro = document.getElementById('intro-overlay');
  if (intro) {
    intro.remove();
  }
}, 3000);

toggleOperationMode('import');
updateInputButtonLabel();
refreshAccessFields();
ensureExportOutputExtension();
const initialExportSource = document.querySelector('input[name="exportSourceType"]:checked');
if (initialExportSource) {
  initialExportSource.dispatchEvent(new Event('change'));
}
