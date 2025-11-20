const selectFileBtn = document.getElementById('selectFileBtn');
const selectedFile = document.getElementById('selectedFile');
const runBtn = document.getElementById('runBtn');
const logPanel = document.getElementById('logPanel');
const progressBar = document.getElementById('progressBar');
const progressLabel = document.getElementById('progressLabel');
const statusPill = document.getElementById('statusPill');
const summary = document.getElementById('summary');

let filePath = '';
let totalRows = 0;
let progressed = 0;

function appendLog(message) {
  const ts = new Date().toLocaleTimeString();
  logPanel.textContent += `[${ts}] ${message}\n`;
  logPanel.scrollTop = logPanel.scrollHeight;
}

function updateStatus(text, intent = 'muted') {
  statusPill.textContent = text;
  const palettes = {
    muted: 'rgba(255,255,255,0.08)',
    running: '#f97316',
    ok: '#22c55e',
    error: '#ef4444',
  };
  statusPill.style.background = palettes[intent] || palettes.muted;
}

function updateProgress(value, total) {
  const pct = total > 0 ? Math.min(100, Math.round((value / total) * 100)) : 0;
  progressBar.style.width = `${pct}%`;
  progressLabel.textContent = `${pct}%`;
}

selectFileBtn.addEventListener('click', async () => {
  const chosen = await window.migrationAPI.chooseFile();
  if (chosen) {
    filePath = chosen;
    selectedFile.textContent = chosen;
    appendLog(`Archivo seleccionado: ${chosen}`);
  }
});

runBtn.addEventListener('click', async () => {
  if (!filePath) {
    appendLog('Selecciona un archivo antes de ejecutar.');
    updateStatus('Falta archivo', 'error');
    return;
  }

  runBtn.disabled = true;
  summary.classList.add('hidden');
  updateStatus('Ejecutando', 'running');
  appendLog('Iniciando migración...');
  totalRows = 0;
  progressed = 0;
  updateProgress(0, 1);

  const payload = {
    inputPath: filePath,
    format: document.getElementById('format').value || null,
    tableName: document.getElementById('tableName').value || null,
    ddlOutputPath: document.getElementById('ddlOut').value,
    insertOutputPath: document.getElementById('insertOut').value,
    reportOutputPath: document.getElementById('reportOut').value,
    bulkInsert: document.getElementById('bulk').checked,
    chunkSize: Number(document.getElementById('chunkSize').value) || 250,
    dryRun: document.getElementById('dryRun').checked,
    continueOnError: !document.getElementById('stopOnError').checked,
    skipLoad: document.getElementById('dryRun').checked,
    encoding: document.getElementById('encoding').value || undefined,
    dbConfig: {
      host: document.getElementById('dbHost').value || 'localhost',
      port: Number(document.getElementById('dbPort').value) || 3306,
      user: document.getElementById('dbUser').value || 'root',
      password: document.getElementById('dbPass').value || '',
      database: document.getElementById('dbName').value || '',
    },
  };

  const response = await window.migrationAPI.runMigration(payload);
  runBtn.disabled = false;

  if (response.ok) {
    updateStatus('Completado', 'ok');
    appendLog('Migración finalizada.');
    summary.classList.remove('hidden');
    summary.innerHTML = `
      <div><strong>Tabla:</strong> ${response.result.table}</div>
      <div><strong>Registros origen:</strong> ${response.result.rowCount || totalRows}</div>
      <div><strong>Insertados:</strong> ${response.result.inserted}</div>
      <div><strong>Errores:</strong> ${response.result.errorCount || 0}</div>
      <div><strong>DDL:</strong> ${response.result.ddlPath || 'n/a'}</div>
      <div><strong>Report:</strong> ${response.result.reportPath || payload.reportOutputPath}</div>
    `;
  } else {
    updateStatus('Error', 'error');
    appendLog(`Error: ${response.error}`);
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
