const path = require('path');
const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const { runMigration } = require('../core/migrationRunner');

const createWindow = () => {
  const win = new BrowserWindow({
    width: 1100,
    height: 800,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
    },
    backgroundColor: '#0f172a',
  });

  win.loadFile(path.join(__dirname, '..', 'renderer', 'index.html'));
};

app.whenReady().then(() => {
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

ipcMain.handle('choose-file', async () => {
  const result = await dialog.showOpenDialog({
    properties: ['openFile'],
    filters: [
      { name: 'Datos', extensions: ['csv', 'json', 'xml'] },
      { name: 'Todos', extensions: ['*'] },
    ],
  });
  if (result.canceled || !result.filePaths.length) return null;
  return result.filePaths[0];
});

ipcMain.handle('run-migration', async (event, payload) => {
  const log = (message) => event.sender.send('migration-log', message);
  const logger = {
    info: log,
    warn: log,
    error: (msg) => event.sender.send('migration-error', msg),
    progress: (count) => event.sender.send('migration-progress', count),
    start: (rows) => event.sender.send('migration-start', rows),
  };

  try {
    const result = await runMigration(payload, logger);
    return { ok: true, result };
  } catch (err) {
    return { ok: false, error: err.message };
  }
});
