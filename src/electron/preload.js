const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('migrationAPI', {
  chooseFile: () => ipcRenderer.invoke('choose-file'),
  runMigration: (payload) => ipcRenderer.invoke('run-migration', payload),
  onLog: (handler) => ipcRenderer.on('migration-log', (_event, msg) => handler(msg)),
  onError: (handler) => ipcRenderer.on('migration-error', (_event, msg) => handler(msg)),
  onProgress: (handler) => ipcRenderer.on('migration-progress', (_event, value) => handler(value)),
  onStart: (handler) => ipcRenderer.on('migration-start', (_event, rows) => handler(rows)),
});
