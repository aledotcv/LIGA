const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('migrationAPI', {
  chooseInput: (options) => ipcRenderer.invoke('choose-input', options),
  runMigration: (payload) => ipcRenderer.invoke('run-migration', payload),
  runExport: (payload) => ipcRenderer.invoke('export-from-mysql', payload),
  listTables: (dbConfig) => ipcRenderer.invoke('list-mysql-tables', dbConfig),
  listTablesFromSql: (sqlPath) => ipcRenderer.invoke('list-tables-from-sql', sqlPath),
  onLog: (handler) => ipcRenderer.on('migration-log', (_event, msg) => handler(msg)),
  onError: (handler) => ipcRenderer.on('migration-error', (_event, msg) => handler(msg)),
  onProgress: (handler) => ipcRenderer.on('migration-progress', (_event, value) => handler(value)),
  onStart: (handler) => ipcRenderer.on('migration-start', (_event, rows) => handler(rows)),
});
