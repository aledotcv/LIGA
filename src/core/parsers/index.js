const path = require('path');
const { parseCSV } = require('./csvParser');
const { parseJSON } = require('./jsonParser');
const { parseXML } = require('./xmlParser');

function inferFormat(filePath) {
  const ext = path.extname(filePath || '').toLowerCase();
  if (ext === '.csv') return 'csv';
  if (ext === '.json') return 'json';
  if (ext === '.xml') return 'xml';
  if (ext === '.txt') return 'csv';
  return null;
}

async function parseFile(filePath, format, options = {}) {
  const resolvedFormat = (format || inferFormat(filePath))?.toLowerCase();
  if (!resolvedFormat) throw new Error('No se pudo inferir el formato. Use --format para especificarlo.');

  if (resolvedFormat === 'csv') return parseCSV(filePath, options);
  if (resolvedFormat === 'json') return parseJSON(filePath, options);
  if (resolvedFormat === 'xml') return parseXML(filePath, options);

  throw new Error(`Formato no soportado: ${resolvedFormat}`);
}

module.exports = {
  parseFile,
  inferFormat,
};
