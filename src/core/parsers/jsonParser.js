const fs = require('fs');
const { detectEncoding, decodeBuffer, flattenRecord } = require('../utils');

function coerceToArray(data) {
  if (Array.isArray(data)) return data;
  if (data && typeof data === 'object') {
    const firstArray = Object.values(data).find(Array.isArray);
    if (firstArray) return firstArray;
  }
  return null;
}

function attemptNdjson(content) {
  const rows = [];
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      rows.push(JSON.parse(trimmed));
    } catch (err) {
      return null;
    }
  }
  return rows.length ? rows : null;
}

function parseJSON(filePath, options = {}) {
  const buffer = fs.readFileSync(filePath);
  const encoding = (options.encoding || detectEncoding(buffer).encoding || 'utf-8').toLowerCase();
  const content = decodeBuffer(buffer, encoding);

  let parsed;
  try {
    parsed = JSON.parse(content);
  } catch (err) {
    const ndjsonRows = attemptNdjson(content);
    if (ndjsonRows) {
      parsed = ndjsonRows;
    } else {
      throw new Error(`Error al parsear JSON: ${err.message}`);
    }
  }

  const records = coerceToArray(parsed);
  if (!records) {
    throw new Error('Formato JSON no reconocido. Se espera un arreglo de objetos o NDJSON.');
  }

  const rows = records.map(flattenRecord);
  return {
    rows,
    meta: {
      format: 'json',
      encoding,
      rowCount: rows.length,
    },
  };
}

module.exports = { parseJSON };
