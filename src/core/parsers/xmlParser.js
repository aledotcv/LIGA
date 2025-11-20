const fs = require('fs');
const { parseStringPromise } = require('xml2js');
const { detectEncoding, decodeBuffer, flattenRecord } = require('../utils');

function findRecordArray(obj) {
  if (Array.isArray(obj)) return obj;
  if (!obj || typeof obj !== 'object') return null;

  for (const value of Object.values(obj)) {
    if (Array.isArray(value)) return value;
    if (value && typeof value === 'object') {
      const nestedArray = findRecordArray(value);
      if (nestedArray) return nestedArray;
    }
  }
  return null;
}

async function parseXML(filePath, options = {}) {
  const buffer = fs.readFileSync(filePath);
  const encoding = (options.encoding || detectEncoding(buffer).encoding || 'utf-8').toLowerCase();
  const content = decodeBuffer(buffer, encoding);

  let parsed;
  try {
    parsed = await parseStringPromise(content, {
      explicitArray: false,
      mergeAttrs: true,
      explicitRoot: true,
      attrValueProcessors: [
        (val) => val,
      ],
      tagNameProcessors: [(name) => name],
    });
  } catch (err) {
    throw new Error(`Error al parsear XML: ${err.message}`);
  }

  const records = findRecordArray(parsed);
  if (!records) {
    throw new Error('No se encontraron elementos repetidos en el XML para construir registros.');
  }

  const rows = records.map(flattenRecord);
  return {
    rows,
    meta: {
      format: 'xml',
      encoding,
      rowCount: rows.length,
    },
  };
}

module.exports = { parseXML };
