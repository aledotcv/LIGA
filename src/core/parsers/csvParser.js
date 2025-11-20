const fs = require('fs');
const { parse } = require('csv-parse/sync');
const { detectDelimiter, detectEncoding, decodeBuffer, flattenRecord } = require('../utils');

function parseCSV(filePath, options = {}) {
  const buffer = fs.readFileSync(filePath);
  const encoding = (options.encoding || detectEncoding(buffer).encoding || 'utf-8').toLowerCase();
  const content = decodeBuffer(buffer, encoding);
  const delimiter = options.delimiter || detectDelimiter(content);

  let records;
  try {
    records = parse(content, {
      columns: true,
      skip_empty_lines: true,
      delimiter,
      trim: true,
      relax_column_count: true,
    });
  } catch (err) {
    throw new Error(`Error al parsear CSV: ${err.message}`);
  }

  const rows = records.map(flattenRecord);
  return {
    rows,
    meta: {
      format: 'csv',
      encoding,
      delimiter,
      rowCount: rows.length,
    },
  };
}

module.exports = { parseCSV };
