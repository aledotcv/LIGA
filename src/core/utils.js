const path = require('path');
const jschardet = require('jschardet');
const iconv = require('iconv-lite');

function sanitizeName(name, fallbackPrefix = 'col', suffix = '') {
  const raw = (name || '').toString().trim();
  const base = raw
    .normalize('NFKD')
    .replace(/[^\w]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase();

  let candidate = base || `${fallbackPrefix}${suffix ? `_${suffix}` : ''}`;
  if (/^\d/.test(candidate)) {
    candidate = `${fallbackPrefix}_${candidate}`;
  }
  return candidate || `${fallbackPrefix}_${Date.now()}`;
}

function deriveTableName(filePath, override) {
  if (override) return sanitizeName(override, 'table');
  const base = path.basename(filePath).replace(path.extname(filePath), '');
  return sanitizeName(base || 'table', 'table');
}

function flattenRecord(record = {}) {
  const flat = {};
  for (const [key, value] of Object.entries(record)) {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      for (const [childKey, childVal] of Object.entries(value)) {
        flat[`${key}_${childKey}`] = childVal;
      }
    } else {
      flat[key] = value;
    }
  }
  return flat;
}

function detectDelimiter(sampleText) {
  const lines = sampleText.split(/\r?\n/).filter(Boolean).slice(0, 5);
  const candidates = [',', ';', '\t'];
  let best = ',';
  let bestScore = -1;

  for (const candidate of candidates) {
    const score = lines.reduce((total, line) => total + (line.split(candidate).length - 1), 0);
    if (score > bestScore) {
      bestScore = score;
      best = candidate;
    }
  }
  return best;
}

function detectEncoding(buffer) {
  const detection = jschardet.detect(buffer);
  let encoding = (detection && detection.encoding ? detection.encoding : 'utf-8').toLowerCase();
  if (encoding.includes('iso-8859-1') || encoding.includes('windows-1252') || encoding === 'latin1') {
    encoding = 'latin1';
  } else {
    encoding = 'utf-8';
  }
  return { encoding, confidence: detection?.confidence || 0 };
}

function decodeBuffer(buffer, preferredEncoding) {
  const { encoding } = detectEncoding(buffer);
  const selected = preferredEncoding ? preferredEncoding.toLowerCase() : encoding;
  if (!iconv.encodingExists(selected)) {
    return buffer.toString('utf-8');
  }
  return iconv.decode(buffer, selected);
}

module.exports = {
  sanitizeName,
  deriveTableName,
  flattenRecord,
  detectDelimiter,
  detectEncoding,
  decodeBuffer,
};
