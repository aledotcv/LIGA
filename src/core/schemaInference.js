const { sanitizeName } = require('./utils');

const MAX_UNIQUE_SAMPLE = 10000;

function isNullish(value) {
  return (
    value === null ||
    value === undefined ||
    (typeof value === 'string' && (value.trim() === '' || value.trim().toLowerCase() === 'null'))
  );
}

function parseDateCandidate(value) {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  const patterns = [
    { regex: /^\d{4}-\d{2}-\d{2}$/, type: 'date' },
    { regex: /^\d{4}-\d{2}-\d{2}[ tT]\d{2}:\d{2}(:\d{2})?$/, type: 'datetime' },
    { regex: /^\d{2}\/\d{2}\/\d{4}$/, type: 'date' },
    { regex: /^\d{2}-\d{2}-\d{4}$/, type: 'date' },
  ];

  for (const pattern of patterns) {
    if (pattern.regex.test(trimmed)) {
      const normalized = normalizeDateString(trimmed);
      const parsedDate = normalized ? new Date(normalized) : new Date(trimmed);
      if (!Number.isNaN(parsedDate.getTime())) {
        return { type: pattern.type, normalized: normalized || trimmed };
      }
    }
  }
  return null;
}

function normalizeDateString(value) {
  if (/^\d{2}\/\d{2}\/\d{4}$/.test(value)) {
    const [d, m, y] = value.split('/');
    return `${y}-${m}-${d}`;
  }
  if (/^\d{2}-\d{2}-\d{4}$/.test(value)) {
    const [d, m, y] = value.split('-');
    return `${y}-${m}-${d}`;
  }
  return null;
}

function analyzeValue(value) {
  if (isNullish(value)) return { kind: 'null' };

  if (typeof value === 'boolean') {
    return { kind: 'boolean', normalized: value ? 1 : 0 };
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return { kind: 'integer', numeric: value };
    }
    return { kind: 'decimal', numeric: value };
  }

  const str = value.toString().trim();
  const lower = str.toLowerCase();

  if (['true', 'false', 'yes', 'no', 'si', 'sí', '1', '0'].includes(lower)) {
    return { kind: 'boolean', normalized: ['true', 'yes', 'si', 'sí', '1'].includes(lower) ? 1 : 0 };
  }

  const numericCandidate = str.replace(',', '.');
  if (/^-?\d+$/.test(numericCandidate)) {
    const asNumber = Number(numericCandidate);
    return { kind: 'integer', numeric: asNumber };
  }
  if (/^-?\d+\.\d+$/.test(numericCandidate)) {
    const asNumber = Number(numericCandidate);
    return { kind: 'decimal', numeric: asNumber, raw: numericCandidate };
  }

  const dateCandidate = parseDateCandidate(str);
  if (dateCandidate) {
    return { kind: dateCandidate.type, normalized: dateCandidate.normalized };
  }

  return { kind: 'text', normalized: str };
}

function initStats(rawName, safeName) {
  return {
    rawName,
    name: safeName,
    total: 0,
    nulls: 0,
    uniqueValues: new Set(),
    uniqueCapped: false,
    maxLength: 0,
    intCount: 0,
    decimalCount: 0,
    boolCount: 0,
    dateCount: 0,
    datetimeCount: 0,
    textCount: 0,
    intMin: null,
    intMax: null,
    decimalPrecision: 0,
    decimalScale: 0,
  };
}

function updateStats(stats, value) {
  stats.total += 1;
  if (isNullish(value)) {
    stats.nulls += 1;
    return;
  }

  const { kind, numeric, normalized, raw } = analyzeValue(value);
  const asString = normalized !== undefined ? normalized.toString() : value?.toString?.() || '';

  if (!stats.uniqueCapped) {
    stats.uniqueValues.add(asString);
    if (stats.uniqueValues.size > MAX_UNIQUE_SAMPLE) {
      stats.uniqueCapped = true;
      stats.uniqueValues.clear();
    }
  }

  if (kind === 'integer') {
    stats.intCount += 1;
    stats.intMin = stats.intMin === null ? numeric : Math.min(stats.intMin, numeric);
    stats.intMax = stats.intMax === null ? numeric : Math.max(stats.intMax, numeric);
    stats.maxLength = Math.max(stats.maxLength, Math.abs(Math.trunc(numeric)).toString().length);
  } else if (kind === 'decimal') {
    stats.decimalCount += 1;
    const valueStr = (raw || numeric.toString()).replace('-', '');
    const [intPart, decimalPart] = valueStr.split('.');
    const precision = (intPart ? intPart.length : 0) + (decimalPart ? decimalPart.length : 0);
    const scale = decimalPart ? decimalPart.length : 0;
    stats.decimalPrecision = Math.max(stats.decimalPrecision, precision);
    stats.decimalScale = Math.max(stats.decimalScale, scale);
    stats.maxLength = Math.max(stats.maxLength, valueStr.length);
  } else if (kind === 'boolean') {
    stats.boolCount += 1;
    stats.maxLength = Math.max(stats.maxLength, asString.length);
  } else if (kind === 'date') {
    stats.dateCount += 1;
    stats.maxLength = Math.max(stats.maxLength, asString.length);
  } else if (kind === 'datetime') {
    stats.datetimeCount += 1;
    stats.maxLength = Math.max(stats.maxLength, asString.length);
  } else {
    stats.textCount += 1;
    stats.maxLength = Math.max(stats.maxLength, asString.length);
  }
}

function finalizeType(stats) {
  const nonNull = stats.total - stats.nulls;
  if (nonNull === 0) {
    return { sqlType: 'VARCHAR(255)', kind: 'text' };
  }

  if (stats.boolCount === nonNull) {
    return { sqlType: 'TINYINT(1)', kind: 'boolean' };
  }

  if (stats.dateCount + stats.datetimeCount === nonNull) {
    return { sqlType: stats.datetimeCount > 0 ? 'DATETIME' : 'DATE', kind: 'date' };
  }

  if (stats.decimalCount === nonNull) {
    const precision = Math.min(Math.max(stats.decimalPrecision || stats.maxLength || 10, 4), 30);
    const scale = Math.min(Math.max(stats.decimalScale, 0), 14);
    return { sqlType: `DECIMAL(${precision},${scale})`, kind: 'decimal', precision, scale };
  }

  if (stats.intCount === nonNull) {
    const needsBigInt = Math.abs(stats.intMin || 0) > 2147483647 || Math.abs(stats.intMax || 0) > 2147483647;
    return { sqlType: needsBigInt ? 'BIGINT' : 'INT', kind: 'integer' };
  }

  if (stats.intCount + stats.decimalCount === nonNull) {
    const precision = Math.min(Math.max(stats.decimalPrecision || stats.maxLength || 10, 4), 30);
    const scale = Math.min(Math.max(stats.decimalScale, 0), 14);
    return { sqlType: `DECIMAL(${precision},${scale})`, kind: 'decimal', precision, scale };
  }

  const maxLen = Math.max(stats.maxLength || 1, 1);
  if (maxLen > 1000) {
    return { sqlType: 'TEXT', kind: 'text', length: maxLen };
  }
  return { sqlType: `VARCHAR(${Math.max(maxLen, 1)})`, kind: 'text', length: maxLen };
}

function ensureUniqueName(candidate, used) {
  let name = candidate;
  let idx = 1;
  while (used.has(name)) {
    idx += 1;
    name = `${candidate}_${idx}`;
  }
  used.add(name);
  return name;
}

function inferSchema(rows, options = {}) {
  const usedNames = new Set();
  const columnNames = new Set();
  rows.forEach((row) => {
    Object.keys(row || {}).forEach((key) => columnNames.add(key));
  });

  const statsMap = new Map();
  Array.from(columnNames).forEach((rawName, idx) => {
    const safe = ensureUniqueName(sanitizeName(rawName, 'col', idx + 1), usedNames);
    statsMap.set(rawName, initStats(rawName, safe));
  });

  rows.forEach((row) => {
    statsMap.forEach((stats, rawName) => {
      updateStats(stats, row[rawName]);
    });
  });

  const columns = Array.from(statsMap.values()).map((stats) => {
    const typeInfo = finalizeType(stats);
    const uniqueCandidate =
      !stats.uniqueCapped &&
      stats.uniqueValues.size > 0 &&
      stats.uniqueValues.size === stats.total - stats.nulls &&
      stats.nulls === 0;
    return {
      rawName: stats.rawName,
      name: stats.name,
      sqlType: typeInfo.sqlType,
      nullable: stats.nulls > 0,
      unique: uniqueCandidate,
      maxLength: stats.maxLength,
      inferredKind: typeInfo.kind,
    };
  });

  const primaryKeys = [];
  const pkCandidate = columns.find((col) => col.unique && !col.nullable);
  if (pkCandidate) {
    pkCandidate.isPrimaryKey = true;
    primaryKeys.push(pkCandidate.name);
  } else {
    const synthetic = ensureUniqueName('id', usedNames);
    columns.unshift({
      rawName: synthetic,
      name: synthetic,
      sqlType: 'INT',
      nullable: false,
      unique: true,
      isPrimaryKey: true,
      autoIncrement: true,
      inferredKind: 'integer',
    });
    primaryKeys.push(synthetic);
  }

  return {
    columns,
    primaryKeys,
  };
}

module.exports = {
  inferSchema,
};
