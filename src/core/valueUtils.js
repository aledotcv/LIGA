function parseDateParts(str) {
  if (!str) return null;
  const value = str.toString().trim();

  const isoDate = /^(\d{4})-(\d{2})-(\d{2})$/;
  const isoDateTime = /^(\d{4})-(\d{2})-(\d{2})[ tT](\d{2}):(\d{2})(?::(\d{2}))?/;
  const slashDate = /^(\d{2})\/(\d{2})\/(\d{4})$/;
  const dashDate = /^(\d{2})-(\d{2})-(\d{4})$/;

  let match = value.match(isoDateTime);
  if (match) {
    return {
      year: Number(match[1]),
      month: Number(match[2]),
      day: Number(match[3]),
      hour: Number(match[4]),
      minute: Number(match[5]),
      second: Number(match[6] || 0),
      hasTime: true,
    };
  }

  match = value.match(isoDate);
  if (match) {
    return {
      year: Number(match[1]),
      month: Number(match[2]),
      day: Number(match[3]),
      hour: 0,
      minute: 0,
      second: 0,
      hasTime: false,
    };
  }

  match = value.match(slashDate) || value.match(dashDate);
  if (match) {
    return {
      year: Number(match[3]),
      month: Number(match[2]),
      day: Number(match[1]),
      hour: 0,
      minute: 0,
      second: 0,
      hasTime: false,
    };
  }

  return null;
}

function formatToMysqlDate(parts) {
  const pad = (v) => String(v).padStart(2, '0');
  return `${parts.year}-${pad(parts.month)}-${pad(parts.day)}`;
}

function formatToMysqlDateTime(parts) {
  const date = formatToMysqlDate(parts);
  const pad = (v) => String(v).padStart(2, '0');
  return `${date} ${pad(parts.hour || 0)}:${pad(parts.minute || 0)}:${pad(parts.second || 0)}`;
}

function normalizeValueForColumn(value, column) {
  if (value === undefined || value === null) return null;
  if (typeof value === 'string' && (value.trim() === '' || value.trim().toLowerCase() === 'null')) return null;

  const type = (column.sqlType || '').toUpperCase();

  // procesar booleanos antes que enteros para TINYINT(1)
  if (type.includes('BOOL') || type.includes('TINYINT(1)') || column.inferredKind === 'boolean') {
    const lower = value.toString().toLowerCase();
    return ['true', 'yes', 'si', 'y', '1'].includes(lower) || value === true ? 1 : 0;
  }

  if (type.includes('DATE')) {
    if (value instanceof Date) {
      const parts = {
        year: value.getUTCFullYear(),
        month: value.getUTCMonth() + 1,
        day: value.getUTCDate(),
        hour: value.getUTCHours(),
        minute: value.getUTCMinutes(),
        second: value.getUTCSeconds(),
        hasTime: true,
      };
      return type === 'DATE' ? formatToMysqlDate(parts) : formatToMysqlDateTime(parts);
    }
    const parts = parseDateParts(value);
    if (parts) {
      if (type === 'DATE') return formatToMysqlDate(parts);
      return formatToMysqlDateTime(parts);
    }
    return value.toString();
  }

  if (type.includes('INT') || type.includes('DECIMAL') || type.includes('FLOAT')) {
    const sanitized = value.toString().replace(',', '.');
    const numeric = Number(sanitized);
    if (Number.isNaN(numeric)) return null;
    return numeric;
  }

  return value.toString();
}

function escapeSqlValue(value) {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'number') return Number.isFinite(value) ? value : 'NULL';
  const str = value.toString().replace(/\\/g, '\\\\').replace(/'/g, "''");
  return `'${str}'`;
}

module.exports = {
  parseDateParts,
  formatToMysqlDate,
  formatToMysqlDateTime,
  normalizeValueForColumn,
  escapeSqlValue,
};
