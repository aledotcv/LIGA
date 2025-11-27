const { parseDateParts } = require('./valueUtils');

/**
 valida un conjunto de datos antes de la inserción
  detecta duplicados, valores inválidos y problemas de integridad
 */
function validateData(rows, schema, options = {}) {
  const issues = [];
  const { checkDuplicates = true, checkInvalidValues = true } = options;

  // detectar valores duplicados en columnas únicas
  if (checkDuplicates) {
    const uniqueColumns = schema.columns.filter((col) => col.unique || col.isPrimaryKey);
    uniqueColumns.forEach((col) => {
      const seen = new Map();
      rows.forEach((row, index) => {
        const value = row[col.rawName];
        if (value !== null && value !== undefined && value !== '') {
          const key = String(value);
          if (seen.has(key)) {
            issues.push({
              type: 'duplicate',
              severity: 'error',
              column: col.name,
              index,
              value,
              message: `Valor duplicado en columna única '${col.name}': ${value}`,
              firstOccurrence: seen.get(key),
            });
          } else {
            seen.set(key, index);
          }
        }
      });
    });
  }

  // identificar valores fuera de rango o inválidos
  if (checkInvalidValues) {
    rows.forEach((row, index) => {
      schema.columns.forEach((col) => {
        const value = row[col.rawName];
        if (value === null || value === undefined || value === '') return;

        const sqlType = (col.sqlType || '').toUpperCase();

        // validar fechas
        if (sqlType.includes('DATE')) {
          const parts = parseDateParts(value);
          if (parts) {
            // verificar fecha válida
            const { year, month, day } = parts;
            const date = new Date(year, month - 1, day);
            if (
              date.getFullYear() !== year ||
              date.getMonth() !== month - 1 ||
              date.getDate() !== day
            ) {
              issues.push({
                type: 'invalid_date',
                severity: 'error',
                column: col.name,
                index,
                value,
                message: `Fecha inválida en '${col.name}': ${value} (ej: 30 de febrero)`,
              });
            }

            // validar rango razonable (1900-2100)
            if (year < 1900 || year > 2100) {
              issues.push({
                type: 'date_out_of_range',
                severity: 'warning',
                column: col.name,
                index,
                value,
                message: `Fecha fuera de rango esperado en '${col.name}': ${value}`,
              });
            }
          } else if (typeof value === 'string') {
            issues.push({
              type: 'unparseable_date',
              severity: 'error',
              column: col.name,
              index,
              value,
              message: `Formato de fecha no reconocido en '${col.name}': ${value}`,
            });
          }
        }

        // validar números
        if (sqlType.includes('INT') || sqlType.includes('DECIMAL')) {
          const numValue = Number(String(value).replace(',', '.'));
          if (Number.isNaN(numValue)) {
            issues.push({
              type: 'invalid_number',
              severity: 'error',
              column: col.name,
              index,
              value,
              message: `Valor numérico inválido en '${col.name}': ${value}`,
            });
          }

          // detectar negativos en columnas que probablemente no deberían tenerlos
          if (numValue < 0) {
            const likelyPositive = ['id', 'age', 'count', 'quantity', 'amount', 'price', 'total'];
            if (likelyPositive.some((keyword) => col.name.toLowerCase().includes(keyword))) {
              issues.push({
                type: 'negative_value',
                severity: 'warning',
                column: col.name,
                index,
                value: numValue,
                message: `Valor negativo en columna que probablemente debería ser positiva '${col.name}': ${numValue}`,
              });
            }
          }

          // validar rangos INT
          if (sqlType === 'INT') {
            if (numValue < -2147483648 || numValue > 2147483647) {
              issues.push({
                type: 'int_overflow',
                severity: 'error',
                column: col.name,
                index,
                value: numValue,
                message: `Valor fuera de rango INT en '${col.name}': ${numValue}`,
              });
            }
          }
        }

        // validar longitud de strings
        if (sqlType.includes('VARCHAR')) {
          const strValue = String(value);
          const match = sqlType.match(/VARCHAR\((\d+)\)/);
          if (match) {
            const maxLength = parseInt(match[1], 10);
            if (strValue.length > maxLength) {
              issues.push({
                type: 'string_too_long',
                severity: 'error',
                column: col.name,
                index,
                value: strValue.substring(0, 50) + '...',
                message: `Texto excede longitud máxima en '${col.name}': ${strValue.length} > ${maxLength}`,
              });
            }
          }
        }
      });
    });
  }

  return issues;
}

/**
 * valida integridad referencial entre múltiples tablas
 */
function validateReferentialIntegrity(tables, options = {}) {
  const issues = [];

  tables.forEach((table) => {
    const { tableName, rows, schema, foreignKeys = [] } = table;

    foreignKeys.forEach((fk) => {
      const { column, referencesTable, referencesColumn } = fk;
      const referencedTable = tables.find((t) => t.tableName === referencesTable);

      if (!referencedTable) {
        issues.push({
          type: 'missing_referenced_table',
          severity: 'error',
          table: tableName,
          column,
          message: `Tabla referenciada '${referencesTable}' no encontrada`,
        });
        return;
      }

      // construir set de valores válidos en la tabla referenciada
      const validValues = new Set();
      referencedTable.rows.forEach((row) => {
        const value = row[referencesColumn];
        if (value !== null && value !== undefined) {
          validValues.add(String(value));
        }
      });

      // verificar que todos los FK apunten a valores existentes
      rows.forEach((row, index) => {
        const fkValue = row[column];
        if (fkValue !== null && fkValue !== undefined && fkValue !== '') {
          if (!validValues.has(String(fkValue))) {
            issues.push({
              type: 'orphan_foreign_key',
              severity: 'error',
              table: tableName,
              column,
              index,
              value: fkValue,
              message: `FK en '${tableName}.${column}' apunta a valor inexistente en '${referencesTable}.${referencesColumn}': ${fkValue}`,
            });
          }
        }
      });
    });
  });

  return issues;
}

/**
 genera reporte detallado de problemas de validación
 */
function generateValidationReport(issues, outputPath) {
  const fs = require('fs');
  const path = require('path');

  const report = {
    timestamp: new Date().toISOString(),
    totalIssues: issues.length,
    errorCount: issues.filter((i) => i.severity === 'error').length,
    warningCount: issues.filter((i) => i.severity === 'warning').length,
    issuesByType: {},
    issues: issues.map((issue) => ({
      ...issue,
      row: undefined, // no incluir datos completos de la fila en el reporte
    })),
  };

  //agrupar por tipo
  issues.forEach((issue) => {
    if (!report.issuesByType[issue.type]) {
      report.issuesByType[issue.type] = 0;
    }
    report.issuesByType[issue.type] += 1;
  });

  if (outputPath) {
    const outDir = path.dirname(outputPath);
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(outputPath, JSON.stringify(report, null, 2), 'utf-8');
  }

  return report;
}

module.exports = {
  validateData,
  validateReferentialIntegrity,
  generateValidationReport,
};
