const fs = require('fs');
const path = require('path');

/**
 carga configuración de transformaciones desde archivo JSON
 */
function loadTransformConfig(configPath) {
  if (!configPath || !fs.existsSync(configPath)) {
    return null;
  }

  try {
    const content = fs.readFileSync(configPath, 'utf-8');
    return JSON.parse(content);
  } catch (err) {
    throw new Error(`Error al cargar configuración de transformaciones: ${err.message}`);
  }
}

/**
 aplica transformaciones de texto a un valor
 */
function applyTextTransformations(value, transformations = []) {
  if (value === null || value === undefined) return value;
  let result = String(value);

  transformations.forEach((transform) => {
    switch (transform.type) {
      case 'upper':
      case 'UPPER':
        result = result.toUpperCase();
        break;
      case 'lower':
      case 'LOWER':
        result = result.toLowerCase();
        break;
      case 'trim':
      case 'TRIM':
        result = result.trim();
        break;
      case 'replace':
        if (transform.from !== undefined && transform.to !== undefined) {
          const regex = transform.regex
            ? new RegExp(transform.from, 'g')
            : new RegExp(transform.from.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g');
          result = result.replace(regex, transform.to);
        }
        break;
      case 'substring':
        if (transform.start !== undefined) {
          result = result.substring(transform.start, transform.end);
        }
        break;
      case 'titleCase':
        result = result
          .toLowerCase()
          .split(' ')
          .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
          .join(' ');
        break;
      default:
        // transformación desconocida, ignorar
        break;
    }
  });

  return result;
}

/**
 alica mapeo de valores categóricos
 */
function applyValueMapping(value, mapping = {}) {
  if (value === null || value === undefined) return value;

  const key = String(value).trim();
  if (mapping.hasOwnProperty(key)) {
    return mapping[key];
  }

  // intentar búsqueda case-insensitive
  const lowerKey = key.toLowerCase();
  for (const [mapKey, mapValue] of Object.entries(mapping)) {
    if (mapKey.toLowerCase() === lowerKey) {
      return mapValue;
    }
  }

  return value;
}

/**
 * renombrar columnas según configuración
 */
function applyColumnRenames(rows, renameMap = {}) {
  if (!renameMap || Object.keys(renameMap).length === 0) {
    return rows;
  }

  return rows.map((row) => {
    const newRow = {};
    Object.entries(row).forEach(([key, value]) => {
      const newKey = renameMap[key] || key;
      newRow[newKey] = value;
    });
    return newRow;
  });
}

/**
 aplica todas las transformaciones configuradas a un conjunto de datos
 */
function applyTransformations(rows, config = null) {
  if (!config) return rows;

  let transformedRows = [...rows];

  // renombrar columnas si está configurado
  if (config.columnRenames) {
    transformedRows = applyColumnRenames(transformedRows, config.columnRenames);
  }

  // aplicar transformaciones de texto por columna
  if (config.textTransformations) {
    transformedRows = transformedRows.map((row) => {
      const transformedRow = { ...row };
      Object.entries(config.textTransformations).forEach(([column, transformations]) => {
        if (transformedRow.hasOwnProperty(column)) {
          transformedRow[column] = applyTextTransformations(transformedRow[column], transformations);
        }
      });
      return transformedRow;
    });
  }

  //aplicar mapeos de valores categóricos
  if (config.valueMappings) {
    transformedRows = transformedRows.map((row) => {
      const transformedRow = { ...row };
      Object.entries(config.valueMappings).forEach(([column, mapping]) => {
        if (transformedRow.hasOwnProperty(column)) {
          transformedRow[column] = applyValueMapping(transformedRow[column], mapping);
        }
      });
      return transformedRow;
    });
  }

  // transformaciones globales (aplicar a todas las columnas de texto)
  if (config.globalTextTransformations) {
    transformedRows = transformedRows.map((row) => {
      const transformedRow = {};
      Object.entries(row).forEach(([key, value]) => {
        if (typeof value === 'string') {
          transformedRow[key] = applyTextTransformations(value, config.globalTextTransformations);
        } else {
          transformedRow[key] = value;
        }
      });
      return transformedRow;
    });
  }

  return transformedRows;
}

/**
 generar archivo de configuración de ejemplo
 */
function generateSampleConfig(outputPath) {
  const sampleConfig = {
    columnRenames: {
      old_column_name: 'new_column_name',
      customer_name: 'client_name',
    },
    textTransformations: {
      first_name: [{ type: 'UPPER' }, { type: 'TRIM' }],
      email: [{ type: 'lower' }],
      description: [
        { type: 'trim' },
        { type: 'replace', from: '  ', to: ' ', regex: false },
      ],
    },
    valueMappings: {
      status: {
        SI: 1,
        NO: 0,
        Sí: 1,
        'sí': 1,
        si: 1,
        no: 0,
        YES: 1,
        NO: 0,
        yes: 1,
        Y: 1,
        N: 0,
      },
      gender: {
        M: 'Male',
        F: 'Female',
        H: 'Male',
        M: 'Female',
        Masculino: 'Male',
        Femenino: 'Female',
      },
    },
    globalTextTransformations: [{ type: 'trim' }],
  };

  const outDir = path.dirname(outputPath);
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outputPath, JSON.stringify(sampleConfig, null, 2), 'utf-8');

  return sampleConfig;
}

module.exports = {
  loadTransformConfig,
  applyTransformations,
  applyTextTransformations,
  applyValueMapping,
  applyColumnRenames,
  generateSampleConfig,
};
