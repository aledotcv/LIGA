const fs = require('fs');
const path = require('path');
const { parseFile, inferFormat } = require('./parsers');

/**
 * Procesa múltiples archivos en lote desde un directorio
 */
async function processBatchFiles(dirPath, options = {}) {
  const { pattern = /\.(csv|json|xml)$/i, encoding } = options;

  if (!fs.existsSync(dirPath)) {
    throw new Error(`Directorio no encontrado: ${dirPath}`);
  }

  const stats = fs.statSync(dirPath);
  let filePaths = [];

  if (stats.isDirectory()) {
    // leer todos los archivos del directorio
    const files = fs.readdirSync(dirPath);
    filePaths = files
      .filter((file) => pattern.test(file))
      .map((file) => path.join(dirPath, file));
  } else {
    // rchivo individual
    filePaths = [dirPath];
  }

  if (filePaths.length === 0) {
    throw new Error('No se encontraron archivos válidos para procesar');
  }

  // parsear todos los archivos
  const results = [];
  for (const filePath of filePaths) {
    const format = inferFormat(filePath);
    const tableName = path.basename(filePath, path.extname(filePath));
    const { rows, meta } = await parseFile(filePath, format, { encoding });

    results.push({
      filePath,
      tableName,
      format,
      rows,
      meta,
    });
  }

  return results;
}

/**
 detecta foreign keys implícitas analizando nombres de columnas
 */
function detectImplicitForeignKeys(tables) {
  const foreignKeys = [];

  tables.forEach((table) => {
    const { tableName, schema } = table;

    schema.columns.forEach((column) => {
      const colName = column.name.toLowerCase();

      // patron: tabla_id o tablaId
      const patterns = [
        /^(.+)_id$/,
        /^(.+)Id$/,
        /^fk_(.+)$/,
        /^id_(.+)$/,
      ];

      for (const pattern of patterns) {
        const match = colName.match(pattern);
        if (match) {
          const referencedTableName = match[1];

          // buscar si existe una tabla con ese nombre
          const referencedTable = tables.find((t) => {
            const tName = t.tableName.toLowerCase();
            return (
              tName === referencedTableName ||
              tName === referencedTableName + 's' ||
              tName === referencedTableName.replace(/s$/, '')
            );
          });

          if (referencedTable) {
            // buscar la columna PK en la tabla referenciada
            const pkColumn = referencedTable.schema.columns.find((c) => c.isPrimaryKey);
            if (pkColumn) {
              foreignKeys.push({
                table: tableName,
                column: column.name,
                referencesTable: referencedTable.tableName,
                referencesColumn: pkColumn.name,
              });
            }
          }
          break;
        }
      }
    });
  });

  return foreignKeys;
}

/**
 ordena tablas por dependencias (padre → hijo)
 */
function topologicalSort(tables, foreignKeys) {
  const graph = new Map();
  const inDegree = new Map();

  // inicializar grafo
  tables.forEach((table) => {
    graph.set(table.tableName, []);
    inDegree.set(table.tableName, 0);
  });

  // construir grafo de dependencias
  foreignKeys.forEach((fk) => {
    // la tabla con FK depende de la tabla referenciada
    // arista: tabla referenciada --> tabla con FK
    if (graph.has(fk.referencesTable) && graph.has(fk.table)) {
      graph.get(fk.referencesTable).push(fk.table);
      inDegree.set(fk.table, inDegree.get(fk.table) + 1);
    }
  });

  // algo de kahn para ordenamiento topologico
  const queue = [];
  const sorted = [];

  // agregar tablas sin dependencias a la cola
  inDegree.forEach((degree, tableName) => {
    if (degree === 0) {
      queue.push(tableName);
    }
  });

  while (queue.length > 0) {
    const current = queue.shift();
    sorted.push(current);

    const neighbors = graph.get(current) || [];
    neighbors.forEach((neighbor) => {
      const newDegree = inDegree.get(neighbor) - 1;
      inDegree.set(neighbor, newDegree);
      if (newDegree === 0) {
        queue.push(neighbor);
      }
    });
  }

  //  verificar ciclos
  if (sorted.length !== tables.length) {
    // hay ciclos, retornar orden original
    return tables.map((t) => t.tableName);
  }

  return sorted;
}

/**
 generar sentencias ALTER TABLE para agregar FKs
 */
function generateForeignKeyConstraints(foreignKeys) {
  const constraints = foreignKeys.map((fk, index) => {
    const constraintName = `fk_${fk.table}_${fk.column}_${index}`;
    return `ALTER TABLE \`${fk.table}\`
  ADD CONSTRAINT \`${constraintName}\`
  FOREIGN KEY (\`${fk.column}\`)
  REFERENCES \`${fk.referencesTable}\` (\`${fk.referencesColumn}\`)
  ON DELETE RESTRICT
  ON UPDATE CASCADE;`;
  });

  return constraints.join('\n\n');
}

/**
 ordena tablas para inserción respetando dependencias
 */
function sortTablesForInsertion(tables) {
  // detectar FKs implicitas
  const foreignKeys = detectImplicitForeignKeys(tables);

  // ordenar topologicamente
  const sortedNames = topologicalSort(tables, foreignKeys);

  // reordenar array de tablas segun el orden
  const sorted = sortedNames
    .map((name) => tables.find((t) => t.tableName === name))
    .filter(Boolean);

  return {
    tables: sorted,
    foreignKeys,
    insertionOrder: sortedNames,
  };
}

module.exports = {
  processBatchFiles,
  detectImplicitForeignKeys,
  topologicalSort,
  generateForeignKeyConstraints,
  sortTablesForInsertion,
};
