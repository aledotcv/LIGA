const odbc = require('odbc');
const path = require('path');

/**
 conecta a un archivo access via ODBC
 */
async function connectToAccess(filePath) {
  if (!filePath) {
    throw new Error('Ruta de archivo Access no especificada');
  }

  const ext = path.extname(filePath).toLowerCase();
  if (!['.mdb', '.accdb'].includes(ext)) {
    throw new Error(`Formato no soportado: ${ext}. Use .mdb o .accdb`);
  }

  // para Access
  const driver = ext === '.accdb' 
    ? 'Microsoft Access Driver (*.mdb, *.accdb)'
    : 'Microsoft Access Driver (*.mdb)';

  const connectionString = `Driver={${driver}};DBQ=${filePath};`;

  try {
    const connection = await odbc.connect(connectionString);
    return connection;
  } catch (err) {
    throw new Error(`Error al conectar a Access: ${err.message}. Asegúrese de tener instalado el driver ODBC de Access.`);
  }
}

/**
 lista todas las tablas disponibles en el archivo Access
 */
async function listAccessTables(connection) {
  try {
    const result = await connection.query(`
      SELECT MSysObjects.Name AS table_name
      FROM MSysObjects
      WHERE (((MSysObjects.Type)=1 AND (MSysObjects.Flags)=0))
      ORDER BY MSysObjects.Name;
    `);
    
    return result.map((row) => row.table_name || row.TABLE_NAME);
  } catch (err) {
    // fallback: usar el metodo tables() de odbc
    try {
      const tables = await connection.tables(null, null, null, 'TABLE');
      return tables.map((t) => t.TABLE_NAME).filter((name) => !name.startsWith('MSys'));
    } catch (fallbackErr) {
      throw new Error(`Error al listar tablas: ${err.message}`);
    }
  }
}

/**
 lee datos de una tabla específica en Access
 */
async function readAccessTable(connection, tableName) {
  try {
    const rows = await connection.query(`SELECT * FROM [${tableName}]`);
    return rows;
  } catch (err) {
    throw new Error(`Error al leer tabla '${tableName}': ${err.message}`);
  }
}

/**
 obtiene la estructura de una tabla (columnas y tipos)
 */
async function getAccessTableSchema(connection, tableName) {
  try {
    const columns = await connection.columns(null, null, tableName, null);
    
    return columns.map((col) => ({
      name: col.COLUMN_NAME,
      type: col.TYPE_NAME,
      size: col.COLUMN_SIZE,
      nullable: col.NULLABLE === 1,
      dataType: mapAccessTypeToMySQL(col.TYPE_NAME, col.COLUMN_SIZE),
    }));
  } catch (err) {
    throw new Error(`Error al obtener esquema de '${tableName}': ${err.message}`);
  }
}

/**
 mapea tipos de Access a tipos mysql
 */
function mapAccessTypeToMySQL(accessType, size) {
  const typeUpper = (accessType || '').toUpperCase();

  const typeMap = {
    'VARCHAR': size && size > 0 ? `VARCHAR(${Math.min(size, 65535)})` : 'VARCHAR(255)',
    'CHAR': size && size > 0 ? `CHAR(${Math.min(size, 255)})` : 'CHAR(255)',
    'TEXT': 'TEXT',
    'LONGTEXT': 'LONGTEXT',
    'MEMO': 'TEXT',
    'INTEGER': 'INT',
    'LONG': 'INT',
    'SHORT': 'SMALLINT',
    'BYTE': 'TINYINT',
    'SINGLE': 'FLOAT',
    'DOUBLE': 'DOUBLE',
    'CURRENCY': 'DECIMAL(19,4)',
    'DECIMAL': 'DECIMAL(10,2)',
    'NUMERIC': 'DECIMAL(10,2)',
    'BIT': 'TINYINT(1)',
    'YESNO': 'TINYINT(1)',
    'DATETIME': 'DATETIME',
    'DATE': 'DATE',
    'TIME': 'TIME',
    'TIMESTAMP': 'TIMESTAMP',
    'BINARY': 'BLOB',
    'VARBINARY': 'BLOB',
    'LONGBINARY': 'LONGBLOB',
    'GUID': 'VARCHAR(36)',
  };

  for (const [key, value] of Object.entries(typeMap)) {
    if (typeUpper.includes(key)) {
      return value;
    }
  }

  return 'VARCHAR(255)'; // default
}

/**
 * parsea archivo Access completo
 */
async function parseAccessFile(filePath, options = {}) {
  const { selectedTables = null } = options;
  
  const connection = await connectToAccess(filePath);
  
  try {
    const allTables = await listAccessTables(connection);
    const tablesToProcess = selectedTables || allTables;
    
    const results = [];
    
    for (const tableName of tablesToProcess) {
      if (!allTables.includes(tableName)) {
        throw new Error(`Tabla '${tableName}' no encontrada en el archivo Access`);
      }
      
      const rows = await readAccessTable(connection, tableName);
      const schema = await getAccessTableSchema(connection, tableName);
      
      results.push({
        tableName,
        rows,
        schema,
        meta: {
          format: 'access',
          rowCount: rows.length,
          source: filePath,
        },
      });
    }
    
    return {
      tables: results,
      availableTables: allTables,
    };
  } finally {
    await connection.close();
  }
}

module.exports = {
  connectToAccess,
  listAccessTables,
  readAccessTable,
  getAccessTableSchema,
  mapAccessTypeToMySQL,
  parseAccessFile,
};
