# LIGA - Database Migration Toolkit

**L**ee, **I**nfiere, **G**enera y **A**plica

A comprehensive toolkit for migrating data from CSV, JSON, XML, Access databases, and SQL dumps into MySQL with automatic schema inference.

## Features

- **Multi-format Import**: CSV, JSON, XML, and Microsoft Access (.mdb/.accdb)
- **SQL File Export**: Convert SQL dump files to CSV/JSON/XML
- **Schema Inference**: Automatically detects column types, constraints, and relationships
- **Data Validation**: Duplicate detection, value validation, and integrity checks
- **Batch Processing**: Process entire directories of files
- **Foreign Key Detection**: Automatically infers relationships between tables
- **Stored Procedure Generation**: Auto-generate CRUD stored procedures
- **MySQL Export**: Export tables or SQL query results to CSV/JSON/XML
- **GUI and CLI**: Electron desktop application and command-line interface
- **Compression**: Optional .zip compression for output files

## Installation

### Prerequisites

- Node.js 18+ and npm
- MySQL 5.7+ or 8.0+
- (Optional) ODBC drivers for Access file support

### Install Dependencies

```bash
npm install
```

## Usage

### GUI Application

Launch the Electron desktop application:

```bash
npm start
```

**Import Mode (Archivo → MySQL)**:
1. Select operation mode: "Importar archivo → MySQL"
2. Click "Seleccionar origen" to choose a file or directory
3. Configure format, encoding, and table name (optional)
4. Set MySQL connection details
5. Enable optional features (validation, transformations, batch mode, etc.)
6. Click "Ejecutar migración"

**Export Mode (MySQL → Archivo)**:
1. Select operation mode: "Exportar MySQL → archivo"
2. Choose source type:
   - **Tabla**: Export from a MySQL table
   - **Archivo SQL**: Parse and convert a SQL dump file
3. For SQL files, select the file and specify the target table name
4. Choose output format (CSV, JSON, XML)
5. Set output path and optional compression
6. Click "Exportar tabla"

### Command Line Interface

```bash
node index.js [options]
```

#### CLI Options

- `-i, --input <path>` - Input file or directory path
- `-f, --format <type>` - Input format: csv, json, xml, access (default: auto-detect)
- `-t, --table <name>` - Target table name (default: derived from filename)
- `-H, --host <host>` - MySQL host (default: localhost)
- `-P, --port <port>` - MySQL port (default: 3306)
- `-u, --user <user>` - MySQL username (default: root)
- `-p, --password <pwd>` - MySQL password
- `-d, --database <db>` - MySQL database name (required)
- `-e, --encoding <enc>` - File encoding (default: auto-detect)
- `--ddl <path>` - DDL output path (default: output/schema.sql)
- `--insert <path>` - INSERT output path (default: output/inserts.sql)
- `--report <path>` - Report output path (default: output/report.json)
- `--bulk` - Use bulk insert (default: true)
- `--chunk-size <n>` - Bulk insert chunk size (default: 250)
- `--dry-run` - Generate DDL/INSERT without loading to MySQL
- `--validate` - Enable data validation
- `--batch` - Batch mode: process directory of files
- `--detect-fks` - Detect foreign key relationships
- `--generate-procedures` - Generate CRUD stored procedures
- `--compress` - Compress output files to .zip

#### CLI Example

**Import single CSV file:**
```bash
node index.js -i data/customers.csv -d mydb -u root -p password
```

## Building

### Build

The `electron-builder` configuration in `package.json` will create Windows installers:

```bash
npm run build
```

Output will be in the `dist/` directory.
