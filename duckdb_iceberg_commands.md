# 🦆 DuckDB Commands for Iceberg Table Inspection

## Prerequisites

```bash
# Install DuckDB Iceberg extension (run once)
duckdb -c "INSTALL iceberg;"
```

## Basic Setup (Required for Each Session)

```sql
-- Load the Iceberg extension
LOAD iceberg;

-- Enable version guessing (required for local Iceberg tables)
SET unsafe_enable_version_guessing = true;
```

## 1. 📋 Inspect Table Schema

```sql
-- View table schema
DESCRIBE SELECT * FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample');

-- Alternative schema inspection
PRAGMA table_info(iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample'));
```

## 2. 📊 Count Records in Tables

```sql
-- Count rows in a single table
SELECT COUNT(*) FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample');

-- Count rows in all tables
SELECT 'transactions_sample' as table_name, COUNT(*) as row_count 
FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample')
UNION ALL
SELECT 'flux_nouveaux_emprunts', COUNT(*) 
FROM iceberg_scan('data/iceberg_warehouse/my_data/flux_nouveaux_emprunts')
UNION ALL
SELECT 'foyers_fiscaux', COUNT(*) 
FROM iceberg_scan('data/iceberg_warehouse/my_data/foyers_fiscaux')
UNION ALL
SELECT 'loyers', COUNT(*) 
FROM iceberg_scan('data/iceberg_warehouse/my_data/loyers')
UNION ALL
SELECT 'parc_immobilier', COUNT(*) 
FROM iceberg_scan('data/iceberg_warehouse/my_data/parc_immobilier')
UNION ALL
SELECT 'taux_endettement', COUNT(*) 
FROM iceberg_scan('data/iceberg_warehouse/my_data/taux_endettement')
UNION ALL
SELECT 'taux_interet', COUNT(*) 
FROM iceberg_scan('data/iceberg_warehouse/my_data/taux_interet')
UNION ALL
SELECT 'indice_reference_loyers', COUNT(*) 
FROM iceberg_scan('data/iceberg_warehouse/my_data/indice_reference_loyers');
```

## 3. 🔍 Query Table Data

```sql
-- View first 10 rows
SELECT * FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample') LIMIT 10;

-- View specific columns
SELECT id, data FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample') LIMIT 5;

-- Filter data
SELECT * FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample') 
WHERE id > 100 LIMIT 10;
```

## 4. 🗂️ Inspect Table Metadata

```sql
-- View table metadata (snapshots, manifests, data files)
SELECT * FROM iceberg_metadata('data/iceberg_warehouse/my_data/transactions_sample');

-- View table snapshots
SELECT * FROM iceberg_snapshots('data/iceberg_warehouse/my_data/transactions_sample');

-- View table manifests
SELECT * FROM iceberg_manifests('data/iceberg_warehouse/my_data/transactions_sample');
```

## 5. 📈 Table Statistics

```sql
-- Get table statistics
SELECT 
    column_name,
    data_type,
    null_frac,
    avg_width,
    n_distinct
FROM iceberg_scan_stats('data/iceberg_warehouse/my_data/transactions_sample');
```

## 6. 🔄 Compare with Original Parquet Files

```sql
-- Compare Iceberg table with original Parquet
SELECT 'iceberg' as source, COUNT(*) as row_count 
FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample')
UNION ALL
SELECT 'parquet' as source, COUNT(*) as row_count 
FROM read_parquet('data/parquet/transactions_sample.parquet');
```

## 7. 🛠️ Interactive DuckDB Session

```bash
# Start interactive DuckDB session
duckdb

# Then run:
```

```sql
LOAD iceberg;
SET unsafe_enable_version_guessing = true;

-- Now you can run any of the above commands interactively
DESCRIBE SELECT * FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample');
```

## 8. 📝 One-liner Commands

```bash
# Quick schema check
duckdb -c "LOAD iceberg; SET unsafe_enable_version_guessing = true; DESCRIBE SELECT * FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample');"

# Quick row count
duckdb -c "LOAD iceberg; SET unsafe_enable_version_guessing = true; SELECT COUNT(*) FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample');"

# Quick data preview
duckdb -c "LOAD iceberg; SET unsafe_enable_version_guessing = true; SELECT * FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample') LIMIT 5;"
```

## 9. 📋 List All Your Tables

```bash
# List all table directories
ls -la data/iceberg_warehouse/my_data/

# Or programmatically in DuckDB:
```

```sql
-- Create a view of all available tables
CREATE TEMP VIEW available_tables AS
SELECT 'transactions_sample' as table_name
UNION ALL SELECT 'flux_nouveaux_emprunts'
UNION ALL SELECT 'foyers_fiscaux'
UNION ALL SELECT 'loyers'
UNION ALL SELECT 'parc_immobilier'
UNION ALL SELECT 'taux_endettement'
UNION ALL SELECT 'taux_interet'
UNION ALL SELECT 'indice_reference_loyers';

SELECT * FROM available_tables;
```

## 10. 🚨 Troubleshooting

### Common Issues:

**Error: "No version was provided"**
```sql
-- Solution: Enable version guessing
SET unsafe_enable_version_guessing = true;
```

**Error: "Extension not loaded"**
```sql
-- Solution: Load the extension
LOAD iceberg;
```

**Error: "Table not found"**
```bash
# Check if table directory exists
ls -la data/iceberg_warehouse/my_data/table_name/
```

## 📚 Additional Resources

- **DuckDB Iceberg Documentation**: https://duckdb.org/docs/extensions/iceberg
- **Your Tables Location**: `./data/iceberg_warehouse/my_data/`
- **REST Catalog**: http://localhost:8181/v1/namespaces/my_data/tables

---

**Note**: Since your tables currently only have schema definitions (no data), most queries will return 0 rows. To populate them with data, you'll need to extend the Go script to insert data from the Parquet files. 