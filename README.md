# The Modern Data Sack 

A comprehensive Go application for processing data through a modern analytics pipeline:

**CSV → Parquet → Iceberg Tables → Analytics**

## 🎯 Overview

This repo provides a complete data processing pipeline that transforms your CSV data into queryable Iceberg tables:

1. **📦 CSV to Parquet**: Convert CSV files to efficient Parquet format using DuckDB
2. **🧊 Parquet to Iceberg**: Create Apache Iceberg tables with proper schemas
3. **🔍 Query & Analysis**: Query tables using DuckDB, Spark, Trino, or any Iceberg-compatible engine

## 🚀 Quick Start

### Prerequisites

- **Go 1.19+**: For building the applications
- **Docker**: For running the Iceberg REST Catalog
- **DuckDB**: For querying (optional, but recommended)

### Complete Workflow

```bash
# 1. Setup and dependencies
just deps
just setup-data

# 2. Place your CSV files in data/source/

# 3. Run the complete pipeline
just full-workflow

# 4. Query your data
just list-data
just query-iceberg your_table_name
```

## 📋 Detailed Steps

### Step 1: CSV to Parquet Conversion

Convert your CSV files to efficient Parquet format:

```bash
# Place CSV files in data/source/
# Then convert them:
just csv-to-parquet
```

**What it does:**
- Discovers all CSV files in `data/source/`
- Uses DuckDB to convert each CSV to Parquet
- Stores results in `data/parquet/`
- Shows sample data and statistics

### Step 2: Iceberg Table Creation

Create Apache Iceberg tables from your Parquet files:

```bash
just start-iceberg-catalog
just create-iceberg-tables
```

### Step 3: Query and Analysis

Query your Iceberg tables using various methods:

```bash
# List available tables
just list-data

# Query a specific table
just query-iceberg transactions_sample

# Show table schema
just describe-iceberg transactions_sample

# Direct DuckDB queries
duckdb -c "LOAD iceberg; SET unsafe_enable_version_guessing = true; 
  SELECT * FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample') LIMIT 10;"
```

## 🏗️ Architecture

### Applications

- **`cmd/csv_to_parquet/`**: CSV to Parquet converter using DuckDB
- **`cmd/create_iceberg_tables/`**: Iceberg table creator with native DuckDB Go client and schema inspection

### Data Flow

```
data/source/*.csv
       ↓ (csv-to-parquet)
data/parquet/*.parquet
       ↓ (create-iceberg-tables)
data/iceberg_warehouse/my_data/*
       ↓ (query engines)
    Analytics & Insights
```

### Directory Structure

```
the-modern-data-stack/
├── cmd/
│   ├── csv_to_parquet/         # CSV → Parquet converter
│   └── create_iceberg_tables/  # Iceberg creator with schema inspection
├── data/
│   ├── source/                 # Input CSV files (you provide)
│   ├── parquet/                # Generated Parquet files
│   └── iceberg_warehouse/      # Iceberg table storage
├── scripts/                    # Helper scripts
└── justfile                    # Task automation
```

## 🔧 Available Commands

### Main Workflow
- `just csv-to-parquet` - Convert CSV files to Parquet
- `just create-iceberg-tables` - Create Iceberg tables with schema inspection
- `just full-workflow` - Complete pipeline automation

### Catalog Management
- `just start-iceberg-catalog` - Start Iceberg REST Catalog
- `just status-iceberg-catalog` - Check catalog status
- `just stop-iceberg-catalog` - Stop catalog

### Data Inspection
- `just list-data` - Show available data files
- `just query-iceberg <table>` - Query table with DuckDB
- `just describe-iceberg <table>` - Show table schema

### Development
- `just build` - Build all applications
- `just clean` - Clean generated files
- `just deps` - Manage dependencies
- `just fmt` - Format code

### Help
- `just help` - Show comprehensive help
- `just help-examples` - Show query examples
- `just info` - Show project information

## 📊 Example Usage

### Processing Real Estate Data

```bash
# 1. Place your CSV files
cp transactions.csv data/source/
cp properties.csv data/source/

# 2. Convert to Parquet
just csv-to-parquet
# Output: data/parquet/transactions.parquet, data/parquet/properties.parquet

# 3. Create Iceberg tables
just start-iceberg-catalog
just create-iceberg-tables
# Output: Iceberg tables in data/iceberg_warehouse/my_data/

# 4. Query the data
just query-iceberg transactions
just describe-iceberg properties

# 5. Advanced analytics
duckdb -c "LOAD iceberg; SET unsafe_enable_version_guessing = true;
  SELECT departement, AVG(prix) as avg_price 
  FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions') 
  GROUP BY departement ORDER BY avg_price DESC LIMIT 10;"
```

## 🔍 Features

### CSV to Parquet Converter
- ✅ Automatic CSV discovery
- ✅ DuckDB-powered conversion
- ✅ Sample data preview
- ✅ Error handling for malformed files
- ✅ Progress reporting

### Iceberg Creator
- ✅ Native DuckDB Go client integration
- ✅ Real Parquet schema reading and inspection
- ✅ Actual sample data preview
- ✅ Row count statistics
- ✅ Nullability preservation
- ✅ Type-safe operations
- ✅ Comprehensive error handling

### Query Integration
- ✅ DuckDB Iceberg extension support
- ✅ Schema inspection commands
- ✅ Sample query helpers
- ✅ Compatible with Spark, Trino, etc.

## 🐳 Docker Integration

The Iceberg REST Catalog runs in Docker with proper configuration:

```bash
# Automatically configured with:
# - Persistent warehouse storage
# - Proper environment variables
# - Health checking
```

## 🐳 Docker Compose Setup

The project includes a comprehensive Docker Compose configuration for easy service management.

### **Quick Start with Docker Compose**

```bash
# Start all services (Trino + Iceberg REST Catalog)
just start-services

# Run the complete workflow
just full-workflow

# Query your data via Trino
just query-trino "SHOW TABLES FROM my_data"
just trino-cli  # Interactive session

# Stop all services
just stop-services
```

### **Services Included**

- **Trino Query Engine** (`localhost:8080`)
  - Web UI for interactive queries
  - SQL interface with Iceberg and Hive catalogs
  - Health checks and automatic restart
  
- **Iceberg REST Catalog** (`localhost:8181`)
  - Metadata management for Iceberg tables
  - REST API for table operations
  - Persistent warehouse storage

### **Available Commands**

```bash
# Service Management
just start-services      # Start all services
just stop-services       # Stop all services  
just status-services     # Check service status
just restart-services    # Restart all services
just logs [service]      # View logs (optional service name)

# Data Querying
just query-trino "SQL"   # Run SQL via Trino
just trino-cli           # Interactive Trino session
just query-parquet file "SQL"  # Query Parquet directly

# Legacy aliases (still work)
just start-iceberg-catalog  # → start-services
just stop-iceberg-catalog   # → stop-services
```

### **Docker Compose Features**

- **Automatic networking**: Services can communicate via container names
- **Health checks**: Ensures services are ready before dependent services start
- **Persistent volumes**: Data survives container restarts
- **Restart policies**: Automatic recovery from failures
- **Port mapping**: Access services from your host machine

### **Service URLs**

- **Trino Web UI**: http://localhost:8080
- **Iceberg REST API**: http://localhost:8181
- **Service Status**: `just status-services`

## 🔗 Dependencies

- **Go**: Modern Go with database/sql interface
- **DuckDB**: Native Go client (`github.com/marcboeker/go-duckdb`)
- **Apache Iceberg**: REST catalog and table format
- **Docker**: For Iceberg REST Catalog

## 📈 Performance

- **Native Go**: No subprocess overhead
- **DuckDB Integration**: Fast Parquet processing
- **Iceberg Format**: Efficient columnar storage
- **Schema Evolution**: Handle changing data structures

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `just fmt` and `just test`
5. Submit a pull request