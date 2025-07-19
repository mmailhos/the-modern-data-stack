# Altertable

A comprehensive Go application for data processing with DuckDB, Parquet, and Apache Iceberg:

1. **Parquet to DuckDB**: Load parquet files from `data/` directory into DuckDB and display the data
2. **CSV to Parquet**: Convert CSV files from `data/` directory to efficient Parquet format
3. **Iceberg Tables**: Create Apache Iceberg tables from Parquet files using an Iceberg REST Catalog

## Features

### Original Functionality
- Load parquet files from `data/` directory
- Load them into DuckDB as tables
- Use the DuckDB API to list all rows on STDOUT

### CSV to Parquet Converter
- Automatically discover all CSV files in the `data/` directory
- Convert each CSV file to Parquet format using DuckDB
- Display sample data from each processed file
- Create organized output in `data/parquet/` directory

### Iceberg Tables Creator
- Create Apache Iceberg tables from Parquet files
- Uses Iceberg REST Catalog for metadata management
- Supports schema evolution and ACID transactions
- Compatible with any Iceberg-enabled query engine
- Docker-based catalog setup for easy development

## Quick Start

### Using Just (Recommended)

```bash
# Install dependencies
just deps

# Run the original parquet loader
just run

# Convert CSV files to Parquet format
just csv-to-parquet

# Create Iceberg tables from Parquet files
just start-iceberg-catalog  # Start the catalog service
just create-iceberg-tables  # Create the tables

# Or run the complete workflow
just full-workflow

# Clean up generated files
just clean

# See all available commands
just help
```

### Manual Usage

```bash
# Original parquet loader
go run main.go

# CSV to Parquet converter
go run cmd/csv_to_parquet/main.go

# Create Iceberg tables
go run cmd/create_iceberg_tables/main.go

# Build binaries
go build -o altertable .
go build -o csv-to-parquet cmd/csv_to_parquet/main.go
go build -o create-iceberg-tables cmd/create_iceberg_tables/main.go
```

## Directory Structure

```
altertable/
├── data/                          # Input CSV and Parquet files
│   ├── *.csv                     # CSV files for conversion
│   └── *.parquet                 # Parquet files for loading
├── data/                          # Data directory
│   ├── *.csv                     # Input CSV files
│   ├── parquet/                  # Converted Parquet files
│   │   └── *.parquet
│   └── iceberg_warehouse/        # Iceberg table metadata and data
├── cmd/
│   ├── csv_to_parquet/
│   │   └── main.go               # CSV to Parquet converter
│   └── create_iceberg_tables/
│       └── main.go               # Iceberg tables creator
├── scripts/
│   └── check_docker.sh           # Docker availability checker
├── main.go                       # Original parquet loader
├── justfile                      # Task runner configuration
└── README.md                     # This file
```

## Dependencies

- Go 1.24+
- DuckDB Go driver (`github.com/marcboeker/go-duckdb`)
- Docker (for Iceberg REST Catalog)
- DuckDB extensions: `iceberg` (automatically installed when needed)

## CSV to Parquet Converter Details

The CSV to Parquet converter:

1. **Auto-Discovery**: Recursively finds all `.csv` files in the `data/` directory
2. **Schema Detection**: Uses DuckDB's `read_csv_auto()` for automatic schema detection
3. **Parquet Conversion**: Converts to efficient Parquet format using DuckDB
4. **Data Preview**: Shows sample data from each processed file
5. **Clean Output**: Organizes converted files in the `parquet_tables/` directory

### Supported Data Types

The converter automatically handles:
- Numeric types (integers, floats)
- Text/string data
- Dates and timestamps
- Boolean values
- NULL values

### Example Output

```
📊 Found 8 CSV file(s):
   - transactions_sample.csv
   - foyers_fiscaux.csv
   - loyers.csv
   ...

🔄 Processing transactions_sample.csv -> table 'transactions_sample'...
📈 Loaded 100 rows from transactions_sample.csv
📦 Creating Parquet table at data/parquet/transactions_sample.parquet...
✅ Created Parquet table: data/parquet/transactions_sample.parquet
```

## Iceberg Tables Creator Details

The Iceberg tables creator:

1. **Prerequisites Check**: Verifies Docker is available and running
2. **Catalog Connection**: Connects to Iceberg REST Catalog at `http://localhost:8181`
3. **Schema Creation**: Creates a schema named `my_data` for organizing tables
4. **Table Creation**: Converts each Parquet file to an Iceberg table
5. **Metadata Management**: Uses the REST Catalog for ACID transactions and schema evolution

### Iceberg Benefits

- **ACID Transactions**: Full ACID compliance for data integrity
- **Schema Evolution**: Add, remove, or modify columns without breaking existing queries
- **Time Travel**: Query historical versions of your data
- **Partitioning**: Efficient data organization and pruning
- **Metadata Management**: Centralized catalog for table discovery and management

### Example Workflow

```bash
# 1. Start the Iceberg REST Catalog
just start-iceberg-catalog

# 2. Create Iceberg tables from existing Parquet files
just create-iceberg-tables

# Output:
🔗 Connecting to Iceberg REST Catalog...
✅ Connected to Iceberg REST Catalog
📁 Creating schema 'my_data'...
✅ Schema 'my_data' created successfully

🧊 Creating Iceberg tables...
🔄 Creating table 'my_data.transactions_sample' from transactions_sample.parquet...
✅ Created table 'my_data.transactions_sample' with 100 rows
```

## Development

### Available Just Commands

- `just build` - Build the main application
- `just run` - Run the parquet loader
- `just csv-to-parquet` - Convert CSV files to Parquet
- `just create-iceberg-tables` - Create Iceberg tables from Parquet files
- `just start-iceberg-catalog` - Start Iceberg REST Catalog with Docker (idempotent)
- `just stop-iceberg-catalog` - Stop Iceberg REST Catalog (graceful)
- `just status-iceberg-catalog` - Check Iceberg REST Catalog status
- `just full-workflow` - Complete CSV → Parquet → Iceberg workflow
- `just build-converters` - Build all converter binaries
- `just check-prereqs` - Check Docker and other prerequisites
- `just clean` - Clean build artifacts and output files
- `just fmt` - Format Go code
- `just lint` - Run linter
- `just test` - Run tests
- `just deps` - Download dependencies
- `just help` - Show all available commands

### Building from Source

```bash
# Clone the repository
git clone <repository-url>
cd altertable

# Install dependencies
go mod download

# Build all applications
go build -o altertable .
go build -o csv-to-parquet cmd/csv_to_parquet/main.go
go build -o create-iceberg-tables cmd/create_iceberg_tables/main.go
```

## Notes

- Parquet format is natively supported by DuckDB without additional extensions
- Large CSV files are processed efficiently using DuckDB's streaming capabilities
- Iceberg tables provide ACID transactions, schema evolution, and time travel capabilities
- The Iceberg REST Catalog runs in Docker for easy development and testing
- Output files are organized in dedicated directories for easy access
- Both Parquet and Iceberg formats provide excellent compression and query performance for analytics
- Iceberg tables are compatible with Spark, Trino, Flink, and other query engines