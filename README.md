# Altertable

A Go application that provides data processing capabilities:

1. **Parquet to DuckDB**: Load parquet files from `data/` directory into DuckDB and display the data
2. **CSV to Iceberg**: Convert CSV files from `data/` directory to Iceberg format (with Parquet fallback)

## Features

### Original Functionality
- Load parquet files from `data/` directory
- Load them into DuckDB as tables
- Use the DuckDB API to list all rows on STDOUT

### New CSV to Iceberg Converter
- Automatically discover all CSV files in the `data/` directory
- Convert each CSV file to Iceberg format using DuckDB
- Fallback to Parquet format if Iceberg is not available
- Display sample data from each processed file
- Create organized output in `iceberg_tables/` directory

## Quick Start

### Using Just (Recommended)

```bash
# Install dependencies
just deps

# Run the original parquet loader
just run

# Convert CSV files to Iceberg format
just csv-to-iceberg

# Build the CSV converter as a standalone binary
just build-csv-converter

# Clean up generated files
just clean

# See all available commands
just help
```

### Manual Usage

```bash
# Original parquet loader
go run main.go

# CSV to Iceberg converter
go run cmd/csv_to_iceberg/main.go

# Build binaries
go build -o altertable .
go build -o csv-to-iceberg cmd/csv_to_iceberg/main.go
```

## Directory Structure

```
altertable/
├── data/                          # Input CSV and Parquet files
│   ├── *.csv                     # CSV files for conversion
│   └── *.parquet                 # Parquet files for loading
├── iceberg_tables/               # Output directory for converted files
│   └── *.parquet                 # Converted Parquet/Iceberg files
├── cmd/
│   └── csv_to_iceberg/
│       └── main.go               # CSV to Iceberg converter
├── main.go                       # Original parquet loader
├── justfile                      # Task runner configuration
└── README.md                     # This file
```

## Dependencies

- Go 1.24+
- DuckDB Go driver (`github.com/marcboeker/go-duckdb`)
- DuckDB extensions: `iceberg`, `httpfs`

## CSV to Iceberg Converter Details

The CSV to Iceberg converter:

1. **Auto-Discovery**: Recursively finds all `.csv` files in the `data/` directory
2. **Schema Detection**: Uses DuckDB's `read_csv_auto()` for automatic schema detection
3. **Iceberg Conversion**: Attempts to convert to Iceberg format using DuckDB's Iceberg extension
4. **Parquet Fallback**: Falls back to Parquet format if Iceberg conversion fails
5. **Data Preview**: Shows sample data from each processed file
6. **Clean Output**: Organizes converted files in the `iceberg_tables/` directory

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
🧊 Creating Iceberg table at iceberg_tables/transactions_sample...
✅ Created Parquet table: iceberg_tables/transactions_sample.parquet
```

## Development

### Available Just Commands

- `just build` - Build the main application
- `just run` - Run the parquet loader
- `just csv-to-iceberg` - Convert CSV files to Iceberg
- `just build-csv-converter` - Build CSV converter binary
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

# Build both applications
go build -o altertable .
go build -o csv-to-iceberg cmd/csv_to_iceberg/main.go
```

## Notes

- The Iceberg extension may not be available in all DuckDB versions
- The converter automatically falls back to Parquet format for compatibility
- Large CSV files are processed efficiently using DuckDB's streaming capabilities
- Output files are organized in the `iceberg_tables/` directory for easy access