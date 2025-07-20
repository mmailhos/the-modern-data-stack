# The Modern Data Stack
# 
# A comprehensive Go application for processing data through this pipeline:
# 1. CSV → Parquet (data/source/*.csv → data/parquet/*.parquet)
# 2. Parquet → Iceberg Tables (using REST Catalog)
# 3. Query & Analysis (using DuckDB or other Iceberg engines)

# Default recipe - show available commands
default:
    @just --list

# ============================================================================
# 📦 BUILD COMMANDS
# ============================================================================

# Build all applications
build:
    @echo "🔨 Building all applications..."
    go build -o csv-to-parquet cmd/csv_to_parquet/main.go
    go build -o create-iceberg-tables cmd/create_iceberg_tables/main.go
    @echo "✅ All applications built successfully!"

# Clean build artifacts and generated data
clean:
    @echo "🧹 Cleaning build artifacts and generated data..."
    rm -f csv-to-parquet create-iceberg-tables
    rm -rf data/parquet data/iceberg_warehouse
    go clean
    @echo "✅ Clean complete!"

# ============================================================================
# 🚀 WORKFLOW COMMANDS (Main Pipeline)
# ============================================================================

# Step 1: Convert CSV files to Parquet format
csv-to-parquet:
    @echo "📦 Step 1: Converting CSV files to Parquet format..."
    go run cmd/csv_to_parquet/main.go
    @echo "✅ CSV to Parquet conversion complete!"

# Step 2: Create Iceberg tables using native DuckDB Go client
create-iceberg-tables:
    @echo "🧊 Step 2: Creating Iceberg tables with DuckDB Go client..."
    @chmod +x scripts/wait_for_catalog.sh
    @scripts/wait_for_catalog.sh
    go run cmd/create_iceberg_tables/main.go
    @echo "✅ Iceberg tables creation complete!"

# Complete workflow: CSV → Parquet → Iceberg
full-workflow:
    @echo "🚀 Running complete workflow: CSV → Parquet → Iceberg"
    just check-prereqs
    just csv-to-parquet
    just start-iceberg-catalog
    just create-iceberg-tables
    @echo "🎉 Complete workflow finished!"

# ============================================================================
# 🐳 ICEBERG CATALOG MANAGEMENT
# ============================================================================

# Start Iceberg REST Catalog with Docker
start-iceberg-catalog:
    @echo "🐳 Starting Iceberg REST Catalog..."
    @mkdir -p data/iceberg_warehouse
    @if docker ps --format "table {{{{.Names}}}}" | grep -q "iceberg-rest"; then \
        echo "✅ Iceberg REST Catalog is already running at http://localhost:8181"; \
    else \
        docker run -d --rm \
            -p 8181:8181 \
            -v $(PWD)/data/iceberg_warehouse:/var/lib/iceberg/warehouse \
            -e CATALOG_WAREHOUSE=/var/lib/iceberg/warehouse \
            -e CATALOG_IO__IMPL=org.apache.iceberg.hadoop.HadoopFileIO \
            --name iceberg-rest \
            tabulario/iceberg-rest && \
        echo "✅ Iceberg REST Catalog started at http://localhost:8181"; \
    fi
    @echo "📁 Warehouse location: ./data/iceberg_warehouse"

# Stop Iceberg REST Catalog
stop-iceberg-catalog:
    @echo "🛑 Stopping Iceberg REST Catalog..."
    @if docker ps --format "table {{{{.Names}}}}" | grep -q "iceberg-rest"; then \
        docker stop iceberg-rest && echo "✅ Iceberg REST Catalog stopped"; \
    else \
        echo "ℹ️  Iceberg REST Catalog is not running"; \
    fi

# Check Iceberg REST Catalog status
status-iceberg-catalog:
    @echo "📊 Iceberg REST Catalog Status:"
    @if docker ps --format "table {{{{.Names}}}}" | grep -q "iceberg-rest"; then \
        echo "✅ Running at http://localhost:8181"; \
        docker ps --filter name=iceberg-rest; \
    else \
        echo "❌ Not running"; \
        echo "💡 Run 'just start-iceberg-catalog' to start it"; \
    fi

# ============================================================================
# 🔍 INSPECTION & ANALYSIS COMMANDS  
# ============================================================================

# List all available data files
list-data:
    @echo "📊 Data Files Overview:"
    @echo ""
    @echo "📁 Source CSV files:"
    @if [ -d "data/source" ]; then \
        find data/source -name "*.csv" -exec basename {} \; 2>/dev/null | head -10 || echo "   (no CSV files found)"; \
    else \
        echo "   (data/source directory not found)"; \
    fi
    @echo ""
    @echo "📁 Parquet files:"
    @if [ -d "data/parquet" ]; then \
        find data/parquet -name "*.parquet" -exec basename {} \; 2>/dev/null | head -10 || echo "   (no Parquet files found)"; \
    else \
        echo "   (data/parquet directory not found - run 'just csv-to-parquet')"; \
    fi
    @echo ""
    @echo "📁 Iceberg tables:"
    @if [ -d "data/iceberg_warehouse/my_data" ]; then \
        ls data/iceberg_warehouse/my_data/ 2>/dev/null || echo "   (no Iceberg tables found)"; \
    else \
        echo "   (no Iceberg warehouse found - run 'just create-iceberg-tables')"; \
    fi

# Query Iceberg tables using DuckDB
query-iceberg table_name:
    @echo "🦆 Querying Iceberg table: {{table_name}}"
    duckdb -c "LOAD iceberg; SET unsafe_enable_version_guessing = true; SELECT * FROM iceberg_scan('data/iceberg_warehouse/my_data/{{table_name}}') LIMIT 10;"

# Show schema of an Iceberg table
describe-iceberg table_name:
    @echo "📋 Schema of Iceberg table: {{table_name}}"
    duckdb -c "LOAD iceberg; SET unsafe_enable_version_guessing = true; DESCRIBE SELECT * FROM iceberg_scan('data/iceberg_warehouse/my_data/{{table_name}}');"

# ============================================================================
# 🛠️ DEVELOPMENT COMMANDS
# ============================================================================

# Download and tidy dependencies
deps:
    @echo "📦 Managing dependencies..."
    go mod download
    go mod tidy
    @echo "✅ Dependencies ready!"

# Format code
fmt:
    @echo "🎨 Formatting code..."
    go fmt ./...
    @echo "✅ Code formatted!"

# Run tests
test:
    @echo "🧪 Running tests..."
    go test ./...
    @echo "✅ Tests complete!"

# Check prerequisites (Docker, DuckDB, etc.)
check-prereqs:
    @echo "🔍 Checking prerequisites..."
    @chmod +x scripts/check_docker.sh
    @scripts/check_docker.sh check
    @echo ""
    @scripts/check_docker.sh catalog

# Setup data directories
setup-data:
    @echo "📁 Setting up data directories..."
    mkdir -p data/source data/parquet data/iceberg_warehouse
    @echo "✅ Data directories created!"
    @echo "📝 Place your CSV files in 'data/source/' directory"

# ============================================================================
# ℹ️ HELP & INFO COMMANDS
# ============================================================================

# Show comprehensive help
help:
    @echo "🔧 Altertable - Data Processing Pipeline"
    @echo ""
    @echo "📋 WORKFLOW OVERVIEW:"
    @echo "  1. Place CSV files in data/source/"
    @echo "  2. Run: just csv-to-parquet"
    @echo "  3. Run: just start-iceberg-catalog"
    @echo "  4. Run: just create-iceberg-tables"
    @echo "  5. Query with DuckDB or other Iceberg engines"
    @echo ""
    @echo "🚀 QUICK START:"
    @echo "  just full-workflow     # Complete pipeline"
    @echo "  just list-data         # See what data you have"
    @echo "  just help-examples     # Show query examples"
    @echo ""
    @echo "📦 MAIN COMMANDS:"
    @echo "  csv-to-parquet         # Convert CSV → Parquet"
    @echo "  create-iceberg-tables  # Create Iceberg tables with schema inspection"
    @echo ""
    @echo "🐳 CATALOG MANAGEMENT:"
    @echo "  start-iceberg-catalog  # Start REST catalog"
    @echo "  status-iceberg-catalog # Check catalog status"
    @echo "  stop-iceberg-catalog   # Stop catalog"
    @echo ""
    @echo "🔍 DATA INSPECTION:"
    @echo "  list-data              # Show available data files"
    @echo "  query-iceberg <table>  # Query table with DuckDB"
    @echo "  describe-iceberg <table> # Show table schema"
    @echo ""
    @echo "🛠️ DEVELOPMENT:"
    @echo "  build                  # Build all applications"
    @echo "  clean                  # Clean generated files"
    @echo "  deps                   # Manage dependencies"
    @echo "  fmt                    # Format code"

# Show query examples
help-examples:
    @echo "🔍 Query Examples:"
    @echo ""
    @echo "📊 List all Iceberg tables:"
    @echo "  just list-data"
    @echo ""
    @echo "📋 Show table schema:"
    @echo "  just describe-iceberg transactions_sample"
    @echo ""
    @echo "🔍 Query table data:"
    @echo "  just query-iceberg transactions_sample"
    @echo ""
    @echo "🦆 Direct DuckDB queries:"
    @echo "  duckdb -c \"LOAD iceberg; SET unsafe_enable_version_guessing = true; \\"
    @echo "    SELECT COUNT(*) FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample');\""
    @echo ""
    @echo "📈 Advanced queries:"
    @echo "  duckdb -c \"LOAD iceberg; SET unsafe_enable_version_guessing = true; \\"
    @echo "    SELECT departement, AVG(prix) as avg_price \\"
    @echo "    FROM iceberg_scan('data/iceberg_warehouse/my_data/transactions_sample') \\"
    @echo "    GROUP BY departement ORDER BY avg_price DESC LIMIT 10;\""

# Show project information
info:
    @echo "📊 Altertable Project Information"
    @echo ""
    @echo "🎯 Purpose: Data processing pipeline (CSV → Parquet → Iceberg)"
    @echo "🔧 Language: Go with DuckDB integration"
    @echo "📦 Components:"
    @echo "  • CSV to Parquet converter"
    @echo "  • Parquet to Iceberg table creator (2 approaches)"
    @echo "  • DuckDB integration for querying"
    @echo "  • Docker-based Iceberg REST Catalog"
    @echo ""
    @echo "📁 Directory Structure:"
    @echo "  data/source/           - Input CSV files"
    @echo "  data/parquet/          - Generated Parquet files"
    @echo "  data/iceberg_warehouse/ - Iceberg table storage"
    @echo "  cmd/                   - Go applications"
    @echo ""
    @echo "🔗 Dependencies:"
    @go list -m all | head -5 
