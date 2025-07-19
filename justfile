# Altertable - A Go application for loading parquet files into DuckDB
# Available commands:
#   just build    - Build the application
#   just run      - Run the application
#   just clean    - Clean build artifacts
#   just test     - Run tests
#   just fmt      - Format code
#   just lint     - Run linter
#   just deps     - Download dependencies
#   just tidy     - Tidy dependencies
#   just help     - Show this help

# Default recipe
default:
    @just --list

# Build the application
build:
    @echo "🔨 Building altertable..."
    go build -o altertable .
    @echo "✅ Build complete!"

# Run the application
run:
    @echo "🚀 Running altertable..."
    ./altertable

# Build and run in one command
dev: build run

# Clean build artifacts
clean:
    @echo "🧹 Cleaning build artifacts..."
    rm -f altertable csv-to-parquet parquet-to-iceberg create-iceberg-tables-duckdb
    rm -rf data/parquet data/iceberg_warehouse
    go clean
    @echo "✅ Clean complete!"

# Run tests
test:
    @echo "🧪 Running tests..."
    go test ./...

# Format code
fmt:
    @echo "🎨 Formatting code..."
    go fmt ./...
    @echo "✅ Code formatted!"

# Run linter (requires golangci-lint)
lint:
    @echo "🔍 Running linter..."
    @if command -v golangci-lint >/dev/null 2>&1; then \
        golangci-lint run; \
    else \
        echo "⚠️  golangci-lint not found. Install with: brew install golangci-lint"; \
    fi

# Download dependencies
deps:
    @echo "📦 Downloading dependencies..."
    go mod download
    @echo "✅ Dependencies downloaded!"

# Tidy dependencies
tidy:
    @echo "🧹 Tidying dependencies..."
    go mod tidy
    @echo "✅ Dependencies tidied!"

# Check for vulnerabilities (requires govulncheck)
vuln:
    @echo "🔒 Checking for vulnerabilities..."
    @if command -v govulncheck >/dev/null 2>&1; then \
        govulncheck ./...; \
    else \
        echo "⚠️  govulncheck not found. Install with: go install golang.org/x/vuln/cmd/govulncheck@latest"; \
    fi

# Create sample data directory structure
setup-data:
    @echo "📁 Setting up data directory..."
    mkdir -p data
    @echo "✅ Data directory created!"
    @echo "📝 Place your .parquet files in the 'data' directory"

# Show project info
info:
    @echo "📊 Project Information:"
    @echo "  Name: altertable"
    @echo "  Language: Go"
    @echo "  Purpose: Load parquet files into DuckDB"
    @echo ""
    @echo "📁 Directory structure:"
    @echo "  data/     - Place your .parquet files here"
    @echo "  main.go   - Main application code"
    @echo "  go.mod    - Go module definition"
    @echo ""
    @echo "🔧 Dependencies:"
    @go list -m all | head -10

# Install development tools
install-tools:
    @echo "🛠️  Installing development tools..."
    go install golang.org/x/tools/cmd/goimports@latest
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
    go install golang.org/x/vuln/cmd/govulncheck@latest
    @echo "✅ Development tools installed!"

# Full development setup
setup: deps setup-data install-tools
    @echo "🎉 Development environment setup complete!"

# Release build (optimized)
release:
    @echo "🚀 Building release version..."
    go build -ldflags="-s -w" -o altertable .
    @echo "✅ Release build complete!"

# Cross-compile for different platforms
cross-compile:
    @echo "🌍 Cross-compiling for multiple platforms..."
    GOOS=linux GOARCH=amd64 go build -o altertable-linux-amd64 .
    GOOS=darwin GOARCH=amd64 go build -o altertable-darwin-amd64 .
    GOOS=darwin GOARCH=arm64 go build -o altertable-darwin-arm64 .
    GOOS=windows GOARCH=amd64 go build -o altertable-windows-amd64.exe .
    @echo "✅ Cross-compilation complete!"

# Convert CSV files to Parquet format
csv-to-parquet:
    @echo "📦 Converting CSV files to Parquet format..."
    go run cmd/csv_to_parquet/main.go
    @echo "✅ CSV to Parquet conversion complete!"

# Create Iceberg tables from Parquet files (native Go)
parquet-to-iceberg:
    @echo "🧊 Converting Parquet files to Iceberg tables..."
    @chmod +x scripts/wait_for_catalog.sh
    @scripts/wait_for_catalog.sh
    go run cmd/parquet_to_iceberg/main.go
    @echo "✅ Parquet to Iceberg conversion complete!"

# Create Iceberg tables using Apache Iceberg Go library (simplified)
create-iceberg-tables-iceberg-go:
    @echo "🧊 Creating Iceberg tables using Apache Iceberg Go library..."
    @chmod +x scripts/wait_for_catalog.sh
    @scripts/wait_for_catalog.sh
    go run cmd/create_iceberg_tables/main.go
    @echo "✅ Iceberg tables creation complete!"

# Legacy DuckDB-based Iceberg creation (deprecated - has compatibility issues)
create-iceberg-tables-duckdb:
    @echo "⚠️  This command uses DuckDB and may have compatibility issues"
    @echo "💡 Consider using 'just parquet-to-iceberg' or 'just create-iceberg-tables-iceberg-go' instead"
    @echo "❌ DuckDB approach disabled due to compatibility issues"

# Alias for backward compatibility
create-iceberg-tables: parquet-to-iceberg

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

# Check prerequisites (Docker, etc.)
check-prereqs:
    @echo "🔍 Checking prerequisites..."
    @chmod +x scripts/check_docker.sh
    @scripts/check_docker.sh check
    @echo ""
    @scripts/check_docker.sh catalog

# Full workflow: CSV -> Parquet -> Iceberg
full-workflow:
    @echo "🚀 Running full workflow: CSV -> Parquet -> Iceberg"
    just check-prereqs
    just csv-to-parquet
    just start-iceberg-catalog
    just parquet-to-iceberg
    @echo "🎉 Full workflow complete!"

# Build all converters
build-converters:
    @echo "🔨 Building all converters..."
    go build -o csv-to-parquet cmd/csv_to_parquet/main.go
    go build -o parquet-to-iceberg cmd/parquet_to_iceberg/main.go
    go build -o create-iceberg-tables-duckdb cmd/create_iceberg_tables/main.go
    @echo "✅ All converters built!"

# Show help
help:
    @echo "🔧 Altertable - Data processing with DuckDB, Parquet & Iceberg"
    @echo ""
    @echo "Usage: just <command>"
    @echo ""
    @echo "🔨 Build Commands:"
    @echo "  build              Build the main application"
    @echo "  build-converters   Build all converter tools"
    @echo "  release            Build optimized release version"
    @echo "  cross-compile      Build for multiple platforms"
    @echo ""
    @echo "🚀 Run Commands:"
    @echo "  run                Run the parquet loader"
    @echo "  dev                Build and run in one command"
    @echo "  csv-to-parquet     Convert CSV files to Parquet format"
    @echo "  parquet-to-iceberg Convert Parquet files to Iceberg tables (REST API approach)"
    @echo "  create-iceberg-tables-iceberg-go Create Iceberg tables using Apache Iceberg Go library"
    @echo "  create-iceberg-tables Alias for parquet-to-iceberg (backward compatibility)"
    @echo "  full-workflow      Complete CSV -> Parquet -> Iceberg workflow"
    @echo ""
    @echo "🐳 Docker Commands:"
    @echo "  start-iceberg-catalog Start Iceberg REST Catalog with Docker"
    @echo "  stop-iceberg-catalog  Stop Iceberg REST Catalog"
    @echo "  status-iceberg-catalog Check Iceberg REST Catalog status"
    @echo ""
    @echo "🛠️  Development Commands:"
    @echo "  clean              Clean build artifacts and generated files"
    @echo "  test               Run tests"
    @echo "  fmt                Format code"
    @echo "  lint               Run linter"
    @echo "  deps               Download dependencies"
    @echo "  tidy               Tidy dependencies"
    @echo "  vuln               Check for vulnerabilities"
    @echo "  setup-data         Create data directory"
    @echo "  info               Show project information"
    @echo "  install-tools      Install development tools"
    @echo "  setup              Full development setup"
    @echo "  help               Show this help"
    @echo ""
    @echo "📁 Workflow:"
    @echo "  1. Place CSV files in the 'data' directory"
    @echo "  2. Run 'just csv-to-parquet' → creates files in 'data/parquet/'"
    @echo "  3. Run 'just start-iceberg-catalog' to start the catalog"
    @echo "  4. Run 'just create-iceberg-tables' to create Iceberg tables"
    @echo "  Or use 'just full-workflow' to do all steps automatically" 