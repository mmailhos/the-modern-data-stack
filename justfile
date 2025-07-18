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
    rm -f altertable csv-to-iceberg
    rm -rf iceberg_tables
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

# Convert CSV files to Iceberg format
csv-to-iceberg:
    @echo "🧊 Converting CSV files to Iceberg format..."
    go run cmd/csv_to_iceberg/main.go
    @echo "✅ CSV to Iceberg conversion complete!"

# Build the CSV to Iceberg converter
build-csv-converter:
    @echo "🔨 Building CSV to Iceberg converter..."
    go build -o csv-to-iceberg cmd/csv_to_iceberg/main.go
    @echo "✅ CSV converter build complete!"

# Show help
help:
    @echo "🔧 Altertable - Parquet to DuckDB loader & CSV to Iceberg converter"
    @echo ""
    @echo "Usage: just <command>"
    @echo ""
    @echo "Commands:"
    @echo "  build              Build the application"
    @echo "  run                Run the application"
    @echo "  dev                Build and run in one command"
    @echo "  clean              Clean build artifacts"
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
    @echo "  release            Build optimized release version"
    @echo "  cross-compile      Build for multiple platforms"
    @echo "  csv-to-iceberg     Convert CSV files to Iceberg format"
    @echo "  build-csv-converter Build CSV to Iceberg converter"
    @echo "  help               Show this help"
    @echo ""
    @echo "📁 Make sure to place your .parquet files in the 'data' directory"
    @echo "🧊 Use 'just csv-to-iceberg' to convert CSV files to Iceberg format" 