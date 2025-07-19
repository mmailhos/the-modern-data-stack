#!/bin/bash

# Check if Docker is installed and running
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker is not installed"
        echo "💡 Please install Docker from https://www.docker.com/get-started"
        return 1
    fi

    if ! docker info &> /dev/null; then
        echo "❌ Docker is not running"
        echo "💡 Please start Docker Desktop or the Docker daemon"
        return 1
    fi

    echo "✅ Docker is available and running"
    return 0
}

# Check if the iceberg-rest container is running
check_iceberg_catalog() {
    if docker ps --format "table {{.Names}}" | grep -q "iceberg-rest"; then
        echo "✅ Iceberg REST Catalog is running"
        echo "🔗 Available at: http://localhost:8181"
        return 0
    else
        echo "⚠️  Iceberg REST Catalog is not running"
        echo "💡 Run 'just start-iceberg-catalog' to start it"
        return 1
    fi
}

# Main execution
case "${1:-check}" in
    "check")
        check_docker
        ;;
    "catalog")
        check_iceberg_catalog
        ;;
    "all")
        check_docker && check_iceberg_catalog
        ;;
    *)
        echo "Usage: $0 [check|catalog|all]"
        echo "  check   - Check if Docker is available"
        echo "  catalog - Check if Iceberg catalog is running"
        echo "  all     - Check both Docker and catalog"
        exit 1
        ;;
esac 