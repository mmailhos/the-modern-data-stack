#!/bin/bash

# Wait for Iceberg REST Catalog to be ready
CATALOG_URL="http://localhost:8181"
MAX_ATTEMPTS=15
SLEEP_INTERVAL=2

echo "🔍 Checking catalog readiness..."

for i in $(seq 1 $MAX_ATTEMPTS); do
    if curl -s "$CATALOG_URL/v1/config" > /dev/null 2>&1; then
        echo "✅ Catalog is ready"
        exit 0
    else
        echo "⏳ Waiting for catalog... ($i/$MAX_ATTEMPTS)"
        sleep $SLEEP_INTERVAL
    fi
done

echo "❌ Catalog not ready after $((MAX_ATTEMPTS * SLEEP_INTERVAL))s"
exit 1 