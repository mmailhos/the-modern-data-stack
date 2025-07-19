package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// IcebergType represents an Iceberg data type
type IcebergType struct {
	Type string `json:"type"`
}

// IcebergField represents a field in an Iceberg schema
type IcebergField struct {
	ID       int         `json:"id"`
	Name     string      `json:"name"`
	Required bool        `json:"required"`
	Type     IcebergType `json:"type"`
}

// IcebergSchema represents an Iceberg table schema
type IcebergSchema struct {
	Type     string         `json:"type"`
	SchemaID int            `json:"schema-id"`
	Fields   []IcebergField `json:"fields"`
}

// CreateTableRequest represents the request to create an Iceberg table
type CreateTableRequest struct {
	Name     string        `json:"name"`
	Schema   IcebergSchema `json:"schema"`
	Location string        `json:"location,omitempty"`
}

// findParquetFiles recursively finds all .parquet files in the given directory
func findParquetFiles(rootDir string) ([]string, error) {
	var parquetFiles []string

	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Check if it's a parquet file
		if !info.IsDir() && filepath.Ext(path) == ".parquet" {
			parquetFiles = append(parquetFiles, path)
		}

		return nil
	})

	return parquetFiles, err
}

// sanitizeTableName creates a valid table name from file path
func sanitizeTableName(filePath string) string {
	// Get filename without extension
	filename := filepath.Base(filePath)
	tableName := strings.TrimSuffix(filename, filepath.Ext(filename))

	// Replace special characters with underscores
	tableName = strings.ReplaceAll(tableName, "-", "_")
	tableName = strings.ReplaceAll(tableName, " ", "_")
	tableName = strings.ReplaceAll(tableName, ".", "_")

	return tableName
}

// checkCatalogHTTP checks if the Iceberg REST Catalog is responding via HTTP
func checkCatalogHTTP(catalogURL string) error {
	resp, err := http.Get(catalogURL + "/v1/config")
	if err != nil {
		return fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP status: %d", resp.StatusCode)
	}

	return nil
}

// waitForCatalog waits for the Iceberg REST Catalog to be available
func waitForCatalog(catalogURL string, maxRetries int) error {
	fmt.Println("🔍 Checking HTTP connectivity to catalog...")
	for i := 0; i < maxRetries; i++ {
		if err := checkCatalogHTTP(catalogURL); err == nil {
			fmt.Println("✅ Catalog HTTP endpoint is responding")
			return nil
		} else if i < maxRetries-1 {
			fmt.Printf("⏳ HTTP check failed (attempt %d/%d): %v\n", i+1, maxRetries, err)
			time.Sleep(2 * time.Second)
		}
	}
	return fmt.Errorf("catalog HTTP endpoint not responding after %d attempts", maxRetries)
}

// createBasicSchema creates a basic Iceberg schema for a table
// In a real implementation, you would read the actual Parquet schema
func createBasicSchema(tableName string) IcebergSchema {
	// Create a basic schema with common fields
	// This is a simplified approach - in production you'd read the actual Parquet schema
	fields := []IcebergField{
		{
			ID:       1,
			Name:     "id",
			Type:     IcebergType{Type: "long"},
			Required: false,
		},
		{
			ID:       2,
			Name:     "data",
			Type:     IcebergType{Type: "string"},
			Required: false,
		},
		{
			ID:       3,
			Name:     "timestamp",
			Type:     IcebergType{Type: "timestamp"},
			Required: false,
		},
	}

	return IcebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields:   fields,
	}
}

// createNamespace creates a namespace via REST API
func createNamespace(catalogURL, namespace string) error {
	url := fmt.Sprintf("%s/v1/namespaces", catalogURL)

	payload := map[string]interface{}{
		"namespace":  []string{namespace},
		"properties": map[string]string{},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal namespace request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create namespace: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 409 {
		// Namespace already exists, which is fine
		return nil
	}

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create namespace, status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// createTable creates an Iceberg table via REST API
func createTable(catalogURL, namespace, tableName string, schema IcebergSchema) error {
	url := fmt.Sprintf("%s/v1/namespaces/%s/tables", catalogURL, namespace)

	request := CreateTableRequest{
		Name:   tableName,
		Schema: schema,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal table request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create table, status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

func main() {
	fmt.Println("🧊 Parquet to Iceberg Table Creator (HTTP API)")

	// Check if data/parquet directory exists
	parquetDir := "data/parquet"
	if _, err := os.Stat(parquetDir); os.IsNotExist(err) {
		fmt.Printf("⚠️  Parquet directory '%s' does not exist.\n", parquetDir)
		fmt.Println("💡 Please run 'just csv-to-parquet' first to create Parquet files")
		return
	}

	// Find all Parquet files
	parquetFiles, err := findParquetFiles(parquetDir)
	if err != nil {
		log.Fatal("Failed to search for Parquet files:", err)
	}

	if len(parquetFiles) == 0 {
		fmt.Printf("⚠️  No Parquet files found in '%s' directory\n", parquetDir)
		fmt.Println("💡 Please run 'just csv-to-parquet' first to create Parquet files")
		return
	}

	fmt.Printf("📊 Found %d Parquet file(s):\n", len(parquetFiles))
	for _, file := range parquetFiles {
		relPath, _ := filepath.Rel(parquetDir, file)
		fmt.Printf("   - %s\n", relPath)
	}

	// Wait for and connect to Iceberg REST Catalog
	catalogURL := "http://localhost:8181"
	fmt.Println("\n🔗 Connecting to Iceberg REST Catalog...")
	fmt.Println("💡 Make sure the Iceberg REST Catalog is running:")
	fmt.Println("   just start-iceberg-catalog")

	err = waitForCatalog(catalogURL, 5)
	if err != nil {
		log.Fatal("Failed to connect to Iceberg REST Catalog:", err)
	}

	// Create namespace (schema)
	namespaceName := "my_data"
	fmt.Printf("📁 Creating namespace '%s'...\n", namespaceName)
	err = createNamespace(catalogURL, namespaceName)
	if err != nil {
		fmt.Printf("ℹ️  Namespace creation result: %v\n", err)
	} else {
		fmt.Printf("✅ Namespace '%s' ready\n", namespaceName)
	}

	// Process each Parquet file
	fmt.Println("\n🧊 Creating Iceberg tables...")
	successCount := 0

	for _, parquetFile := range parquetFiles {
		relPath, _ := filepath.Rel(parquetDir, parquetFile)
		tableName := sanitizeTableName(parquetFile)

		fmt.Printf("\n🔄 Processing %s -> table '%s.%s'...\n", relPath, namespaceName, tableName)

		// Create a basic schema (in production, you'd read the actual Parquet schema)
		icebergSchema := createBasicSchema(tableName)

		fmt.Printf("📋 Schema: %d fields (basic template)\n", len(icebergSchema.Fields))
		for _, field := range icebergSchema.Fields {
			fmt.Printf("   - %s: %s\n", field.Name, field.Type.Type)
		}

		// Create Iceberg table
		fmt.Printf("🔨 Creating Iceberg table '%s.%s'...\n", namespaceName, tableName)

		err = createTable(catalogURL, namespaceName, tableName, icebergSchema)
		if err != nil {
			log.Printf("Failed to create table %s.%s: %v", namespaceName, tableName, err)
			continue
		}

		fmt.Printf("✅ Created Iceberg table '%s.%s'\n", namespaceName, tableName)
		successCount++
	}

	fmt.Printf("\n🎉 Successfully created %d Iceberg tables!\n", successCount)

	// Show summary
	fmt.Println("\n📊 Summary:")
	fmt.Printf("   - Namespace: %s\n", namespaceName)
	fmt.Printf("   - Parquet files processed: %d\n", len(parquetFiles))
	fmt.Printf("   - Iceberg tables created: %d\n", successCount)
	fmt.Printf("   - Catalog URI: %s\n", catalogURL)
	fmt.Printf("   - Warehouse location: ./data/iceberg_warehouse\n")

	fmt.Println("\n💡 Important notes:")
	fmt.Println("   - Tables were created with a basic schema template")
	fmt.Println("   - In production, you'd read the actual Parquet schema")
	fmt.Println("   - Data would need to be copied separately using Iceberg writers")
	fmt.Println("   - You can query table metadata with any Iceberg-compatible engine")

	fmt.Println("\n🔧 Next steps to improve:")
	fmt.Println("   - Add proper Parquet schema reading")
	fmt.Println("   - Implement data copying from Parquet to Iceberg")
	fmt.Println("   - Add support for partitioning and table properties")
}
