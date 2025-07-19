package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

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

// IcebergField represents a field in an Iceberg schema
type IcebergField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
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

// ParquetColumn represents a column from DuckDB's DESCRIBE output
type ParquetColumn struct {
	Name string
	Type string
	Null string
}

// convertDuckDBTypeToIceberg converts DuckDB data types to Iceberg type strings
func convertDuckDBTypeToIceberg(duckdbType string) string {
	// Normalize the type string
	typeUpper := strings.ToUpper(strings.TrimSpace(duckdbType))

	switch {
	case typeUpper == "BOOLEAN":
		return "boolean"
	case typeUpper == "TINYINT" || typeUpper == "SMALLINT" || typeUpper == "INTEGER":
		return "int"
	case typeUpper == "BIGINT":
		return "long"
	case typeUpper == "REAL" || typeUpper == "FLOAT":
		return "float"
	case typeUpper == "DOUBLE":
		return "double"
	case strings.Contains(typeUpper, "VARCHAR") || typeUpper == "TEXT":
		return "string"
	case typeUpper == "BLOB":
		return "binary"
	case typeUpper == "DATE":
		return "date"
	case strings.Contains(typeUpper, "TIME"):
		return "time"
	case strings.Contains(typeUpper, "TIMESTAMP"):
		return "timestamp"
	case strings.Contains(typeUpper, "DECIMAL"):
		return "decimal(38,18)" // Default precision and scale
	default:
		// For unknown types, default to string
		return "string"
	}
}

// initDuckDB initializes a DuckDB connection and installs required extensions
func initDuckDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %v", err)
	}

	// Install and load required extensions
	extensions := []string{
		"INSTALL parquet",
		"LOAD parquet",
	}

	for _, ext := range extensions {
		if _, err := db.Exec(ext); err != nil {
			// Ignore errors for already installed extensions
			fmt.Printf("Extension command '%s': %v (this is often normal)\n", ext, err)
		}
	}

	return db, nil
}

// readParquetSchemaWithDuckDB reads the schema from a Parquet file using DuckDB Go client
func readParquetSchemaWithDuckDB(db *sql.DB, filePath string) (IcebergSchema, error) {
	// Build the DuckDB query to describe the Parquet file
	query := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", filePath)

	rows, err := db.Query(query)
	if err != nil {
		return IcebergSchema{}, fmt.Errorf("failed to execute DuckDB query: %v", err)
	}
	defer rows.Close()

	var columns []ParquetColumn

	// Read the results
	for rows.Next() {
		var col ParquetColumn
		var key, defaultVal, extra sql.NullString

		err := rows.Scan(&col.Name, &col.Type, &col.Null, &key, &defaultVal, &extra)
		if err != nil {
			return IcebergSchema{}, fmt.Errorf("failed to scan row: %v", err)
		}

		columns = append(columns, col)
	}

	if err = rows.Err(); err != nil {
		return IcebergSchema{}, fmt.Errorf("error reading rows: %v", err)
	}

	if len(columns) == 0 {
		return IcebergSchema{}, fmt.Errorf("no columns found in parquet file schema")
	}

	// Convert to Iceberg schema
	var fields []IcebergField
	for i, col := range columns {
		icebergField := IcebergField{
			ID:       i + 1, // Iceberg field IDs start from 1
			Name:     col.Name,
			Required: col.Null == "NO", // Convert NULL column to Required field
			Type:     convertDuckDBTypeToIceberg(col.Type),
		}
		fields = append(fields, icebergField)
	}

	return IcebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields:   fields,
	}, nil
}

// readParquetSampleDataWithDuckDB reads sample data from a Parquet file using DuckDB Go client
func readParquetSampleDataWithDuckDB(db *sql.DB, filePath string, limit int) ([]map[string]interface{}, error) {
	// Build the DuckDB query to read sample data
	query := fmt.Sprintf("SELECT * FROM read_parquet('%s') LIMIT %d", filePath, limit)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute DuckDB query: %v", err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	var sampleData []map[string]interface{}

	// Read the data rows
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// Scan the row
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// Create a map for this row
		row := make(map[string]interface{})
		for i, col := range columns {
			var value interface{}
			if values[i] != nil {
				// Convert byte slices to strings for better display
				if b, ok := values[i].([]byte); ok {
					value = string(b)
				} else {
					value = values[i]
				}
			}
			row[col] = value
		}

		sampleData = append(sampleData, row)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading rows: %v", err)
	}

	return sampleData, nil
}

// getParquetRowCount gets the total number of rows in a Parquet file
func getParquetRowCount(db *sql.DB, filePath string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", filePath)

	var count int64
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}

	return count, nil
}

// createBasicSchema creates a basic Iceberg schema for a table (fallback)
func createBasicSchema(tableName string) IcebergSchema {
	fields := []IcebergField{
		{
			ID:       1,
			Name:     "id",
			Type:     "long",
			Required: false,
		},
		{
			ID:       2,
			Name:     "data",
			Type:     "string",
			Required: false,
		},
		{
			ID:       3,
			Name:     "timestamp",
			Type:     "timestamp",
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
	fmt.Println("🧊 Iceberg Table Creator (Apache Iceberg Go - Enhanced with DuckDB Go Client)")

	// Initialize DuckDB connection
	fmt.Println("🦆 Initializing DuckDB connection...")
	db, err := initDuckDB()
	if err != nil {
		log.Fatal("Failed to initialize DuckDB:", err)
	}
	defer db.Close()
	fmt.Println("✅ DuckDB connection established")

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
	fmt.Println("   docker run -d --rm -p 8181:8181 \\")
	fmt.Println("     -v $PWD/data/iceberg_warehouse:/var/lib/iceberg/warehouse \\")
	fmt.Println("     -e CATALOG_WAREHOUSE=/var/lib/iceberg/warehouse \\")
	fmt.Println("     -e CATALOG_IO__IMPL=org.apache.iceberg.hadoop.HadoopFileIO \\")
	fmt.Println("     --name iceberg-rest tabulario/iceberg-rest")

	err = waitForCatalog(catalogURL, 10)
	if err != nil {
		log.Fatal("Failed to connect to Iceberg REST Catalog:", err)
	}

	fmt.Println("✅ Connected to Iceberg REST Catalog")

	// Create namespace
	namespaceName := "my_data"
	fmt.Printf("📁 Creating namespace '%s'...\n", namespaceName)

	// Try to create namespace, ignore if it already exists
	err = createNamespace(catalogURL, namespaceName)
	if err != nil {
		fmt.Printf("ℹ️  Namespace may already exist: %v\n", err)
	} else {
		fmt.Printf("✅ Namespace '%s' created successfully\n", namespaceName)
	}

	// Create Iceberg tables from Parquet files
	fmt.Println("\n🧊 Creating Iceberg tables with real schemas...")
	successCount := 0

	for _, parquetFile := range parquetFiles {
		relPath, _ := filepath.Rel(parquetDir, parquetFile)
		tableName := sanitizeTableName(parquetFile)

		fmt.Printf("\n🔄 Processing table '%s.%s' from %s...\n", namespaceName, tableName, relPath)

		// Get row count first
		rowCount, err := getParquetRowCount(db, parquetFile)
		if err != nil {
			fmt.Printf("⚠️  Failed to get row count: %v\n", err)
			rowCount = -1
		} else {
			fmt.Printf("📊 Data: %d rows in Parquet file\n", rowCount)
		}

		// Read the actual Parquet schema using DuckDB Go client
		fmt.Println("📋 Reading Parquet schema with DuckDB Go client...")
		icebergSchema, err := readParquetSchemaWithDuckDB(db, parquetFile)
		if err != nil {
			fmt.Printf("⚠️  Failed to read Parquet schema with DuckDB, using basic template: %v\n", err)
			icebergSchema = createBasicSchema(tableName)
		}

		fmt.Printf("📊 Schema: %d fields (from Parquet file)\n", len(icebergSchema.Fields))
		for i, field := range icebergSchema.Fields {
			if i < 5 { // Show first 5 fields
				required := ""
				if field.Required {
					required = " (required)"
				}
				fmt.Printf("   - %s: %s%s\n", field.Name, field.Type, required)
			} else if i == 5 {
				fmt.Printf("   ... and %d more fields\n", len(icebergSchema.Fields)-5)
				break
			}
		}

		// Create Iceberg table
		fmt.Printf("🔨 Creating Iceberg table '%s.%s'...\n", namespaceName, tableName)

		err = createTable(catalogURL, namespaceName, tableName, icebergSchema)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "409") {
				fmt.Printf("⚠️  Table '%s.%s' already exists, skipping...\n", namespaceName, tableName)
				continue
			}
			log.Printf("Failed to create table %s.%s: %v", namespaceName, tableName, err)
			continue
		}

		fmt.Printf("✅ Created Iceberg table '%s.%s'\n", namespaceName, tableName)

		// Read and display sample data
		fmt.Println("📖 Reading sample data from Parquet file...")
		sampleData, err := readParquetSampleDataWithDuckDB(db, parquetFile, 3)
		if err != nil {
			fmt.Printf("⚠️  Failed to read sample data: %v\n", err)
		} else {
			fmt.Printf("📊 Sample data (%d rows shown):\n", len(sampleData))
			for i, row := range sampleData {
				fmt.Printf("   Row %d: ", i+1)
				fieldCount := 0
				for key, value := range row {
					if fieldCount >= 3 { // Show only first 3 fields per row
						fmt.Printf("...")
						break
					}
					fmt.Printf("%s=%v ", key, value)
					fieldCount++
				}
				fmt.Println()
			}
		}

		successCount++
	}

	fmt.Printf("\n🎉 Successfully processed %d Iceberg tables!\n", successCount)

	// Show summary
	fmt.Println("\n📊 Summary:")
	fmt.Printf("   - Namespace: %s\n", namespaceName)
	fmt.Printf("   - Parquet files processed: %d\n", len(parquetFiles))
	fmt.Printf("   - Iceberg tables created: %d\n", successCount)
	fmt.Printf("   - Catalog URI: %s\n", catalogURL)
	fmt.Printf("   - Warehouse location: ./data/iceberg_warehouse\n")

	fmt.Println("\n💡 Tables created with real Parquet schemas!")
	fmt.Println("   - Tables now have the actual column structure from your data")
	fmt.Println("   - Schema information is stored in Iceberg metadata")
	fmt.Println("   - Nullability information is preserved from Parquet files")

	fmt.Println("\n🦆 DuckDB Go Client Integration:")
	fmt.Println("   - Native Go client for better performance and reliability")
	fmt.Println("   - Proper data type handling and conversion")
	fmt.Println("   - Real sample data preview with actual values")
	fmt.Println("   - Accurate row counts and schema information")

	fmt.Println("\n📝 Note about data insertion:")
	fmt.Println("   - Table structures are created with proper schemas")
	fmt.Println("   - For data loading into Iceberg tables, use:")
	fmt.Println("     • Apache Spark with Iceberg")
	fmt.Println("     • Trino with Iceberg connector")
	fmt.Println("     • Or copy data files manually to the warehouse")

	fmt.Println("\n🔧 Next steps:")
	fmt.Println("   - Use DuckDB to inspect your table schemas and data")
	fmt.Println("   - Set up Spark/Trino for data insertion into Iceberg tables")
	fmt.Println("   - Add partitioning strategies for better performance")
	fmt.Println("   - Set up table maintenance (compaction, cleanup)")
}
