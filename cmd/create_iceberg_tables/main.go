package main

import (
	"database/sql"
	"fmt"
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
func waitForCatalog(db *sql.DB, maxRetries int) error {
	catalogURL := "http://localhost:8181"

	// First, check if catalog is already attached
	rows, err := db.Query("SELECT database_name FROM duckdb_databases() WHERE database_name = 'iceberg_catalog'")
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			fmt.Println("✅ Iceberg catalog already attached")
			return nil
		}
	}

	// Check HTTP connectivity first
	fmt.Println("🔍 Checking HTTP connectivity to catalog...")
	for i := 0; i < 5; i++ {
		if err := checkCatalogHTTP(catalogURL); err == nil {
			fmt.Println("✅ Catalog HTTP endpoint is responding")
			break
		} else if i < 4 {
			fmt.Printf("⏳ HTTP check failed (attempt %d/5): %v\n", i+1, err)
			time.Sleep(2 * time.Second)
		} else {
			return fmt.Errorf("catalog HTTP endpoint not responding after 5 attempts")
		}
	}

	// Now try to attach via DuckDB
	for i := 0; i < maxRetries; i++ {
		// Try to attach the catalog
		_, err := db.Exec(`
			ATTACH '' AS iceberg_catalog (
				TYPE ICEBERG,
				CATALOG 'rest',
				URI 'http://localhost:8181'
			)
		`)
		if err == nil {
			return nil
		}

		if i < maxRetries-1 {
			fmt.Printf("⏳ DuckDB connection attempt %d/%d failed: %v\n", i+1, maxRetries, err)
			time.Sleep(3 * time.Second)
		}
	}
	return fmt.Errorf("failed to connect to Iceberg REST Catalog after %d attempts", maxRetries)
}

func main() {
	// Connect to DuckDB (in-memory database)
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal("Failed to connect to DuckDB:", err)
	}
	defer db.Close()

	// Test the connection
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping DuckDB:", err)
	}

	fmt.Println("✅ Connected to DuckDB successfully")

	// Install and load Iceberg extension
	fmt.Println("🔧 Installing and loading Iceberg extension...")

	_, err = db.Exec("INSTALL 'iceberg';")
	if err != nil {
		log.Fatal("Failed to install Iceberg extension:", err)
	}

	_, err = db.Exec("LOAD 'iceberg';")
	if err != nil {
		log.Fatal("Failed to load Iceberg extension:", err)
	}

	fmt.Println("✅ Iceberg extension loaded successfully")

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
	fmt.Println("\n🔗 Connecting to Iceberg REST Catalog...")
	fmt.Println("💡 Make sure the Iceberg REST Catalog is running:")
	fmt.Println("   docker run -d --rm -p 8181:8181 \\")
	fmt.Println("     -v $PWD/data/iceberg_warehouse:/var/lib/iceberg/warehouse \\")
	fmt.Println("     --name iceberg-rest tabulario/iceberg-rest")

	err = waitForCatalog(db, 10)
	if err != nil {
		log.Fatal("Failed to connect to Iceberg REST Catalog:", err)
	}

	fmt.Println("✅ Connected to Iceberg REST Catalog")

	// Switch to use the Iceberg catalog
	_, err = db.Exec("USE iceberg_catalog;")
	if err != nil {
		log.Fatal("Failed to use Iceberg catalog:", err)
	}

	// Create schema
	schemaName := "my_data"
	fmt.Printf("📁 Creating schema '%s'...\n", schemaName)
	_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schemaName))
	if err != nil {
		log.Fatal("Failed to create schema:", err)
	}

	fmt.Printf("✅ Schema '%s' created successfully\n", schemaName)

	// Create Iceberg tables from Parquet files
	fmt.Println("\n🧊 Creating Iceberg tables...")
	successCount := 0

	for _, parquetFile := range parquetFiles {
		relPath, _ := filepath.Rel(parquetDir, parquetFile)
		tableName := sanitizeTableName(parquetFile)
		fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)

		fmt.Printf("\n🔄 Creating table '%s' from %s...\n", fullTableName, relPath)

		// Get absolute path for the Parquet file
		absParquetPath, err := filepath.Abs(parquetFile)
		if err != nil {
			log.Printf("Failed to get absolute path for %s: %v", parquetFile, err)
			continue
		}

		// Create Iceberg table from Parquet file
		createTableSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM '%s';", fullTableName, absParquetPath)
		_, err = db.Exec(createTableSQL)
		if err != nil {
			log.Printf("Failed to create table %s: %v", fullTableName, err)
			continue
		}

		// Get row count
		var rowCount int
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", fullTableName)
		err = db.QueryRow(countSQL).Scan(&rowCount)
		if err != nil {
			log.Printf("Failed to get row count for %s: %v", fullTableName, err)
		} else {
			fmt.Printf("✅ Created table '%s' with %d rows\n", fullTableName, rowCount)
		}

		// Show sample data
		fmt.Printf("📋 Sample data from %s:\n", fullTableName)
		fmt.Println("=" + strings.Repeat("=", 50))

		sampleSQL := fmt.Sprintf("SELECT * FROM %s LIMIT 3", fullTableName)
		rows, err := db.Query(sampleSQL)
		if err != nil {
			log.Printf("Failed to query sample data from %s: %v", fullTableName, err)
		} else {
			// Get column names
			columns, err := rows.Columns()
			if err != nil {
				log.Printf("Failed to get columns for %s: %v", fullTableName, err)
			} else {
				// Print header
				for i, col := range columns {
					if i > 0 {
						fmt.Print(" | ")
					}
					fmt.Printf("%-15s", col)
				}
				fmt.Println()
				fmt.Println(strings.Repeat("-", len(columns)*18))

				// Print sample data
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				sampleCount := 0
				for rows.Next() && sampleCount < 3 {
					err := rows.Scan(valuePtrs...)
					if err != nil {
						log.Printf("Failed to scan row: %v", err)
						continue
					}

					for i, val := range values {
						if i > 0 {
							fmt.Print(" | ")
						}
						if val == nil {
							fmt.Printf("%-15s", "NULL")
						} else {
							fmt.Printf("%-15v", val)
						}
					}
					fmt.Println()
					sampleCount++
				}
			}
			rows.Close()
		}

		successCount++
	}

	fmt.Printf("\n🎉 Successfully created %d Iceberg tables!\n", successCount)

	// Show summary
	fmt.Println("\n📊 Summary:")
	fmt.Printf("   - Schema: %s\n", schemaName)
	fmt.Printf("   - Parquet files processed: %d\n", len(parquetFiles))
	fmt.Printf("   - Iceberg tables created: %d\n", successCount)
	fmt.Printf("   - Catalog URI: http://localhost:8181\n")
	fmt.Printf("   - Warehouse location: ./data/iceberg_warehouse\n")

	// List all tables in the schema
	fmt.Println("\n📋 Created tables:")
	listTablesSQL := fmt.Sprintf("SHOW TABLES FROM %s;", schemaName)
	rows, err := db.Query(listTablesSQL)
	if err != nil {
		log.Printf("Failed to list tables: %v", err)
	} else {
		for rows.Next() {
			var tableName string
			err := rows.Scan(&tableName)
			if err != nil {
				log.Printf("Failed to scan table name: %v", err)
				continue
			}
			fmt.Printf("   - %s.%s\n", schemaName, tableName)
		}
		rows.Close()
	}

	fmt.Println("\n💡 You can now query these Iceberg tables using:")
	fmt.Println("   - DuckDB with the Iceberg extension")
	fmt.Println("   - Any Iceberg-compatible query engine")
	fmt.Println("   - The REST Catalog API at http://localhost:8181")
}
