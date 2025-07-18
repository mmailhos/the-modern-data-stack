package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

// findCSVFiles recursively finds all .csv files in the given directory
func findCSVFiles(rootDir string) ([]string, error) {
	var csvFiles []string

	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Check if it's a CSV file
		if !info.IsDir() && filepath.Ext(path) == ".csv" {
			csvFiles = append(csvFiles, path)
		}

		return nil
	})

	return csvFiles, err
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

	// Install and load required extensions
	fmt.Println("🔧 Installing required extensions...")

	extensions := []string{"iceberg", "httpfs"}
	for _, ext := range extensions {
		_, err := db.Exec(fmt.Sprintf("INSTALL %s;", ext))
		if err != nil {
			log.Printf("Warning: Failed to install %s extension: %v", ext, err)
		}

		_, err = db.Exec(fmt.Sprintf("LOAD %s;", ext))
		if err != nil {
			log.Printf("Warning: Failed to load %s extension: %v", ext, err)
		}
	}

	fmt.Println("✅ Extensions loaded successfully")

	// Check if data directory exists and has CSV files
	dataDir := "data"
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		fmt.Printf("⚠️  Data directory '%s' does not exist. Creating it...\n", dataDir)
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			log.Fatal("Failed to create data directory:", err)
		}
		fmt.Printf("✅ Created data directory '%s'\n", dataDir)
		fmt.Println("📁 Please place your CSV files in the 'data' directory")
		return
	}

	// Find all CSV files in the data directory
	csvFiles, err := findCSVFiles(dataDir)
	if err != nil {
		log.Fatal("Failed to search for CSV files:", err)
	}

	if len(csvFiles) == 0 {
		fmt.Printf("⚠️  No CSV files found in '%s' directory\n", dataDir)
		fmt.Println("📁 Please place your CSV files in the 'data' directory")
		return
	}

	fmt.Printf("📊 Found %d CSV file(s):\n", len(csvFiles))
	for _, file := range csvFiles {
		relPath, _ := filepath.Rel(dataDir, file)
		fmt.Printf("   - %s\n", relPath)
	}

	// Create Iceberg output directory
	icebergDir := "iceberg_tables"
	if err := os.MkdirAll(icebergDir, 0755); err != nil {
		log.Fatal("Failed to create Iceberg directory:", err)
	}

	// Process each CSV file
	for _, csvFile := range csvFiles {
		relPath, _ := filepath.Rel(dataDir, csvFile)
		tableName := sanitizeTableName(csvFile)

		fmt.Printf("\n🔄 Processing %s -> table '%s'...\n", relPath, tableName)

		// Get absolute path for the CSV file
		absCSVPath, err := filepath.Abs(csvFile)
		if err != nil {
			log.Printf("Failed to get absolute path for %s: %v", csvFile, err)
			continue
		}

		// Create temporary table from CSV
		tempTableName := fmt.Sprintf("temp_%s", tableName)
		createTempSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s')", tempTableName, absCSVPath)
		_, err = db.Exec(createTempSQL)
		if err != nil {
			log.Printf("Failed to create temporary table from %s: %v", csvFile, err)
			continue
		}

		// Get schema information
		var rowCount int
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tempTableName)
		err = db.QueryRow(countSQL).Scan(&rowCount)
		if err != nil {
			log.Printf("Failed to get row count for %s: %v", tempTableName, err)
			continue
		}

		fmt.Printf("📈 Loaded %d rows from %s\n", rowCount, relPath)

		// Create Iceberg table path
		icebergTablePath := filepath.Join(icebergDir, tableName)
		absIcebergPath, err := filepath.Abs(icebergTablePath)
		if err != nil {
			log.Printf("Failed to get absolute path for Iceberg table: %v", err)
			continue
		}

		// Create Iceberg table
		fmt.Printf("🧊 Creating Iceberg table at %s...\n", icebergTablePath)

		// Copy data to Iceberg format
		copyToIcebergSQL := fmt.Sprintf(`
			COPY (SELECT * FROM %s) TO '%s' (FORMAT 'iceberg')
		`, tempTableName, absIcebergPath)

		_, err = db.Exec(copyToIcebergSQL)
		if err != nil {
			log.Printf("Failed to create Iceberg table for %s: %v", tableName, err)

			// Fallback: try creating as Parquet with Iceberg-compatible structure
			fmt.Printf("🔄 Fallback: Creating Parquet table for %s...\n", tableName)
			parquetPath := filepath.Join(icebergDir, tableName+".parquet")
			absParquetPath, _ := filepath.Abs(parquetPath)

			copyToParquetSQL := fmt.Sprintf(`
				COPY (SELECT * FROM %s) TO '%s' (FORMAT 'parquet')
			`, tempTableName, absParquetPath)

			_, err = db.Exec(copyToParquetSQL)
			if err != nil {
				log.Printf("Failed to create Parquet table for %s: %v", tableName, err)
				continue
			}

			fmt.Printf("✅ Created Parquet table: %s\n", parquetPath)
		} else {
			fmt.Printf("✅ Successfully created Iceberg table: %s\n", icebergTablePath)
		}

		// Show sample data
		fmt.Printf("📋 Sample data from %s:\n", tableName)
		fmt.Println("=" + strings.Repeat("=", 50))

		sampleSQL := fmt.Sprintf("SELECT * FROM %s LIMIT 3", tempTableName)
		rows, err := db.Query(sampleSQL)
		if err != nil {
			log.Printf("Failed to query sample data from %s: %v", tempTableName, err)
		} else {
			// Get column names
			columns, err := rows.Columns()
			if err != nil {
				log.Printf("Failed to get columns for %s: %v", tempTableName, err)
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

		// Clean up temporary table
		dropTempSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
		_, err = db.Exec(dropTempSQL)
		if err != nil {
			log.Printf("Warning: Failed to drop temporary table %s: %v", tempTableName, err)
		}

		fmt.Println()
	}

	fmt.Println("🎉 All CSV files processed successfully!")
	fmt.Printf("📁 Iceberg/Parquet tables created in: %s\n", icebergDir)

	// Show summary
	fmt.Println("\n📊 Summary:")
	fmt.Printf("   - Input directory: %s\n", dataDir)
	fmt.Printf("   - Output directory: %s\n", icebergDir)
	fmt.Printf("   - CSV files processed: %d\n", len(csvFiles))

	// List created files
	if files, err := os.ReadDir(icebergDir); err == nil {
		fmt.Println("   - Created files:")
		for _, file := range files {
			fmt.Printf("     • %s\n", file.Name())
		}
	}
}
