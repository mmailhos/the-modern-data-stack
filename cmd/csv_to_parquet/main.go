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

	// No extensions needed for Parquet conversion
	fmt.Println("🔧 Ready for Parquet conversion...")

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

	// Create Parquet output directory
	parquetDir := "data/parquet"
	if err := os.MkdirAll(parquetDir, 0755); err != nil {
		log.Fatal("Failed to create Parquet directory:", err)
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

		// Create Parquet table path
		parquetPath := filepath.Join(parquetDir, tableName+".parquet")
		absParquetPath, err := filepath.Abs(parquetPath)
		if err != nil {
			log.Printf("Failed to get absolute path for Parquet table: %v", err)
			continue
		}

		// Create Parquet table
		fmt.Printf("📦 Creating Parquet table at %s...\n", parquetPath)

		// Copy data to Parquet format
		copyToParquetSQL := fmt.Sprintf(`
			COPY (SELECT * FROM %s) TO '%s' (FORMAT 'parquet')
		`, tempTableName, absParquetPath)

		_, err = db.Exec(copyToParquetSQL)
		if err != nil {
			log.Printf("Failed to create Parquet table for %s: %v", tableName, err)
			continue
		}

		fmt.Printf("✅ Created Parquet table: %s\n", parquetPath)

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
	fmt.Printf("📁 Parquet tables created in: %s\n", parquetDir)

	// Show summary
	fmt.Println("\n📊 Summary:")
	fmt.Printf("   - Input directory: %s\n", dataDir)
	fmt.Printf("   - Output directory: %s\n", parquetDir)
	fmt.Printf("   - CSV files processed: %d\n", len(csvFiles))

	// List created files
	if files, err := os.ReadDir(parquetDir); err == nil {
		fmt.Println("   - Created files:")
		for _, file := range files {
			fmt.Printf("     • %s\n", file.Name())
		}
	}
}
