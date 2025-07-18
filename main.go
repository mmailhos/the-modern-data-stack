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

	// Check if data directory exists and has parquet files
	dataDir := "data"
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		fmt.Printf("⚠️  Data directory '%s' does not exist. Creating it...\n", dataDir)
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			log.Fatal("Failed to create data directory:", err)
		}
		fmt.Printf("✅ Created data directory '%s'\n", dataDir)
		fmt.Println("📁 Please place your parquet files in the 'data' directory")
		return
	}

	// Find all parquet files in the data directory and subdirectories
	parquetFiles, err := findParquetFiles(dataDir)
	if err != nil {
		log.Fatal("Failed to search for parquet files:", err)
	}

	if len(parquetFiles) == 0 {
		fmt.Printf("⚠️  No parquet files found in '%s' directory\n", dataDir)
		fmt.Println("📁 Please place your parquet files in the 'data' directory")
		return
	}

	fmt.Printf("📊 Found %d parquet file(s):\n", len(parquetFiles))
	for _, file := range parquetFiles {
		relPath, _ := filepath.Rel(dataDir, file)
		fmt.Printf("   - %s\n", relPath)
	}

	// Load each parquet file into DuckDB
	for _, parquetFile := range parquetFiles {
		// Create table name from relative path, replacing path separators with underscores
		relPath, _ := filepath.Rel(dataDir, parquetFile)
		tableName := strings.ReplaceAll(relPath[:len(relPath)-len(filepath.Ext(relPath))], string(filepath.Separator), "_")
		// Replace any remaining special characters that might cause SQL issues
		tableName = strings.ReplaceAll(tableName, "-", "_")

		fmt.Printf("\n🔄 Loading %s into table '%s'...\n", relPath, tableName)

		// Create table from parquet file (use absolute path to avoid path issues)
		absPath, err := filepath.Abs(parquetFile)
		if err != nil {
			log.Printf("Failed to get absolute path for %s: %v", parquetFile, err)
			continue
		}
		createTableSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')", tableName, absPath)
		_, err = db.Exec(createTableSQL)
		if err != nil {
			log.Printf("Failed to load %s: %v", parquetFile, err)
			continue
		}

		fmt.Printf("✅ Successfully loaded %s into table '%s'\n", relPath, tableName)

		// Get row count
		var rowCount int
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
		err = db.QueryRow(countSQL).Scan(&rowCount)
		if err != nil {
			log.Printf("Failed to get row count for %s: %v", tableName, err)
			continue
		}

		fmt.Printf("📈 Table '%s' contains %d rows\n", tableName, rowCount)

		// List all rows
		fmt.Printf("\n📋 All rows in table '%s':\n", tableName)
		fmt.Println("=" + strings.Repeat("=", 50))

		rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
		if err != nil {
			log.Printf("Failed to query %s: %v", tableName, err)
			continue
		}

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			log.Printf("Failed to get columns for %s: %v", tableName, err)
			rows.Close()
			continue
		}

		// Print header
		for i, col := range columns {
			if i > 0 {
				fmt.Print(" | ")
			}
			fmt.Printf("%-15s", col)
		}
		fmt.Println()
		fmt.Println(strings.Repeat("-", len(columns)*18))

		// Print data
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		rowCount = 0
		for rows.Next() {
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
			rowCount++

			// Limit output to first 10 rows to avoid overwhelming output
			if rowCount >= 10 {
				fmt.Printf("... (showing first 10 rows, total rows: %d)\n", rowCount)
				break
			}
		}

		rows.Close()
		fmt.Println()
	}

	fmt.Println("🎉 All parquet files processed successfully!")
}
