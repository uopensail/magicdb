package engine

import (
	"magicdb/engine/model"
	"magicdb/engine/table"
	"path/filepath"
	"sync"

	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

// Tables structure to hold table references
type Tables struct {
	tableMap map[string]*table.Table
}

// DataBase structure for managing database operations
type DataBase struct {
	mergeOperator table.MergeOperator
	tables        *Tables
}

// NewDataBase initializes a new DataBase instance from the given configuration.
// It copies table data directories to the specified destination within the working directory
// and logs any errors encountered during this process.
func NewDataBase(config *model.DataBase) *DataBase {
	// Create a map to hold table references, initialized with the number of tables in config
	tableMap := make(map[string]*table.Table, len(config.Tables))

	// Iterate over each table in the configuration
	for _, tbl := range config.Tables {
		// Construct destination path for the table data
		dstPath := filepath.Join(config.Workdir, tbl.Version, tbl.Name)

		// Copy the table data directory to the destination path
		if err := table.CopyDir(tbl.DataDir, dstPath); err != nil {
			zlog.LOG.Error("Failed to copy table directory",
				zap.String("table_name", tbl.Name),
				zap.String("source_dir", tbl.DataDir),
				zap.String("destination_dir", dstPath),
				zap.Error(err))
			// Continue to the next table without adding this one
			continue
		}

		// Create a new table instance
		newTable := table.NewTable(tbl.Name, dstPath)
		if newTable != nil {
			// Add the new table to the map
			tableMap[tbl.Name] = newTable
		}
	}

	// Return a new DataBase instance with the initialized tables and a merge operator
	return &DataBase{
		mergeOperator: &table.JSONMergeOperator{},  // Initialize the merge operator
		tables:        &Tables{tableMap: tableMap}, // Initialize tables map
	}
}

// Get retrieves a merged value for the given key across specified tables
func (db *DataBase) Get(key string, tableNames []string) []byte {
	currentTables := db.tables

	var result []byte
	resultChannel := make(chan []byte, len(tableNames)) // Channel for storing table results
	var waitGroup sync.WaitGroup

	// Iterate through table names and retrieve data
	for _, tableName := range tableNames {
		if tableInstance, exists := currentTables.tableMap[tableName]; exists && tableInstance != nil {
			waitGroup.Add(1) // Increment wait group before launching goroutine
			go func(tbl *table.Table) {
				defer waitGroup.Done() // Decrement wait group after execution
				data, err := tbl.Get(key)
				if err == nil { // Only send data if no error occurred
					resultChannel <- data
				}
			}(tableInstance)
		}
	}

	waitGroup.Wait()     // Wait for all goroutines to finish
	close(resultChannel) // Close channel to signal completion

	// Merge results from all tables
	for value := range resultChannel {
		result = db.mergeOperator.Merge(value, result)
	}

	return result
}

// GetAll retrieves a merged value for the given key across all tables
func (db *DataBase) GetAll(key string) []byte {
	currentTables := db.tables

	resultChannel := make(chan []byte, len(currentTables.tableMap)) // Channel for storing table results
	var waitGroup sync.WaitGroup

	// Iterate through all tables and retrieve data
	for _, tableInstance := range currentTables.tableMap {
		waitGroup.Add(1) // Increment wait group before launching goroutine
		go func(tbl *table.Table) {
			defer waitGroup.Done() // Decrement wait group after execution
			data, err := tbl.Get(key)
			if err == nil { // Only send data if no error occurred
				resultChannel <- data
			}
		}(tableInstance)
	}

	waitGroup.Wait()     // Wait for all goroutines to finish
	close(resultChannel) // Close channel to signal completion

	var result []byte
	// Merge results from all tables
	for value := range resultChannel {
		result = db.mergeOperator.Merge(value, result)
	}

	return result
}
