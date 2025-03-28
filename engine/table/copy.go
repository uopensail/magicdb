package table

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	maxWorkers     = 4               // Limit concurrent goroutines
	copyBufferSize = 1 * 1024 * 1024 // 1M buffer
	success        = "_SUCCESS"      // success mark
	extension      = ".db"           // sqlite db file extension
)

// CopyConfig contains configuration parameters for file copy operation
type CopyConfig struct {
	SrcDir    string // Source directory path
	DstDir    string // Destination directory path
	CheckFile string // Success check filename
	Extension string // Target file extension
}

// NewCopyConfig creates a new CopyConfig with default values
func NewCopyConfig(src, dst string) *CopyConfig {
	return &CopyConfig{
		SrcDir:    src,
		DstDir:    dst,
		CheckFile: success,
		Extension: extension,
	}
}

// Validate checks if required conditions are met for copying
func (c *CopyConfig) Validate() error {
	// Check for existence of success file
	successFile := filepath.Join(c.SrcDir, c.CheckFile)
	if _, err := os.Stat(successFile); os.IsNotExist(err) {
		return fmt.Errorf("success file not found: %s", successFile)
	}
	return nil
}

// CopyDir performs the actual file copy operation
func CopyDir(src, dst string) error {
	config := NewCopyConfig(src, dst)
	// Validate pre-conditions
	if err := config.Validate(); err != nil {
		return err
	}

	// Read source directory
	entries, err := os.ReadDir(config.SrcDir)
	if err != nil {
		return fmt.Errorf("error reading source directory: %w", err)
	}

	// Create destination directory if not exists
	if err := os.MkdirAll(config.DstDir, os.ModePerm); err != nil {
		return fmt.Errorf("error creating destination directory: %w", err)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(entries))
	sem := make(chan struct{}, maxWorkers)

	// Process each entry in source directory
	for _, entry := range entries {
		// Skip directories and non-matching extensions
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), config.Extension) {
			continue
		}

		wg.Add(1)
		go func(e os.DirEntry) {
			defer wg.Done()
			sem <- struct{}{}        // Acquire semaphore
			defer func() { <-sem }() // Release semaphore

			srcPath := filepath.Join(config.SrcDir, e.Name())
			dstPath := filepath.Join(config.DstDir, e.Name())

			if err := copyFile(srcPath, dstPath); err != nil {
				errChan <- fmt.Errorf("error copying %s: %w", srcPath, err)
			}
		}(entry)
	}

	// Wait for all operations to complete
	wg.Wait()
	close(errChan)
	close(sem)

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("encountered %d errors during copy", len(errors))
	}

	// create _SUCCESS file
	successFile := filepath.Join(config.DstDir, config.CheckFile)
	file, err := os.Create(successFile)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	return nil
}

// copyFile handles the actual file copy operation with buffer
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	// Use buffer for more efficient copy
	buf := make([]byte, copyBufferSize)
	if _, err = io.CopyBuffer(destFile, sourceFile, buf); err != nil {
		return err
	}

	return destFile.Sync() // Ensure file is flushed to disk
}
