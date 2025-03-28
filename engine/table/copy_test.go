package table

import (
	"fmt"
	"os"
	"testing"
)

func Test_Copy(t *testing.T) {
	if err := CopyDir("/tmp/src", "/tmp/dst"); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("File copy completed successfully")
}
