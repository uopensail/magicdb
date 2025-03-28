package table

// MergeOperator defines an interface for merging two byte slices.
// Implementations should handle the specific merging strategy for different data formats.
type MergeOperator interface {
	// Merge combines two byte slices and returns the merged result.
	// The implementation should handle format-specific merging logic.
	Merge(left, right []byte) []byte
}

// JSONMergeOperator implements MergeOperator for merging JSON fragments.
// It specializes in concatenating JSON array elements with proper comma separation.
type JSONMergeOperator struct{}

// Merge combines two JSON fragments while maintaining valid JSON syntax.
// It inserts a comma between the left and right values and handles empty inputs.
func (m *JSONMergeOperator) Merge(left, right []byte) []byte {
	// Handle edge cases for empty inputs
	switch {
	case len(left) == 0:
		return right
	case len(right) == 0:
		return left
	}

	// Calculate total size: left + comma + right
	totalSize := len(left) + len(right) - 1
	merged := make([]byte, totalSize)

	// Copy left part
	copy(merged, left)

	// Copy right part
	copy(merged[len(left)-1:], right)

	// Add comma separator
	merged[len(left)-1] = ','

	return merged
}
