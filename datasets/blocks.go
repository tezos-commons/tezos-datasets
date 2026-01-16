package datasets

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pierrec/lz4/v4"
)

const (
	// BlocksPerFile is the number of blocks stored in each LZ4 file
	BlocksPerFile = 100
)

// BlocksDataset handles writing blocks to LZ4 compressed files
type BlocksDataset struct {
	outputDir string
}

// NewBlocksDataset creates a new blocks dataset writer
func NewBlocksDataset(outputDir string) (*BlocksDataset, error) {
	blocksDir := filepath.Join(outputDir, "blocks")
	if err := os.MkdirAll(blocksDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create blocks directory: %w", err)
	}
	return &BlocksDataset{outputDir: blocksDir}, nil
}

// WriteBatch writes a batch of blocks to an LZ4 compressed file
// startLevel should be the first block level, and blocks should contain exactly BlocksPerFile blocks
func (d *BlocksDataset) WriteBatch(startLevel int64, blocks [][]byte) error {
	if len(blocks) != BlocksPerFile {
		return fmt.Errorf("expected %d blocks, got %d", BlocksPerFile, len(blocks))
	}

	endLevel := startLevel + int64(BlocksPerFile)
	filename := fmt.Sprintf("%d-%d.lz4", startLevel, endLevel)
	filepath := filepath.Join(d.outputDir, filename)

	// Compact each block's JSON and join with newlines
	var buffer bytes.Buffer
	for i, block := range blocks {
		// Compact the JSON (remove whitespace)
		compacted, err := compactJSON(block)
		if err != nil {
			return fmt.Errorf("failed to compact JSON for block %d: %w", startLevel+int64(i), err)
		}
		buffer.Write(compacted)
		buffer.WriteByte('\n')
	}

	// Compress with LZ4 at maximum compression level
	compressed, err := compressLZ4(buffer.Bytes())
	if err != nil {
		return fmt.Errorf("failed to compress blocks: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filepath, compressed, 0644); err != nil {
		return fmt.Errorf("failed to write file %s: %w", filepath, err)
	}

	return nil
}

// compactJSON removes whitespace from JSON
func compactJSON(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	if err := json.Compact(&buffer, data); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// compressLZ4 compresses data using LZ4 at maximum compression level
func compressLZ4(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := lz4.NewWriter(&buffer)
	writer.Apply(lz4.CompressionLevelOption(lz4.Level9))

	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// FileExists checks if a batch file already exists
func (d *BlocksDataset) FileExists(startLevel int64) bool {
	endLevel := startLevel + int64(BlocksPerFile)
	filename := fmt.Sprintf("%d-%d.lz4", startLevel, endLevel)
	filepath := filepath.Join(d.outputDir, filename)
	_, err := os.Stat(filepath)
	return err == nil
}
