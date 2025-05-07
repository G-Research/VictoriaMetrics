package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

// mustOpenFilePart opens file-based part from the given path.
func openFilePart(path string) (*part, error) {
	path = filepath.Clean(path)

	var ph partHeader
	if err := ph.readMetadata(path); err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	timestampsPath := filepath.Join(path, timestampsFilename)
	timestampsFile := fs.MustOpenReaderAt(timestampsPath)
	timestampsSize := fs.MustFileSize(timestampsPath)

	valuesPath := filepath.Join(path, valuesFilename)
	valuesFile := fs.MustOpenReaderAt(valuesPath)
	valuesSize := fs.MustFileSize(valuesPath)

	indexPath := filepath.Join(path, indexFilename)
	indexFile := fs.MustOpenReaderAt(indexPath)
	indexSize := fs.MustFileSize(indexPath)

	metaindexPath := filepath.Join(path, metaindexFilename)
	metaindexFile, err := filestream.Open(metaindexPath, true)
	if err != nil {
		return nil, fmt.Errorf("failed to open main index path: %w", err)
	}
	metaindexSize := fs.MustFileSize(metaindexPath)

	size := timestampsSize + valuesSize + indexSize + metaindexSize
	return newPartReadOnly(&ph, path, size, metaindexFile, timestampsFile, valuesFile, indexFile)
}

func (ph *partHeader) readMetadata(partPath string) error {
	ph.Reset()

	metadataPath := filepath.Join(partPath, metadataFilename)
	if !fs.IsPathExist(metadataPath) {
		// This is a part created before v1.90.0.
		// Fall back to reading the metadata from the partPath itself.
		if err := ph.ParseFromPath(partPath); err != nil {
			return fmt.Errorf("failed to parse part header from path %q: %w", partPath, err)
		}
	} else {
		metadata, err := os.ReadFile(metadataPath)
		if err != nil {
			return fmt.Errorf("cannot read %q: %s", metadataPath, err)
		}
		if err := json.Unmarshal(metadata, ph); err != nil {
			return fmt.Errorf("cannot parse %q: %s", metadataPath, err)
		}
	}

	// Perform various checks
	if ph.MinTimestamp > ph.MaxTimestamp {
		return fmt.Errorf("minTimestamp cannot exceed maxTimestamp at %q; got %d vs %d", metadataPath, ph.MinTimestamp, ph.MaxTimestamp)
	}
	if ph.RowsCount <= 0 {
		return fmt.Errorf("rowsCount must be greater than 0 at %q; got %d", metadataPath, ph.RowsCount)
	}
	if ph.BlocksCount <= 0 {
		return fmt.Errorf("blocksCount must be greater than 0 at %q; got %d", metadataPath, ph.BlocksCount)
	}
	if ph.BlocksCount > ph.RowsCount {
		return fmt.Errorf("blocksCount cannot be bigger than rowsCount at %q; got blocksCount=%d, rowsCount=%d", metadataPath, ph.BlocksCount, ph.RowsCount)
	}
	return nil
}
