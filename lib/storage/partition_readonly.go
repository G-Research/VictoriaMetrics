package storage

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// partition represents a partition.

// mustOpenPartitionReadOnly opens the existing partition from the given paths.
func mustOpenPartitionReadOnly(smallPartsPath, bigPartsPath string) (*partition, error) {
	smallPartsPath = filepath.Clean(smallPartsPath)
	bigPartsPath = filepath.Clean(bigPartsPath)

	name := filepath.Base(smallPartsPath)
	if !strings.HasSuffix(bigPartsPath, name) {
		return nil, fmt.Errorf("partition name in bigPartsPath %q doesn't match smallPartsPath %q; want %q", bigPartsPath, smallPartsPath, name)
	}

	var tr TimeRange
	if err := tr.fromPartitionName(name); err != nil {
		logger.Panicf("FATAL: cannot obtain partition time range from smallPartsPath %q: %s", smallPartsPath, err)
	}

	partsFile := filepath.Join(smallPartsPath, partsFilename)
	partNamesSmall, partNamesBig, err := mustReadPartNamesReadOnly(partsFile, smallPartsPath, bigPartsPath)
	if err != nil {
		return nil, err
	}

	smallParts, err := openParts(partsFile, smallPartsPath, partNamesSmall)
	if err != nil {
		return nil, err
	}
	bigParts, err := openParts(partsFile, bigPartsPath, partNamesBig)
	if err != nil {
		return nil, err
	}
	if !fs.IsPathExist(partsFile) {
		// Create parts.json file if it doesn't exist yet.
		// This should protect from possible carshloops just after the migration from versions below v1.90.0
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4336
		return nil, fmt.Errorf("parts.json file doesn't exist yet")
	}

	pt := newPartition(name, smallPartsPath, bigPartsPath, tr, nil) // Storage is used for merging so hopefully, we can get away with nil
	pt.smallParts = smallParts
	pt.bigParts = bigParts

	return pt, nil
}

func openParts(partsFile, path string, partNames []string) ([]*partWrapper, error) {
	if !fs.IsPathExist(path) {
		return nil, nil
	}

	// Open parts
	var pws []*partWrapper
	m := make(map[string]struct{}, len(partNames))
	for _, partName := range partNames {
		// Make sure the partName exists on disk.
		// If it is missing, then manual action from the user is needed,
		// since this is unexpected state, which cannot occur under normal operation,
		// including unclean shutdown.
		partPath := filepath.Join(path, partName)
		if !fs.IsPathExist(partPath) {
			return nil, fmt.Errorf("part %q is missing on disk; partsFile=%q; path=%q; partNames=%q; this is unexpected state, which cannot occur under normal operation, including unclean shutdown; manual action is needed to fix the issue", partName, partsFile, path, strings.Join(partNames, ", "))
		}

		m[partName] = struct{}{}
	}

	for _, partName := range partNames {
		partPath := filepath.Join(path, partName)
		p, err := openFilePart(partPath)
		if err != nil {
			return nil, fmt.Errorf("cannot open part %q for path %q: %w", partName, path, err)
		}
		pw := &partWrapper{
			p: p,
		}
		pw.incRef()
		pws = append(pws, pw)
	}

	return pws, nil
}
