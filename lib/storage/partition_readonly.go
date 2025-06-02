package storage

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

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
	var i int
	m := make(map[string]struct{}, len(partNames))
outer:
	for i = range 5 {
		pws = pws[:0]
		clear(m)
		for _, partName := range partNames {
			// Make sure the partName exists on disk.
			// If it is missing, then manual action from the user is needed,
			// since this is unexpected state, which cannot occur under normal operation,
			// including unclean shutdown.
			partPath := filepath.Join(path, partName)
			if !fs.IsPathExist(partPath) {
				logger.Errorf("part %q is listed in %q, but is missing on disk; "+
					"ensure %q contents is not corrupted; remove %q to rebuild its content from the list of existing parts",
					partPath, partsFile, partsFile, partsFile)
				time.Sleep(100 * time.Millisecond)
				continue outer
			}

			m[partName] = struct{}{}
		}

		for _, partName := range partNames {
			partPath := filepath.Join(path, partName)
			p, err := openFilePart(partPath)
			if err != nil {
				logger.Errorf("failed to open file part %q: %w", partPath, err)
				time.Sleep(100 * time.Millisecond)
				continue outer
			}
			pw := &partWrapper{
				p: p,
			}
			pw.incRef()
			pws = append(pws, pw)
		}
		break
	}

	if i == 5 {
		return nil, fmt.Errorf("failed to open %d parts from %q after 5 attempts", len(partNames), path)
	} else if i > 0 {
		logger.Infof("opened %d parts from %q after %d attempts", len(partNames), path, i)
	}

	return pws, nil
}
