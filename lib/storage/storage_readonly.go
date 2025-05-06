package storage

import (
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type ReadOnlyConfig struct {
	Retention          time.Duration
	DisablePerDayIndex bool
	StoragePath        string
}

type ReadOnlyStorage struct {
	retentionMsecs     int64
	storagePath        string
	disablePerDayIndex bool

	// The minimum timestamp when composite index search can be used.
	minTimestampForCompositeIndex int64
}

func NewReadOnlyStorage(cfg *ReadOnlyConfig) *ReadOnlyStorage {
	retention := cfg.Retention
	if retention <= 0 || retention > retentionMax {
		retention = retentionMax
	}

	return &ReadOnlyStorage{
		retentionMsecs:     retention.Milliseconds(),
		storagePath:        cfg.StoragePath,
		disablePerDayIndex: cfg.DisablePerDayIndex,
	}
}

// RetentionMsecs returns retentionMsecs for s.
func (s *ReadOnlyStorage) RetentionMsecs() int64 {
	return s.retentionMsecs
}

func (s *ReadOnlyStorage) adjustTimeRange(tr TimeRange) TimeRange {
	if s.disablePerDayIndex {
		return globalIndexTimeRange
	}

	minDate, maxDate := tr.DateRange()
	if maxDate-minDate > maxDaysForPerDaySearch {
		return globalIndexTimeRange
	}

	return tr
}

func (s *ReadOnlyStorage) mustOpenIndexDBTables(path string) *indexDBReadOnly {
	// Search for the three most recent tables - the prev, curr and next.
	des := fs.MustReadDir(path)
	var tableNames []string
	for _, de := range des {
		if !fs.IsDirOrSymlink(de) {
			// Skip non-directories.
			continue
		}
		tableName := de.Name()
		if !indexDBTableNameRegexp.MatchString(tableName) {
			// Skip invalid directories.
			continue
		}
		tableNames = append(tableNames, tableName)
	}
	sort.Slice(tableNames, func(i, j int) bool {
		return tableNames[i] < tableNames[j]
	})
	switch len(tableNames) {
	case 0:
		prevName := nextIndexDBTableName()
		currName := nextIndexDBTableName()
		nextName := nextIndexDBTableName()
		tableNames = append(tableNames, prevName, currName, nextName)
	case 1:
		currName := nextIndexDBTableName()
		nextName := nextIndexDBTableName()
		tableNames = append(tableNames, currName, nextName)
	case 2:
		nextName := nextIndexDBTableName()
		tableNames = append(tableNames, nextName)
	default:
		tableNames = tableNames[len(tableNames)-3:]
	}

	currPath := filepath.Join(path, tableNames[1])

	return openReadOnlyIndexDB(currPath, s)
}

func getMinTimestampForCompositeIndex(metadataDir string, isEmptyDB bool) int64 {
	path := filepath.Join(metadataDir, "minTimestampForCompositeIndex")
	minTimestamp, err := loadMinTimestampForCompositeIndex(path)
	if err == nil {
		return minTimestamp
	}
	if !os.IsNotExist(err) {
		logger.Errorf("cannot read minTimestampForCompositeIndex, so trying to re-create it; error: %s", err)
	}
	date := time.Now().UnixNano() / 1e6 / msecPerDay
	if !isEmptyDB {
		// The current and the next day can already contain non-composite indexes,
		// so they cannot be queried with composite indexes.
		date += 2
	} else {
		date = 0
	}
	minTimestamp = date * msecPerDay
	return minTimestamp
}
