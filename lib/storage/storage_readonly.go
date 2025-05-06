package storage

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/workingsetcache"
)

type ReadOnlyConfig struct {
	Retention          time.Duration
	DisablePerDayIndex bool
	CachePath          string
	StoragePath        string
}

type ReadOnlyStorage struct {
	retentionMsecs     int64
	cachePath          string
	storagePath        string
	disablePerDayIndex bool

	// The minimum timestamp when composite index search can be used.
	minTimestampForCompositeIndex int64

	// tsidCache is MetricName -> TSID cache.
	tsidCache *workingsetcache.Cache

	// metricIDCache is MetricID -> TSID cache.
	metricIDCache *workingsetcache.Cache

	// metricNameCache is MetricID -> MetricName cache.
	metricNameCache *workingsetcache.Cache

	// dateMetricIDCache is (generation, Date, MetricID) cache, where generation is the indexdb generation.
	// See generationTSID for details.
	dateMetricIDCache *dateMetricIDCache

	deletedMetricIDsUpdateLock sync.Mutex
	deletedMetricIDs           atomic.Pointer[uint64set.Set]

	// missingMetricIDs maps metricID to the deadline in unix timestamp seconds
	// after which all the indexdb entries for the given metricID
	// must be deleted if index entry isn't found by the given metricID.
	// This is used inside searchMetricNameWithCache() and getTSIDsFromMetricIDs()
	// for detecting permanently missing metricID->metricName/TSID entries.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5959
	missingMetricIDsLock          sync.Mutex
	missingMetricIDs              map[uint64]uint64
	missingMetricIDsResetDeadline uint64
}

func NewReadOnlyStorage(cfg *ReadOnlyConfig) *ReadOnlyStorage {
	retention := cfg.Retention
	if retention <= 0 || retention > retentionMax {
		retention = retentionMax
	}

	s := &ReadOnlyStorage{
		cachePath:          cfg.CachePath,
		retentionMsecs:     retention.Milliseconds(),
		storagePath:        cfg.StoragePath,
		disablePerDayIndex: cfg.DisablePerDayIndex,
	}

	// Load caches.
	mem := memory.Allowed()
	s.tsidCache = s.mustLoadCache("metricName_tsid", getTSIDCacheSize())
	s.metricIDCache = s.mustLoadCache("metricID_tsid", mem/16)
	s.metricNameCache = s.mustLoadCache("metricID_metricName", mem/10)
	s.dateMetricIDCache = newDateMetricIDCache()

	return s
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

func (s *ReadOnlyStorage) mustLoadCache(name string, sizeBytes int) *workingsetcache.Cache {
	path := filepath.Join(s.cachePath, name)
	return workingsetcache.Load(path, sizeBytes)
}

func (s *ReadOnlyStorage) getDeletedMetricIDs() *uint64set.Set {
	return s.deletedMetricIDs.Load()
}

func (s *ReadOnlyStorage) setDeletedMetricIDs(dmis *uint64set.Set) {
	s.deletedMetricIDs.Store(dmis)
}

func (s *ReadOnlyStorage) mustSaveCache(c *workingsetcache.Cache, name string) {
	saveCacheLock.Lock()
	defer saveCacheLock.Unlock()

	path := filepath.Join(s.cachePath, name)
	if err := c.Save(path); err != nil {
		logger.Panicf("FATAL: cannot save cache to %q: %s", path, err)
	}
}

func (s *ReadOnlyStorage) updateDeletedMetricIDs(metricIDs *uint64set.Set) {
	s.deletedMetricIDsUpdateLock.Lock()
	dmisOld := s.getDeletedMetricIDs()
	dmisNew := dmisOld.Clone()
	dmisNew.Union(metricIDs)
	s.setDeletedMetricIDs(dmisNew)
	s.deletedMetricIDsUpdateLock.Unlock()
}

func (s *ReadOnlyStorage) resetAndSaveTSIDCache() {
	// Reset cache and then store the reset cache on disk in order to prevent
	// from inconsistent behaviour after possible unclean shutdown.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1347
	s.tsidCache.Reset()
	s.mustSaveCache(s.tsidCache, "metricName_tsid")
}

// wasMetricIDMissingBefore checks if passed metricID was already registered as missing before.
// It returns true if metricID was registered as missing for more than 60s.
//
// This function is called when storage can't find TSID for corresponding metricID.
// There are the following expected cases when this may happen:
//  1. The corresponding metricID -> metricName/tsid entry isn't visible for search yet.
//     The solution is to wait for some time and try the search again.
//     It is OK if newly registered time series isn't visible for search during some time.
//     This should resolve https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5959
//  2. The metricID -> metricName/tsid entry doesn't exist in the indexdb.
//     This is possible after unclean shutdown or after restoring of indexdb from a snapshot.
//     In this case the metricID must be deleted, so new metricID is registered
//     again when new sample for the given metric is ingested next time.
func (s *ReadOnlyStorage) wasMetricIDMissingBefore(metricID uint64) bool {
	ct := fasttime.UnixTimestamp()
	s.missingMetricIDsLock.Lock()
	defer s.missingMetricIDsLock.Unlock()

	if ct > s.missingMetricIDsResetDeadline {
		s.missingMetricIDs = nil
		s.missingMetricIDsResetDeadline = ct + 2*60
	}
	deleteDeadline, ok := s.missingMetricIDs[metricID]
	if !ok {
		if s.missingMetricIDs == nil {
			s.missingMetricIDs = make(map[uint64]uint64)
		}
		deleteDeadline = ct + 60
		s.missingMetricIDs[metricID] = deleteDeadline
	}
	return ct > deleteDeadline
}
