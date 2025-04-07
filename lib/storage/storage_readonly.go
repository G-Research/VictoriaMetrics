package storage

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage/metricnamestats"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
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

	tb *readOnlyTable

	// The minimum timestamp when composite index search can be used.
	minTimestampForCompositeIndex int64

	// tsidCache is MetricName -> TSID cache.
	tsidCache *workingsetcache.Cache

	// metricIDCache is MetricID -> TSID cache.
	metricIDCache *workingsetcache.Cache

	// metricNameCache is MetricID -> MetricName cache.
	metricNameCache *workingsetcache.Cache

	// prefetchedMetricIDs contains metricIDs for pre-fetched metricNames in the prefetchMetricNames function.
	prefetchedMetricIDsLock sync.Mutex
	prefetchedMetricIDs     *uint64set.Set

	// prefetchedMetricIDsDeadline is used for periodic reset of prefetchedMetricIDs in order to limit its size under high rate of creating new series.
	prefetchedMetricIDsDeadline atomic.Uint64

	metricsTracker *metricnamestats.Tracker
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

	tablePath := filepath.Join(cfg.StoragePath, dataDirname)
	tb := mustOpenReadOnlyTable(tablePath, s)
	s.tb = tb

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

func (s *ReadOnlyStorage) mustLoadCache(name string, sizeBytes int) *workingsetcache.Cache {
	path := filepath.Join(s.cachePath, name)
	return workingsetcache.Load(path, sizeBytes)
}

func (s *ReadOnlyStorage) prefetchMetricNames(qt *querytracer.Tracer, idb *readOnlyIndexDB, accountID, projectID uint32, srcMetricIDs []uint64, deadline uint64) error {
	qt = qt.NewChild("prefetch metric names for %d metricIDs", len(srcMetricIDs))
	defer qt.Done()

	if len(srcMetricIDs) < 500 {
		qt.Printf("skip pre-fetching metric names for low number of metric ids=%d", len(srcMetricIDs))
		return nil
	}

	var metricIDs []uint64
	s.prefetchedMetricIDsLock.Lock()
	prefetchedMetricIDs := s.prefetchedMetricIDs
	for _, metricID := range srcMetricIDs {
		if prefetchedMetricIDs.Has(metricID) {
			continue
		}
		metricIDs = append(metricIDs, metricID)
	}
	s.prefetchedMetricIDsLock.Unlock()

	qt.Printf("%d out of %d metric names must be pre-fetched", len(metricIDs), len(srcMetricIDs))
	if len(metricIDs) < 500 {
		// It is cheaper to skip pre-fetching and obtain metricNames inline.
		qt.Printf("skip pre-fetching metric names for low number of missing metric ids=%d", len(metricIDs))
		return nil
	}
	// s.slowMetricNameLoads.Add(uint64(len(metricIDs)))

	// Pre-fetch metricIDs.
	var missingMetricIDs []uint64
	var metricName []byte
	var err error
	is := idb.getIndexSearch(accountID, projectID, deadline)
	defer idb.putIndexSearch(is)
	for loops, metricID := range metricIDs {
		if loops&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		var ok bool
		metricName, ok = is.searchMetricNameWithCache(metricName[:0], metricID)
		if !ok {
			missingMetricIDs = append(missingMetricIDs, metricID)
			continue
		}
	}
	idb.doExtDB(func(extDB *readOnlyIndexDB) {
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		defer extDB.putIndexSearch(is)
		for loops, metricID := range missingMetricIDs {
			if loops&paceLimiterSlowIterationsMask == 0 {
				if err = checkSearchDeadlineAndPace(is.deadline); err != nil {
					return
				}
			}
			metricName, _ = is.searchMetricNameWithCache(metricName[:0], metricID)
		}
	})
	if err != nil && err != io.EOF {
		return err
	}
	qt.Printf("pre-fetch metric names for %d metric ids", len(metricIDs))

	// Store the pre-fetched metricIDs, so they aren't pre-fetched next time.
	s.prefetchedMetricIDsLock.Lock()
	if fasttime.UnixTimestamp() > s.prefetchedMetricIDsDeadline.Load() {
		// Periodically reset the prefetchedMetricIDs in order to limit its size.
		s.prefetchedMetricIDs = &uint64set.Set{}
		d := timeutil.AddJitterToDuration(time.Second * 20 * 60)
		metricIDsDeadline := fasttime.UnixTimestamp() + uint64(d.Seconds())
		s.prefetchedMetricIDsDeadline.Store(metricIDsDeadline)
	}
	s.prefetchedMetricIDs.AddMulti(metricIDs)
	s.prefetchedMetricIDsLock.Unlock()

	qt.Printf("cache metric ids for pre-fetched metric names")
	return nil
}

// SearchMetricNames returns marshaled metric names matching the given tfss on
// the given tr.
//
// The marshaled metric names must be unmarshaled via
// MetricName.UnmarshalString().
//
// If -disablePerDayIndex flag is not set, the metric names are searched
// within the given time range (as long as the time range is no more than 40
// days), i.e. the per-day index are used for searching.
//
// If -disablePerDayIndex is set or the time range is more than 40 days, the
// time range is ignored and the metrics are searched within the entire
// retention period, i.e. the global index are used for searching.
func (s *ReadOnlyStorage) SearchMetricNames(qt *querytracer.Tracer, tfss []*TagFilters, tr TimeRange, maxMetrics int, deadline uint64) ([]string, error) {
	tr = s.adjustTimeRange(tr)
	qt = qt.NewChild("search for matching metric names: filters=%s, timeRange=%s", tfss, &tr)
	defer qt.Done()

	idbPath := filepath.Join(s.storagePath, indexdbDirname)
	idb := openReadOnlyCurrentIndexDB(idbPath, s)
	defer idb.MustClose()

	metricIDs, err := idb.searchMetricIDs(qt, tfss, tr, maxMetrics, deadline)
	if err != nil {
		return nil, err
	}
	if len(metricIDs) == 0 || len(tfss) == 0 {
		return nil, nil
	}

	accountID := tfss[0].accountID
	projectID := tfss[0].projectID
	if err = s.prefetchMetricNames(qt, idb, accountID, projectID, metricIDs, deadline); err != nil {
		return nil, err
	}
	metricNames := make([]string, 0, len(metricIDs))
	metricNamesSeen := make(map[string]struct{}, len(metricIDs))
	var metricName []byte
	for i, metricID := range metricIDs {
		if i&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(deadline); err != nil {
				return nil, err
			}
		}
		var ok bool
		metricName, ok = idb.searchMetricName(metricName[:0], metricID, accountID, projectID, false)
		if !ok {
			// Skip missing metricName for metricID.
			// It should be automatically fixed. See indexDB.searchMetricNameWithCache for details.
			continue
		}
		if _, ok := metricNamesSeen[string(metricName)]; ok {
			// The given metric name was already seen; skip it
			continue
		}
		metricNames = append(metricNames, string(metricName))
		metricNamesSeen[metricNames[len(metricNames)-1]] = struct{}{}
	}
	qt.Printf("loaded %d metric names", len(metricNames))
	return metricNames, nil
}

// SearchLabelValues searches for label values for the given labelName, filters
// and tr.
//
// If -disablePerDayIndex flag is not set, the label values are searched
// within the given time range (as long as the time range is no more than 40
// days), i.e. the per-day index are used for searching.
//
// If -disablePerDayIndex is set or the time range is more than 40 days, the
// time range is ignored and the label values are searched within the entire
// retention period, i.e. the global index are used for searching.
func (s *ReadOnlyStorage) SearchLabelValues(qt *querytracer.Tracer, accountID, projectID uint32, labelName string, tfss []*TagFilters, tr TimeRange, maxLabelValues, maxMetrics int, deadline uint64) ([]string, error) {
	idbPath := filepath.Join(s.storagePath, indexdbDirname)
	idb := openReadOnlyCurrentIndexDB(idbPath, s)
	defer idb.MustClose()
	tr = s.adjustTimeRange(tr)

	key := labelName
	if key == "__name__" {
		key = ""
	}
	if len(tfss) == 1 && len(tfss[0].tfs) == 1 && string(tfss[0].tfs[0].key) == key {
		// tfss contains only a single filter on labelName. It is faster searching for label values
		// without any filters and limits and then later applying the filter and the limit to the found label values.
		qt.Printf("search for up to %d values for the label %q on the time range %s", maxMetrics, labelName, &tr)

		lvs, err := idb.SearchLabelValues(qt, accountID, projectID, labelName, nil, tr, maxMetrics, maxMetrics, deadline)
		if err != nil {
			return nil, err
		}
		needSlowSearch := len(lvs) == maxMetrics

		lvsLen := len(lvs)
		lvs = filterLabelValues(accountID, projectID, lvs, &tfss[0].tfs[0], key)
		qt.Printf("found %d out of %d values for the label %q after filtering", len(lvs), lvsLen, labelName)
		if len(lvs) >= maxLabelValues {
			qt.Printf("leave %d out of %d values for the label %q because of the limit", maxLabelValues, len(lvs), labelName)
			lvs = lvs[:maxLabelValues]

			// We found at least maxLabelValues unique values for the label with the given filters.
			// It is OK returning all these values instead of falling back to the slow search.
			needSlowSearch = false
		}
		if !needSlowSearch {
			return lvs, nil
		}
		qt.Printf("fall back to slow search because only a subset of label values is found")
	}

	return idb.SearchLabelValues(qt, accountID, projectID, labelName, tfss, tr, maxLabelValues, maxMetrics, deadline)
}

// SearchTagValueSuffixes returns all the tag value suffixes for the given
// tagKey and tagValuePrefix on the given tr.
//
// This allows implementing
// https://graphite-api.readthedocs.io/en/latest/api.html#metrics-find
// or similar APIs.
//
// If more than maxTagValueSuffixes suffixes is found, then only the first
// maxTagValueSuffixes suffixes is returned.
//
// If -disablePerDayIndex flag is not set, the tag value suffixes are searched
// within the given time range (as long as the time range is no more than 40
// days), i.e. the per-day index are used for searching.
//
// If -disablePerDayIndex is set or the time range is more than 40 days, the
// time range is ignored and the tag value suffixes are searched within the
// entire retention period, i.e. the global index are used for searching.
func (s *ReadOnlyStorage) SearchTagValueSuffixes(qt *querytracer.Tracer, accountID, projectID uint32, tr TimeRange, tagKey, tagValuePrefix string,
	delimiter byte, maxTagValueSuffixes int, deadline uint64,
) ([]string, error) {
	idbPath := filepath.Join(s.storagePath, indexdbDirname)
	idb := openReadOnlyCurrentIndexDB(idbPath, s)
	defer idb.MustClose()
	tr = s.adjustTimeRange(tr)
	return idb.SearchTagValueSuffixes(qt, accountID, projectID, tr, tagKey, tagValuePrefix, delimiter, maxTagValueSuffixes, deadline)
}

// SearchLabelNames searches for label names matching the given tfss on tr.
//
// If -disablePerDayIndex flag is not set, the label names are searched
// within the given time range (as long as the time range is no more than 40
// days), i.e. the per-day index are used for searching.
//
// If -disablePerDayIndex is set or the time range is more than 40 days, the
// time range is ignored and the label names are searched within the entire
// retention period, i.e. the global index are used for searching.
func (s *ReadOnlyStorage) SearchLabelNames(qt *querytracer.Tracer, accountID, projectID uint32, tfss []*TagFilters, tr TimeRange, maxLabelNames, maxMetrics int, deadline uint64) ([]string, error) {
	idbPath := filepath.Join(s.storagePath, indexdbDirname)
	idb := openReadOnlyCurrentIndexDB(idbPath, s)
	defer idb.MustClose()
	tr = s.adjustTimeRange(tr)
	return idb.SearchLabelNames(qt, accountID, projectID, tfss, tr, maxLabelNames, maxMetrics, deadline)
}

// GetSeriesCount returns the approximate number of unique time series for the given (accountID, projectID).
//
// It includes the deleted series too and may count the same series
// up to two times - in db and extDB.
func (s *ReadOnlyStorage) GetSeriesCount(accountID, projectID uint32, deadline uint64) (uint64, error) {
	idbPath := filepath.Join(s.storagePath, indexdbDirname)
	idb := openReadOnlyCurrentIndexDB(idbPath, s)
	defer idb.MustClose()
	return idb.GetSeriesCount(accountID, projectID, deadline)
}

// SearchTenants returns list of registered tenants on the given tr.
func (s *ReadOnlyStorage) SearchTenants(qt *querytracer.Tracer, tr TimeRange, deadline uint64) ([]string, error) {
	idbPath := filepath.Join(s.storagePath, indexdbDirname)
	idb := openReadOnlyCurrentIndexDB(idbPath, s)
	defer idb.MustClose()
	return idb.SearchTenants(qt, tr, deadline)
}

// GetTSDBStatus returns TSDB status data for /api/v1/status/tsdb
//
// If -disablePerDayIndex flag is not set, the status is calculated for the
// given date, i.e. the per-day index are used for calculation.
//
// Otherwise, the date is ignored and the status is calculated for the entire
// retention period, i.e. the global index are used for calculation.
func (s *ReadOnlyStorage) GetTSDBStatus(qt *querytracer.Tracer, accountID, projectID uint32, tfss []*TagFilters, date uint64, focusLabel string, topN, maxMetrics int, deadline uint64) (*TSDBStatus, error) {
	idbPath := filepath.Join(s.storagePath, indexdbDirname)
	idb := openReadOnlyCurrentIndexDB(idbPath, s)
	defer idb.MustClose()
	if s.disablePerDayIndex {
		date = globalIndexDate
	}
	return idb.GetTSDBStatus(qt, accountID, projectID, tfss, date, focusLabel, topN, maxMetrics, deadline)
}

// RegisterMetricNames registers all the metric names from mrs in the indexdb, so they can be queried later.
//
// The the MetricRow.Timestamp is used for registering the metric name at the given day according to the timestamp.
// Th MetricRow.Value field is ignored.
func (s *ReadOnlyStorage) RegisterMetricNames(qt *querytracer.Tracer, mrs []MetricRow) {
	panic("not implemented")
}

// SearchGraphitePaths returns all the matching paths for the given graphite
// query on the given tr.
//
// If -disablePerDayIndex flag is not set, the graphite paths are searched
// within the given time range (as long as the time range is no more than 40
// days), i.e. the per-day index are used for searching.
//
// If -disablePerDayIndex is set or the time range is more than 40 days, the
// time range is ignored and the graphite paths are searched within the entire
// retention period, i.e. global index are used for searching.
func (s *ReadOnlyStorage) SearchGraphitePaths(qt *querytracer.Tracer, accountID, projectID uint32, tr TimeRange, query []byte, maxPaths int, deadline uint64) ([]string, error) {
	idbPath := filepath.Join(s.storagePath, indexdbDirname)
	idb := openReadOnlyCurrentIndexDB(idbPath, s)
	defer idb.MustClose()
	tr = s.adjustTimeRange(tr)
	query = replaceAlternateRegexpsWithGraphiteWildcards(query)
	return s.searchGraphitePaths(qt, idb, accountID, projectID, tr, nil, query, maxPaths, deadline)
}

func (s *ReadOnlyStorage) searchGraphitePaths(qt *querytracer.Tracer, idb *readOnlyIndexDB, accountID, projectID uint32, tr TimeRange, qHead, qTail []byte, maxPaths int, deadline uint64) ([]string, error) {
	n := bytes.IndexAny(qTail, "*[{")
	if n < 0 {
		// Verify that qHead matches a metric name.
		qHead = append(qHead, qTail...)
		suffixes, err := idb.SearchTagValueSuffixes(qt, accountID, projectID, tr, "", bytesutil.ToUnsafeString(qHead), '.', 1, deadline)
		if err != nil {
			return nil, err
		}
		if len(suffixes) == 0 {
			// The query doesn't match anything.
			return nil, nil
		}
		if len(suffixes[0]) > 0 {
			// The query matches a metric name with additional suffix.
			return nil, nil
		}
		return []string{string(qHead)}, nil
	}
	qHead = append(qHead, qTail[:n]...)
	suffixes, err := idb.SearchTagValueSuffixes(qt, accountID, projectID, tr, "", bytesutil.ToUnsafeString(qHead), '.', maxPaths, deadline)
	if err != nil {
		return nil, err
	}
	if len(suffixes) == 0 {
		return nil, nil
	}
	if len(suffixes) >= maxPaths {
		return nil, fmt.Errorf("more than maxPaths=%d suffixes found", maxPaths)
	}
	qNode := qTail[n:]
	qTail = nil
	mustMatchLeafs := true
	if m := bytes.IndexByte(qNode, '.'); m >= 0 {
		qTail = qNode[m+1:]
		qNode = qNode[:m+1]
		mustMatchLeafs = false
	}
	re, err := getRegexpForGraphiteQuery(string(qNode))
	if err != nil {
		return nil, err
	}
	qHeadLen := len(qHead)
	var paths []string
	for _, suffix := range suffixes {
		if len(paths) > maxPaths {
			return nil, fmt.Errorf("more than maxPath=%d paths found", maxPaths)
		}
		if !re.MatchString(suffix) {
			continue
		}
		if mustMatchLeafs {
			qHead = append(qHead[:qHeadLen], suffix...)
			paths = append(paths, string(qHead))
			continue
		}
		qHead = append(qHead[:qHeadLen], suffix...)
		ps, err := s.searchGraphitePaths(qt, idb, accountID, projectID, tr, qHead, qTail, maxPaths, deadline)
		if err != nil {
			return nil, err
		}
		paths = append(paths, ps...)
	}
	return paths, nil
}

// GetMetricNamesStats returns metric names usage stats with give limit and lte predicate
func (s *ReadOnlyStorage) GetMetricNamesStats(_ *querytracer.Tracer, tt *TenantToken, limit, le int, matchPattern string) MetricNamesStatsResponse {
	if tt != nil {
		return s.metricsTracker.GetStatsForTenant(tt.AccountID, tt.ProjectID, limit, le, matchPattern)
	}
	res := s.metricsTracker.GetStats(limit, le, matchPattern)
	res.DeduplicateMergeRecords()
	return res
}

// ResetMetricNamesStats resets state for metric names usage tracker
func (s *ReadOnlyStorage) ResetMetricNamesStats(_ *querytracer.Tracer) {
	s.metricsTracker.Reset(s.tsidCache.Reset)
}
