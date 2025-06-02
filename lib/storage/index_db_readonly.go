package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/panicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/workingsetcache"
)

// readOnlyIndexDB represents an index db.
type readOnlyIndexDB struct {
	// The number of references to indexDBReadOnly struct.
	refCount atomic.Int32

	// if the mustDrop is set to true, then the indexDBReadOnly must be dropped after refCount reaches zero.
	mustDrop atomic.Bool

	// The number of calls for date range searches.
	dateRangeSearchCalls atomic.Uint64

	// The number of hits for date range searches.
	dateRangeSearchHits atomic.Uint64

	// The number of calls for global search.
	globalSearchCalls atomic.Uint64

	// generation identifies the index generation ID
	// and is used for syncing items from different indexDBReadOnlys
	generation uint64

	name string
	tb   *mergeset.ReadOnlyTable

	extDB     *readOnlyIndexDB
	extDBLock sync.Mutex

	// The parent storage.
	s *ReadOnlyStorage

	// Cache for (date, tagFilter) -> loopsCount, which is used for reducing
	// the amount of work when matching a set of filters.
	loopsPerDateTagFilterCache *workingsetcache.Cache

	indexSearchReadOnlyPool sync.Pool
}

func getCurrentIndexDBPath(indexDBpath string) (string, error) {
	// Search for the three most recent tables - the prev, curr and next.
	des := fs.MustReadDir(indexDBpath)
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

	if len(tableNames) < 2 {
		return "", fmt.Errorf("index db doesn't exist yet")
	}
	return filepath.Join(indexDBpath, tableNames[len(tableNames)-2]), nil
}

func openReadOnlyCurrentIndexDB(path string, s *ReadOnlyStorage) (*readOnlyIndexDB, error) {
	// if s == nil {
	// 	logger.Panicf("BUG: Storage must be non-nil")
	// }

	var indexDBPath string
	for range 5 {
		var err error
		indexDBPath, err = getCurrentIndexDBPath(path)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
		continue
	}

	name := filepath.Base(indexDBPath)
	gen, err := strconv.ParseUint(name, 16, 64)
	if err != nil {
		logger.Panicf("FATAL: cannot parse indexdb path %q: %s", indexDBPath, err)
	}

	s.tb = mustOpenReadOnlyTable(filepath.Join(s.storagePath, dataDirname), s)

	// Do not persist tagFiltersToMetricIDsCache in files, since it is very volatile because of tagFiltersKeyGen.
	mem := memory.Allowed()

	var tb *mergeset.ReadOnlyTable
	if err := panicutil.ToError(func() error {
		tb = mergeset.MustOpenTableReadOnly(indexDBPath)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cannot open index db at %q: %w", indexDBPath, err)
	}

	db := &readOnlyIndexDB{
		generation: gen,
		name:       name,

		tb:                         tb,
		s:                          s,
		loopsPerDateTagFilterCache: workingsetcache.New(mem / 128),
	}
	db.incRef()
	return db, nil
}

// doExtDB calls f for non-nil db.extDB.
//
// f isn't called if db.extDB is nil.
func (db *readOnlyIndexDB) doExtDB(f func(extDB *readOnlyIndexDB)) {
	db.extDBLock.Lock()
	extDB := db.extDB
	if extDB != nil {
		extDB.incRef()
	}
	db.extDBLock.Unlock()
	if extDB != nil {
		f(extDB)
		extDB.decRef()
	}
}

// hasExtDB returns true if db.extDB != nil
func (db *readOnlyIndexDB) hasExtDB() bool {
	db.extDBLock.Lock()
	ok := db.extDB != nil
	db.extDBLock.Unlock()
	return ok
}

// SetExtDB sets external db to search.
//
// It decrements refCount for the previous extDB.
func (db *readOnlyIndexDB) SetExtDB(extDB *readOnlyIndexDB) {
	db.extDBLock.Lock()
	prevExtDB := db.extDB
	db.extDB = extDB
	db.extDBLock.Unlock()

	if prevExtDB != nil {
		prevExtDB.decRef()
	}
}

// MustClose closes db.
func (db *readOnlyIndexDB) MustClose() {
	db.decRef()
}

func (db *readOnlyIndexDB) incRef() {
	db.refCount.Add(1)
}

func (db *readOnlyIndexDB) decRef() {
	n := db.refCount.Add(-1)
	if n < 0 {
		logger.Panicf("BUG: %q negative refCount: %d", db.name, n)
	}
	if n > 0 {
		return
	}

	db.tb = nil
	db.SetExtDB(nil)

	// Free space occupied by caches owned by db.
	db.loopsPerDateTagFilterCache.Stop()

	db.s = nil
	db.loopsPerDateTagFilterCache = nil

	if !db.mustDrop.Load() {
		return
	}
}

func (db *readOnlyIndexDB) getFromMetricIDCache(dst *TSID, metricID uint64) error {
	// There is no need in prefixing the key with (accountID, projectID),
	// since metricID is globally unique across all (accountID, projectID) values.
	// See getUniqueUint64.

	// There is no need in checking for deleted metricIDs here, since they
	// must be checked by the caller.
	buf := (*[unsafe.Sizeof(*dst)]byte)(unsafe.Pointer(dst))
	key := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	tmp := db.s.metricIDCache.Get(buf[:0], key[:])
	if len(tmp) == 0 {
		// The TSID for the given metricID wasn't found in the cache.
		return io.EOF
	}
	if &tmp[0] != &buf[0] || len(tmp) != len(buf) {
		return fmt.Errorf("corrupted MetricID->TSID cache: unexpected size for metricID=%d value; got %d bytes; want %d bytes", metricID, len(tmp), len(buf))
	}
	return nil
}

func (db *readOnlyIndexDB) putToMetricIDCache(metricID uint64, tsid *TSID) {
	buf := (*[unsafe.Sizeof(*tsid)]byte)(unsafe.Pointer(tsid))
	key := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	db.s.metricIDCache.Set(key[:], buf[:])
}

func (db *readOnlyIndexDB) getMetricNameFromCache(dst []byte, metricID uint64) []byte {
	// There is no need in prefixing the key with (accountID, projectID),
	// since metricID is globally unique across all (accountID, projectID) values.
	// See getUniqueUint64.

	// There is no need in checking for deleted metricIDs here, since they
	// must be checked by the caller.
	key := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	return db.s.metricNameCache.Get(dst, key[:])
}

func (db *readOnlyIndexDB) putMetricNameToCache(metricID uint64, metricName []byte) {
	key := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	db.s.metricNameCache.Set(key[:], metricName)
}

type indexSearchReadOnly struct {
	db *readOnlyIndexDB
	ts mergeset.ReadOnlyTableSearch
	kb bytesutil.ByteBuffer
	mp tagToMetricIDsRowParser

	accountID uint32
	projectID uint32

	// deadline in unix timestamp seconds for the given search.
	deadline uint64
}

// getIndexSearch returns an indexSearchReadOnly with default configuration
func (db *readOnlyIndexDB) getIndexSearch(accountID, projectID uint32, deadline uint64) *indexSearchReadOnly {
	return db.getIndexSearchInternal(accountID, projectID, deadline, false)
}

func (db *readOnlyIndexDB) getIndexSearchInternal(accountID, projectID uint32, deadline uint64, sparse bool) *indexSearchReadOnly {
	v := db.indexSearchReadOnlyPool.Get()
	if v == nil {
		v = &indexSearchReadOnly{
			db: db,
		}
	}
	is := v.(*indexSearchReadOnly)
	is.accountID = accountID
	is.projectID = projectID
	is.ts.Init(db.tb, sparse)
	is.deadline = deadline
	return is
}

func (db *readOnlyIndexDB) putIndexSearch(is *indexSearchReadOnly) {
	is.kb.Reset()
	is.mp.Reset()
	is.accountID = 0
	is.projectID = 0
	is.deadline = 0

	db.indexSearchReadOnlyPool.Put(is)
}

// SearchLabelNames returns all the label names, which match the given tfss on
// the given tr.
func (db *readOnlyIndexDB) SearchLabelNames(qt *querytracer.Tracer, accountID, projectID uint32, tfss []*TagFilters, tr TimeRange,
	maxLabelNames, maxMetrics int, deadline uint64,
) ([]string, error) {
	qt = qt.NewChild("search for label names: filters=%s, timeRange=%s, maxLabelNames=%d, maxMetrics=%d", tfss, &tr, maxLabelNames, maxMetrics)
	defer qt.Done()

	lns := make(map[string]struct{})
	qtChild := qt.NewChild("search for label names in the current indexdb")
	is := db.getIndexSearch(accountID, projectID, deadline)
	err := is.searchLabelNamesWithFiltersOnTimeRange(qtChild, lns, tfss, tr, maxLabelNames, maxMetrics)
	db.putIndexSearch(is)
	qtChild.Donef("found %d label names", len(lns))
	if err != nil {
		return nil, err
	}

	db.doExtDB(func(extDB *readOnlyIndexDB) {
		qtChild := qt.NewChild("search for label names in the previous indexdb")
		lnsLen := len(lns)
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		err = is.searchLabelNamesWithFiltersOnTimeRange(qtChild, lns, tfss, tr, maxLabelNames, maxMetrics)
		extDB.putIndexSearch(is)
		qtChild.Donef("found %d additional label names", len(lns)-lnsLen)
	})
	if err != nil {
		return nil, err
	}

	labelNames := make([]string, 0, len(lns))
	for labelName := range lns {
		labelNames = append(labelNames, labelName)
	}
	// Do not sort label names, since they must be sorted by vmselect.
	qt.Printf("found %d label names in the current and the previous indexdb", len(labelNames))
	return labelNames, nil
}

func (is *indexSearchReadOnly) searchLabelNamesWithFiltersOnTimeRange(qt *querytracer.Tracer, lns map[string]struct{}, tfss []*TagFilters, tr TimeRange, maxLabelNames, maxMetrics int) error {
	if tr == globalIndexTimeRange {
		qtChild := qt.NewChild("search for label names in global index: filters=%s", tfss)
		err := is.searchLabelNamesWithFiltersOnDate(qtChild, lns, tfss, globalIndexDate, maxLabelNames, maxMetrics)
		qtChild.Done()
		return err
	}

	minDate, maxDate := tr.DateRange()
	var mu sync.Mutex
	wg := getWaitGroup()
	var errGlobal error
	qt = qt.NewChild("parallel search for label names: filters=%s, timeRange=%s", tfss, &tr)
	for date := minDate; date <= maxDate; date++ {
		wg.Add(1)
		qtChild := qt.NewChild("search for label names: filters=%s, date=%s", tfss, dateToString(date))
		go func(date uint64) {
			defer func() {
				qtChild.Done()
				wg.Done()
			}()
			lnsLocal := make(map[string]struct{})
			isLocal := is.db.getIndexSearch(is.accountID, is.projectID, is.deadline)
			err := isLocal.searchLabelNamesWithFiltersOnDate(qtChild, lnsLocal, tfss, date, maxLabelNames, maxMetrics)
			is.db.putIndexSearch(isLocal)
			mu.Lock()
			defer mu.Unlock()
			if errGlobal != nil {
				return
			}
			if err != nil {
				errGlobal = err
				return
			}
			if len(lns) >= maxLabelNames {
				return
			}
			for k := range lnsLocal {
				lns[k] = struct{}{}
			}
		}(date)
	}
	wg.Wait()
	putWaitGroup(wg)
	qt.Done()
	return errGlobal
}

func (is *indexSearchReadOnly) searchLabelNamesWithFiltersOnDate(qt *querytracer.Tracer, lns map[string]struct{}, tfss []*TagFilters, date uint64, maxLabelNames, maxMetrics int) error {
	filter, err := is.searchMetricIDsWithFiltersOnDate(qt, tfss, date, maxMetrics)
	if err != nil {
		return err
	}
	if filter != nil && filter.Len() <= 100e3 {
		// It is faster to obtain label names by metricIDs from the filter
		// instead of scanning the inverted index for the matching filters.
		// This should help https://github.com/VictoriaMetrics/VictoriaMetrics/issues/2978
		metricIDs := filter.AppendTo(nil)
		qt.Printf("sort %d metricIDs", len(metricIDs))
		is.getLabelNamesForMetricIDs(qt, metricIDs, lns, maxLabelNames)
		return nil
	}

	var prevLabelName []byte
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	loopsPaceLimiter := 0
	underscoreNameSeen := false
	nsPrefixExpected := byte(nsPrefixDateTagToMetricIDs)
	if date == globalIndexDate {
		nsPrefixExpected = nsPrefixTagToMetricIDs
	}

	hasCompositeLabelName := false
	kb.B = is.marshalCommonPrefixForDate(kb.B[:0], date)
	if name := getCommonMetricNameForTagFilterss(tfss); len(name) > 0 {
		compositeLabelName := marshalCompositeTagKey(nil, name, nil)
		kb.B = marshalTagValue(kb.B, compositeLabelName)
		// Drop trailing tagSeparator
		kb.B = kb.B[:len(kb.B)-1]
		hasCompositeLabelName = true
	}
	prefix := append([]byte{}, kb.B...)

	ts.Seek(prefix)
	for len(lns) < maxLabelNames && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefixExpected); err != nil {
			return err
		}
		labelName := mp.Tag.Key
		if len(labelName) == 0 || hasCompositeLabelName {
			underscoreNameSeen = true
		}
		if (!hasCompositeLabelName && isArtificialTagKey(labelName)) || string(labelName) == string(prevLabelName) {
			// Search for the next tag key.
			// The last char in kb.B must be tagSeparatorChar.
			// Just increment it in order to jump to the next tag key.
			kb.B = is.marshalCommonPrefixForDate(kb.B[:0], date)
			if !hasCompositeLabelName && len(labelName) > 0 && labelName[0] == compositeTagKeyPrefix {
				// skip composite tag entries
				kb.B = append(kb.B, compositeTagKeyPrefix)
			} else {
				kb.B = marshalTagValue(kb.B, labelName)
			}
			kb.B[len(kb.B)-1]++
			ts.Seek(kb.B)
			continue
		}
		if !hasCompositeLabelName {
			lns[string(labelName)] = struct{}{}
		} else {
			_, key, err := unmarshalCompositeTagKey(labelName)
			if err != nil {
				return fmt.Errorf("cannot unmarshal composite tag key: %s", err)
			}
			lns[string(key)] = struct{}{}
		}
		prevLabelName = append(prevLabelName[:0], labelName...)
	}
	if underscoreNameSeen {
		lns["__name__"] = struct{}{}
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error during search for prefix %q: %w", prefix, err)
	}
	return nil
}

func (is *indexSearchReadOnly) getLabelNamesForMetricIDs(qt *querytracer.Tracer, metricIDs []uint64, lns map[string]struct{}, maxLabelNames int) {
	if len(metricIDs) > 0 {
		lns["__name__"] = struct{}{}
	}

	var mn MetricName
	foundLabelNames := 0
	var buf []byte
	for _, metricID := range metricIDs {
		var ok bool
		buf, ok = is.searchMetricNameWithCache(buf[:0], metricID)
		if !ok {
			// It is likely the metricID->metricName entry didn't propagate to inverted index yet.
			// Skip this metricID for now.
			continue
		}
		if err := mn.Unmarshal(buf); err != nil {
			logger.Panicf("FATAL: cannot unmarshal metricName %q: %s", buf, err)
		}
		for _, tag := range mn.Tags {
			if _, ok := lns[string(tag.Key)]; !ok {
				foundLabelNames++
				lns[string(tag.Key)] = struct{}{}
				if len(lns) >= maxLabelNames {
					qt.Printf("hit the limit on the number of unique label names: %d", maxLabelNames)
					return
				}
			}
		}
	}
	qt.Printf("get %d distinct label names from %d metricIDs", foundLabelNames, len(metricIDs))
}

// SearchTenants returns all tenants on the given tr.
func (db *readOnlyIndexDB) SearchTenants(qt *querytracer.Tracer, tr TimeRange, deadline uint64) ([]string, error) {
	qt = qt.NewChild("search for tenants on timeRange=%s", &tr)
	defer qt.Done()
	tenants := make(map[string]struct{})
	qtChild := qt.NewChild("search for tenants in the current indexdb")
	is := db.getIndexSearch(0, 0, deadline)
	err := is.searchTenantsOnTimeRange(qtChild, tenants, tr)
	db.putIndexSearch(is)
	qtChild.Donef("found %d tenants", len(tenants))
	if err != nil {
		return nil, err
	}
	db.doExtDB(func(extDB *readOnlyIndexDB) {
		qtChild := qt.NewChild("search for tenants in the previous indexdb")
		tenantsLen := len(tenants)
		is := extDB.getIndexSearch(0, 0, deadline)
		err = is.searchTenantsOnTimeRange(qtChild, tenants, tr)
		extDB.putIndexSearch(is)
		qtChild.Donef("found %d additional tenants", len(tenants)-tenantsLen)
	})
	if err != nil {
		return nil, err
	}

	tenantsList := make([]string, 0, len(tenants))
	for tenant := range tenants {
		tenantsList = append(tenantsList, tenant)
	}
	// Do not sort tenants, since they must be sorted by vmselect.
	qt.Printf("found %d tenants in the current and the previous indexdb", len(tenantsList))
	return tenantsList, nil
}

func (is *indexSearchReadOnly) searchTenantsOnTimeRange(qt *querytracer.Tracer, tenants map[string]struct{}, tr TimeRange) error {
	minDate := uint64(tr.MinTimestamp) / msecPerDay
	maxDate := uint64(tr.MaxTimestamp-1) / msecPerDay
	if maxDate == 0 || minDate > maxDate || maxDate-minDate > maxDaysForPerDaySearch {
		qtChild := qt.NewChild("search for tenants in global index")
		err := is.searchTenantsOnDate(tenants, 0)
		qtChild.Done()
		return err
	}
	var mu sync.Mutex
	wg := getWaitGroup()
	var errGlobal error
	qt = qt.NewChild("parallel search for tenants on timeRange=%s", &tr)
	for date := minDate; date <= maxDate; date++ {
		wg.Add(1)
		qtChild := qt.NewChild("search for tenants on date=%s", dateToString(date))
		go func(date uint64) {
			defer func() {
				qtChild.Done()
				wg.Done()
			}()
			tenantsLocal := make(map[string]struct{})
			isLocal := is.db.getIndexSearch(0, 0, is.deadline)
			err := isLocal.searchTenantsOnDate(tenantsLocal, date)
			is.db.putIndexSearch(isLocal)
			mu.Lock()
			defer mu.Unlock()
			if errGlobal != nil {
				return
			}
			if err != nil {
				errGlobal = err
				return
			}
			for tenant := range tenantsLocal {
				tenants[tenant] = struct{}{}
			}
		}(date)
	}
	wg.Wait()
	putWaitGroup(wg)
	qt.Done()
	return errGlobal
}

func (is *indexSearchReadOnly) searchTenantsOnDate(tenants map[string]struct{}, date uint64) error {
	loopsPaceLimiter := 0
	ts := &is.ts
	kb := &is.kb
	is.accountID = 0
	is.projectID = 0
	kb.B = is.marshalCommonPrefixForDate(kb.B[:0], date)
	_, prefixNeeded, _, _, err := unmarshalCommonPrefix(kb.B)
	if err != nil {
		logger.Panicf("BUG: cannot unmarshal common prefix from %q: %s", kb.B, err)
	}
	ts.Seek(kb.B)
	for ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		_, prefix, accountID, projectID, err := unmarshalCommonPrefix(ts.Item)
		if err != nil {
			return err
		}
		if prefix != prefixNeeded {
			// Reached the end of enteris with the needed prefix.
			break
		}
		tenant := fmt.Sprintf("%d:%d", accountID, projectID)
		tenants[tenant] = struct{}{}
		// Seek for the next (accountID, projectID)
		projectID++
		if projectID == 0 {
			accountID++
			if accountID == 0 {
				// Reached the end (accountID, projectID) space
				break
			}
		}
		is.accountID = accountID
		is.projectID = projectID
		kb.B = is.marshalCommonPrefixForDate(kb.B[:0], date)
		ts.Seek(kb.B)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error during search for prefix %q: %w", kb.B, err)
	}
	return nil
}

// SearchLabelValues returns label values for the given labelName, tfss and tr.
func (db *readOnlyIndexDB) SearchLabelValues(qt *querytracer.Tracer, accountID, projectID uint32, labelName string, tfss []*TagFilters, tr TimeRange,
	maxLabelValues, maxMetrics int, deadline uint64,
) ([]string, error) {
	qt = qt.NewChild("search for label values: labelName=%q, filters=%s, timeRange=%s, maxLabelNames=%d, maxMetrics=%d", labelName, tfss, &tr, maxLabelValues, maxMetrics)
	defer qt.Done()

	lvs := make(map[string]struct{})
	qtChild := qt.NewChild("search for label values in the current indexdb")
	is := db.getIndexSearch(accountID, projectID, deadline)
	err := is.searchLabelValuesWithFiltersOnTimeRange(qtChild, lvs, labelName, tfss, tr, maxLabelValues, maxMetrics)
	db.putIndexSearch(is)
	qtChild.Donef("found %d label values", len(lvs))
	if err != nil {
		return nil, err
	}
	db.doExtDB(func(extDB *readOnlyIndexDB) {
		qtChild := qt.NewChild("search for label values in the previous indexdb")
		lvsLen := len(lvs)
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		err = is.searchLabelValuesWithFiltersOnTimeRange(qtChild, lvs, labelName, tfss, tr, maxLabelValues, maxMetrics)
		extDB.putIndexSearch(is)
		qtChild.Donef("found %d additional label values", len(lvs)-lvsLen)
	})
	if err != nil {
		return nil, err
	}

	labelValues := make([]string, 0, len(lvs))
	for labelValue := range lvs {
		if len(labelValue) == 0 {
			// Skip empty values, since they have no any meaning.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/600
			continue
		}
		labelValues = append(labelValues, labelValue)
	}
	// Do not sort labelValues, since they must be sorted by vmselect.
	qt.Printf("found %d label values in the current and the previous indexdb", len(labelValues))
	return labelValues, nil
}

func (is *indexSearchReadOnly) searchLabelValuesWithFiltersOnTimeRange(qt *querytracer.Tracer, lvs map[string]struct{}, labelName string, tfss []*TagFilters,
	tr TimeRange, maxLabelValues, maxMetrics int,
) error {
	if tr == globalIndexTimeRange {
		qtChild := qt.NewChild("search for label values in global index: labelName=%q, filters=%s", labelName, tfss)
		err := is.searchLabelValuesWithFiltersOnDate(qtChild, lvs, labelName, tfss, globalIndexDate, maxLabelValues, maxMetrics)
		qtChild.Done()
		return err
	}

	minDate, maxDate := tr.DateRange()
	var mu sync.Mutex
	wg := getWaitGroup()
	var errGlobal error
	qt = qt.NewChild("parallel search for label values: labelName=%q, filters=%s, timeRange=%s", labelName, tfss, &tr)
	for date := minDate; date <= maxDate; date++ {
		wg.Add(1)
		qtChild := qt.NewChild("search for label values: filters=%s, date=%s", tfss, dateToString(date))
		go func(date uint64) {
			defer func() {
				qtChild.Done()
				wg.Done()
			}()
			lvsLocal := make(map[string]struct{})
			isLocal := is.db.getIndexSearch(is.accountID, is.projectID, is.deadline)
			err := isLocal.searchLabelValuesWithFiltersOnDate(qtChild, lvsLocal, labelName, tfss, date, maxLabelValues, maxMetrics)
			is.db.putIndexSearch(isLocal)
			mu.Lock()
			defer mu.Unlock()
			if errGlobal != nil {
				return
			}
			if err != nil {
				errGlobal = err
				return
			}
			if len(lvs) >= maxLabelValues {
				return
			}
			for v := range lvsLocal {
				lvs[v] = struct{}{}
			}
		}(date)
	}
	wg.Wait()
	putWaitGroup(wg)
	qt.Done()
	return errGlobal
}

func (is *indexSearchReadOnly) searchLabelValuesWithFiltersOnDate(qt *querytracer.Tracer, lvs map[string]struct{}, labelName string, tfss []*TagFilters,
	date uint64, maxLabelValues, maxMetrics int,
) error {
	filter, err := is.searchMetricIDsWithFiltersOnDate(qt, tfss, date, maxMetrics)
	if err != nil {
		return err
	}
	if filter != nil && filter.Len() <= 100e3 {
		// It is faster to obtain label values by metricIDs from the filter
		// instead of scanning the inverted index for the matching filters.
		// This should help https://github.com/VictoriaMetrics/VictoriaMetrics/issues/2978
		metricIDs := filter.AppendTo(nil)
		qt.Printf("sort %d metricIDs", len(metricIDs))
		is.getLabelValuesForMetricIDs(qt, lvs, labelName, metricIDs, maxLabelValues)
		return nil
	}
	if labelName == "__name__" {
		// __name__ label is encoded as empty string in indexdb.
		labelName = ""
	}

	labelNameBytes := bytesutil.ToUnsafeBytes(labelName)
	if name := getCommonMetricNameForTagFilterss(tfss); len(name) > 0 && labelName != "" {
		labelNameBytes = marshalCompositeTagKey(nil, name, labelNameBytes)
	}

	var prevLabelValue []byte
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	loopsPaceLimiter := 0
	nsPrefixExpected := byte(nsPrefixDateTagToMetricIDs)
	if date == globalIndexDate {
		nsPrefixExpected = nsPrefixTagToMetricIDs
	}
	kb.B = is.marshalCommonPrefixForDate(kb.B[:0], date)
	kb.B = marshalTagValue(kb.B, labelNameBytes)
	prefix := append([]byte{}, kb.B...)
	ts.Seek(prefix)
	for len(lvs) < maxLabelValues && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefixExpected); err != nil {
			return err
		}
		labelValue := mp.Tag.Value
		if string(labelValue) == string(prevLabelValue) {
			// Search for the next tag value.
			// The last char in kb.B must be tagSeparatorChar.
			// Just increment it in order to jump to the next tag value.
			kb.B = is.marshalCommonPrefixForDate(kb.B[:0], date)
			kb.B = marshalTagValue(kb.B, labelNameBytes)
			kb.B = marshalTagValue(kb.B, labelValue)
			kb.B[len(kb.B)-1]++
			ts.Seek(kb.B)
			continue
		}
		lvs[string(labelValue)] = struct{}{}
		prevLabelValue = append(prevLabelValue[:0], labelValue...)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for tag name prefix %q: %w", prefix, err)
	}
	return nil
}

func (is *indexSearchReadOnly) getLabelValuesForMetricIDs(qt *querytracer.Tracer, lvs map[string]struct{}, labelName string, metricIDs []uint64, maxLabelValues int) {
	if labelName == "" {
		labelName = "__name__"
	}

	var mn MetricName
	foundLabelValues := 0
	var buf []byte
	for _, metricID := range metricIDs {
		var ok bool
		buf, ok = is.searchMetricNameWithCache(buf[:0], metricID)
		if !ok {
			// It is likely the metricID->metricName entry didn't propagate to inverted index yet.
			// Skip this metricID for now.
			continue
		}
		if err := mn.Unmarshal(buf); err != nil {
			logger.Panicf("FATAL: cannot unmarshal metricName %q: %s", buf, err)
		}
		tagValue := mn.GetTagValue(labelName)
		if _, ok := lvs[string(tagValue)]; !ok {
			foundLabelValues++
			lvs[string(tagValue)] = struct{}{}
			if len(lvs) >= maxLabelValues {
				qt.Printf("hit the limit on the number of unique label values for label %q: %d", labelName, maxLabelValues)
				return
			}
		}
	}
	qt.Printf("get %d distinct values for label %q from %d metricIDs", foundLabelValues, labelName, len(metricIDs))
}

// SearchTagValueSuffixes returns all the tag value suffixes for the given tagKey and tagValuePrefix on the given tr.
//
// This allows implementing https://graphite-api.readthedocs.io/en/latest/api.html#metrics-find or similar APIs.
//
// If it returns maxTagValueSuffixes suffixes, then it is likely more than maxTagValueSuffixes suffixes is found.
func (db *readOnlyIndexDB) SearchTagValueSuffixes(qt *querytracer.Tracer, accountID, projectID uint32, tr TimeRange, tagKey, tagValuePrefix string,
	delimiter byte, maxTagValueSuffixes int, deadline uint64,
) ([]string, error) {
	qt = qt.NewChild("search tag value suffixes for accountID=%d, projectID=%d, timeRange=%s, tagKey=%q, tagValuePrefix=%q, delimiter=%c, maxTagValueSuffixes=%d",
		accountID, projectID, &tr, tagKey, tagValuePrefix, delimiter, maxTagValueSuffixes)
	defer qt.Done()

	// TODO: cache results?

	tvss := make(map[string]struct{})
	is := db.getIndexSearch(accountID, projectID, deadline)
	err := is.searchTagValueSuffixesForTimeRange(tvss, tr, tagKey, tagValuePrefix, delimiter, maxTagValueSuffixes)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}
	if len(tvss) < maxTagValueSuffixes {
		db.doExtDB(func(extDB *readOnlyIndexDB) {
			is := extDB.getIndexSearch(accountID, projectID, deadline)
			qtChild := qt.NewChild("search tag value suffixes in the previous indexdb")
			err = is.searchTagValueSuffixesForTimeRange(tvss, tr, tagKey, tagValuePrefix, delimiter, maxTagValueSuffixes)
			qtChild.Done()
			extDB.putIndexSearch(is)
		})
		if err != nil {
			return nil, err
		}
	}

	suffixes := make([]string, 0, len(tvss))
	for suffix := range tvss {
		// Do not skip empty suffixes, since they may represent leaf tag values.
		suffixes = append(suffixes, suffix)
	}
	if len(suffixes) > maxTagValueSuffixes {
		suffixes = suffixes[:maxTagValueSuffixes]
	}
	// Do not sort suffixes, since they must be sorted by vmselect.
	qt.Printf("found %d suffixes", len(suffixes))
	return suffixes, nil
}

func (is *indexSearchReadOnly) searchTagValueSuffixesForTimeRange(tvss map[string]struct{}, tr TimeRange, tagKey, tagValuePrefix string, delimiter byte, maxTagValueSuffixes int) error {
	if tr == globalIndexTimeRange {
		return is.searchTagValueSuffixesAll(tvss, tagKey, tagValuePrefix, delimiter, maxTagValueSuffixes)
	}

	minDate, maxDate := tr.DateRange()
	// Query over multiple days in parallel.
	wg := getWaitGroup()
	var errGlobal error
	var mu sync.Mutex // protects tvss + errGlobal from concurrent access below.
	for minDate <= maxDate {
		wg.Add(1)
		go func(date uint64) {
			defer wg.Done()
			tvssLocal := make(map[string]struct{})
			isLocal := is.db.getIndexSearch(is.accountID, is.projectID, is.deadline)
			err := isLocal.searchTagValueSuffixesForDate(tvssLocal, date, tagKey, tagValuePrefix, delimiter, maxTagValueSuffixes)
			is.db.putIndexSearch(isLocal)
			mu.Lock()
			defer mu.Unlock()
			if errGlobal != nil {
				return
			}
			if err != nil {
				errGlobal = err
				return
			}
			if len(tvss) > maxTagValueSuffixes {
				return
			}
			for k := range tvssLocal {
				tvss[k] = struct{}{}
			}
		}(minDate)
		minDate++
	}
	wg.Wait()
	putWaitGroup(wg)
	return errGlobal
}

func (is *indexSearchReadOnly) searchTagValueSuffixesAll(tvss map[string]struct{}, tagKey, tagValuePrefix string, delimiter byte, maxTagValueSuffixes int) error {
	kb := &is.kb
	nsPrefix := byte(nsPrefixTagToMetricIDs)
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefix)
	kb.B = marshalTagValue(kb.B, bytesutil.ToUnsafeBytes(tagKey))
	kb.B = marshalTagValue(kb.B, bytesutil.ToUnsafeBytes(tagValuePrefix))
	kb.B = kb.B[:len(kb.B)-1] // remove tagSeparatorChar from the end of kb.B
	prefix := append([]byte(nil), kb.B...)
	return is.searchTagValueSuffixesForPrefix(tvss, nsPrefix, prefix, len(tagValuePrefix), delimiter, maxTagValueSuffixes)
}

func (is *indexSearchReadOnly) searchTagValueSuffixesForDate(tvss map[string]struct{}, date uint64, tagKey, tagValuePrefix string, delimiter byte, maxTagValueSuffixes int) error {
	nsPrefix := byte(nsPrefixDateTagToMetricIDs)
	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefix)
	kb.B = encoding.MarshalUint64(kb.B, date)
	kb.B = marshalTagValue(kb.B, bytesutil.ToUnsafeBytes(tagKey))
	kb.B = marshalTagValue(kb.B, bytesutil.ToUnsafeBytes(tagValuePrefix))
	kb.B = kb.B[:len(kb.B)-1] // remove tagSeparatorChar from the end of kb.B
	prefix := append([]byte(nil), kb.B...)
	return is.searchTagValueSuffixesForPrefix(tvss, nsPrefix, prefix, len(tagValuePrefix), delimiter, maxTagValueSuffixes)
}

func (is *indexSearchReadOnly) searchTagValueSuffixesForPrefix(tvss map[string]struct{}, nsPrefix byte, prefix []byte, tagValuePrefixLen int, delimiter byte, maxTagValueSuffixes int) error {
	kb := &is.kb
	ts := &is.ts
	mp := &is.mp
	loopsPaceLimiter := 0
	ts.Seek(prefix)
	for len(tvss) < maxTagValueSuffixes && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefix); err != nil {
			return err
		}
		tagValue := mp.Tag.Value
		suffix := tagValue[tagValuePrefixLen:]
		n := bytes.IndexByte(suffix, delimiter)
		if n < 0 {
			// Found leaf tag value that doesn't have delimiters after the given tagValuePrefix.
			tvss[string(suffix)] = struct{}{}
			continue
		}
		// Found non-leaf tag value. Extract suffix that end with the given delimiter.
		suffix = suffix[:n+1]
		tvss[string(suffix)] = struct{}{}
		if suffix[len(suffix)-1] == 255 {
			continue
		}
		// Search for the next suffix
		suffix[len(suffix)-1]++
		kb.B = append(kb.B[:0], prefix...)
		kb.B = marshalTagValue(kb.B, suffix)
		kb.B = kb.B[:len(kb.B)-1] // remove tagSeparatorChar
		ts.Seek(kb.B)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for tag value suffixes for prefix %q: %w", prefix, err)
	}
	return nil
}

// GetSeriesCount returns the approximate number of unique timeseries for the given (accountID, projectID).
//
// It includes the deleted series too and may count the same series
// up to two times - in db and extDB.
func (db *readOnlyIndexDB) GetSeriesCount(accountID, projectID uint32, deadline uint64) (uint64, error) {
	is := db.getIndexSearch(accountID, projectID, deadline)
	n, err := is.getSeriesCount()
	db.putIndexSearch(is)
	if err != nil {
		return 0, err
	}

	var nExt uint64
	db.doExtDB(func(extDB *readOnlyIndexDB) {
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		nExt, err = is.getSeriesCount()
		extDB.putIndexSearch(is)
	})
	if err != nil {
		return 0, fmt.Errorf("error when searching in extDB: %w", err)
	}
	return n + nExt, nil
}

func (is *indexSearchReadOnly) getSeriesCount() (uint64, error) {
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	loopsPaceLimiter := 0
	var metricIDsLen uint64
	// Extract the number of series from ((__name__=value): metricIDs) rows
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs)
	kb.B = marshalTagValue(kb.B, nil)
	ts.Seek(kb.B)
	for ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return 0, err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, kb.B) {
			break
		}
		tail := item[len(kb.B):]
		n := bytes.IndexByte(tail, tagSeparatorChar)
		if n < 0 {
			return 0, fmt.Errorf("invalid tag->metricIDs line %q: cannot find tagSeparatorChar %d", item, tagSeparatorChar)
		}
		tail = tail[n+1:]
		if err := mp.InitOnlyTail(item, tail); err != nil {
			return 0, err
		}
		// Take into account deleted timeseries too.
		// It is OK if series can be counted multiple times in rare cases -
		// the returned number is an estimation.
		metricIDsLen += uint64(mp.MetricIDsLen())
	}
	if err := ts.Error(); err != nil {
		return 0, fmt.Errorf("error when counting unique timeseries: %w", err)
	}
	return metricIDsLen, nil
}

// GetTSDBStatus returns topN entries for tsdb status for the given tfss, date and focusLabel.
func (db *readOnlyIndexDB) GetTSDBStatus(qt *querytracer.Tracer, accountID, projectID uint32, tfss []*TagFilters, date uint64, focusLabel string, topN, maxMetrics int, deadline uint64) (*TSDBStatus, error) {
	qtChild := qt.NewChild("collect tsdb stats in the current indexdb")

	is := db.getIndexSearch(accountID, projectID, deadline)
	status, err := is.getTSDBStatus(qtChild, tfss, date, focusLabel, topN, maxMetrics)
	qtChild.Done()
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}
	if status.hasEntries() {
		return status, nil
	}
	db.doExtDB(func(extDB *readOnlyIndexDB) {
		qtChild := qt.NewChild("collect tsdb stats in the previous indexdb")
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		status, err = is.getTSDBStatus(qtChild, tfss, date, focusLabel, topN, maxMetrics)
		qtChild.Done()
		extDB.putIndexSearch(is)
	})
	if err != nil {
		return nil, fmt.Errorf("error when obtaining TSDB status from extDB: %w", err)
	}
	return status, nil
}

// getTSDBStatus returns topN entries for tsdb status for the given tfss, date and focusLabel.
func (is *indexSearchReadOnly) getTSDBStatus(qt *querytracer.Tracer, tfss []*TagFilters, date uint64, focusLabel string, topN, maxMetrics int) (*TSDBStatus, error) {
	filter, err := is.searchMetricIDsWithFiltersOnDate(qt, tfss, date, maxMetrics)
	if err != nil {
		return nil, err
	}
	if filter != nil && filter.Len() == 0 {
		qt.Printf("no matching series for filter=%s", tfss)
		return &TSDBStatus{}, nil
	}
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	thSeriesCountByMetricName := newTopHeap(topN)
	thSeriesCountByLabelName := newTopHeap(topN)
	thSeriesCountByFocusLabelValue := newTopHeap(topN)
	thSeriesCountByLabelValuePair := newTopHeap(topN)
	thLabelValueCountByLabelName := newTopHeap(topN)
	var tmp, prevLabelName, prevLabelValuePair []byte
	var labelValueCountByLabelName, seriesCountByLabelValuePair uint64
	var totalSeries, labelSeries, totalLabelValuePairs uint64
	nameEqualBytes := []byte("__name__=")
	focusLabelEqualBytes := []byte(focusLabel + "=")

	loopsPaceLimiter := 0
	nsPrefixExpected := byte(nsPrefixDateTagToMetricIDs)
	if date == globalIndexDate {
		nsPrefixExpected = nsPrefixTagToMetricIDs
	}
	kb.B = is.marshalCommonPrefixForDate(kb.B[:0], date)
	prefix := append([]byte{}, kb.B...)
	ts.Seek(prefix)
	for ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return nil, err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefixExpected); err != nil {
			return nil, err
		}
		matchingSeriesCount := mp.GetMatchingSeriesCount(filter, nil)
		if matchingSeriesCount == 0 {
			// Skip rows without matching metricIDs.
			continue
		}
		tmp = append(tmp[:0], mp.Tag.Key...)
		labelName := tmp
		if isArtificialTagKey(labelName) {
			// Skip artificially created tag keys.
			kb.B = append(kb.B[:0], prefix...)
			if len(labelName) > 0 && labelName[0] == compositeTagKeyPrefix {
				kb.B = append(kb.B, compositeTagKeyPrefix)
			} else {
				kb.B = marshalTagValue(kb.B, labelName)
			}
			kb.B[len(kb.B)-1]++
			ts.Seek(kb.B)
			continue
		}
		if len(labelName) == 0 {
			labelName = append(labelName, "__name__"...)
			tmp = labelName
		}
		if string(labelName) == "__name__" {
			totalSeries += uint64(matchingSeriesCount)
		}
		tmp = append(tmp, '=')
		tmp = append(tmp, mp.Tag.Value...)
		labelValuePair := tmp
		if len(prevLabelName) == 0 {
			prevLabelName = append(prevLabelName[:0], labelName...)
		}
		if string(labelName) != string(prevLabelName) {
			thLabelValueCountByLabelName.push(prevLabelName, labelValueCountByLabelName)
			thSeriesCountByLabelName.push(prevLabelName, labelSeries)
			labelSeries = 0
			labelValueCountByLabelName = 0
			prevLabelName = append(prevLabelName[:0], labelName...)
		}
		if len(prevLabelValuePair) == 0 {
			prevLabelValuePair = append(prevLabelValuePair[:0], labelValuePair...)
			labelValueCountByLabelName++
		}
		if string(labelValuePair) != string(prevLabelValuePair) {
			thSeriesCountByLabelValuePair.push(prevLabelValuePair, seriesCountByLabelValuePair)
			if bytes.HasPrefix(prevLabelValuePair, nameEqualBytes) {
				thSeriesCountByMetricName.push(prevLabelValuePair[len(nameEqualBytes):], seriesCountByLabelValuePair)
			}
			if bytes.HasPrefix(prevLabelValuePair, focusLabelEqualBytes) {
				thSeriesCountByFocusLabelValue.push(prevLabelValuePair[len(focusLabelEqualBytes):], seriesCountByLabelValuePair)
			}
			seriesCountByLabelValuePair = 0
			labelValueCountByLabelName++
			prevLabelValuePair = append(prevLabelValuePair[:0], labelValuePair...)
		}
		// It is OK if series can be counted multiple times in rare cases -
		// the returned number is an estimation.
		labelSeries += uint64(matchingSeriesCount)
		seriesCountByLabelValuePair += uint64(matchingSeriesCount)
		totalLabelValuePairs += uint64(matchingSeriesCount)
	}
	if err := ts.Error(); err != nil {
		return nil, fmt.Errorf("error when counting time series by metric names: %w", err)
	}
	thLabelValueCountByLabelName.push(prevLabelName, labelValueCountByLabelName)
	thSeriesCountByLabelName.push(prevLabelName, labelSeries)
	thSeriesCountByLabelValuePair.push(prevLabelValuePair, seriesCountByLabelValuePair)
	if bytes.HasPrefix(prevLabelValuePair, nameEqualBytes) {
		thSeriesCountByMetricName.push(prevLabelValuePair[len(nameEqualBytes):], seriesCountByLabelValuePair)
	}
	if bytes.HasPrefix(prevLabelValuePair, focusLabelEqualBytes) {
		thSeriesCountByFocusLabelValue.push(prevLabelValuePair[len(focusLabelEqualBytes):], seriesCountByLabelValuePair)
	}
	status := &TSDBStatus{
		TotalSeries:                  totalSeries,
		TotalLabelValuePairs:         totalLabelValuePairs,
		SeriesCountByMetricName:      thSeriesCountByMetricName.getSortedResult(),
		SeriesCountByLabelName:       thSeriesCountByLabelName.getSortedResult(),
		SeriesCountByFocusLabelValue: thSeriesCountByFocusLabelValue.getSortedResult(),
		SeriesCountByLabelValuePair:  thSeriesCountByLabelValuePair.getSortedResult(),
		LabelValueCountByLabelName:   thLabelValueCountByLabelName.getSortedResult(),
	}
	return status, nil
}

// searchMetricName appends metric name for the given metricID to dst
// and returns the result.
func (db *readOnlyIndexDB) searchMetricName(dst []byte, metricID uint64, accountID, projectID uint32, noCache bool) ([]byte, bool) {
	if !noCache {
		metricName := db.getMetricNameFromCache(dst, metricID)
		if len(metricName) > len(dst) {
			return metricName, true
		}
	}

	is := db.getIndexSearchInternal(accountID, projectID, noDeadline, noCache)
	var ok bool
	dst, ok = is.searchMetricName(dst, metricID)
	db.putIndexSearch(is)
	if ok {
		// There is no need in verifying whether the given metricID is deleted,
		// since the filtering must be performed before calling this func.
		if !noCache {
			db.putMetricNameToCache(metricID, dst)
		}
		return dst, true
	}

	// Try searching in the external indexDBReadOnly.
	db.doExtDB(func(extDB *readOnlyIndexDB) {
		is := extDB.getIndexSearchInternal(accountID, projectID, noDeadline, noCache)
		dst, ok = is.searchMetricName(dst, metricID)
		extDB.putIndexSearch(is)
		if ok && !noCache {
			// There is no need in verifying whether the given metricID is deleted,
			// since the filtering must be performed before calling this func.
			extDB.putMetricNameToCache(metricID, dst)
		}
	})
	if ok {
		return dst, true
	}

	return dst, false
}

// searchMetricIDs returns metricIDs for the given tfss and tr.
//
// The returned metricIDs are sorted.
func (db *readOnlyIndexDB) searchMetricIDs(qt *querytracer.Tracer, tfss []*TagFilters, tr TimeRange, maxMetrics int, deadline uint64) ([]uint64, error) {
	qt = qt.NewChild("search for matching metricIDs: filters=%s, timeRange=%s", tfss, &tr)
	defer qt.Done()

	if len(tfss) == 0 {
		return nil, nil
	}

	qtChild := qt.NewChild("search for metricIDs in the current indexdb")
	tfKeyBuf := tagFiltersKeyBufPool.Get()
	defer tagFiltersKeyBufPool.Put(tfKeyBuf)

	tfKeyBuf.B = marshalTagFiltersKey(tfKeyBuf.B[:0], tfss, tr, true)
	var metricIDs []uint64

	// Slow path - search for metricIDs in the db and extDB.
	accountID := tfss[0].accountID
	projectID := tfss[0].projectID
	is := db.getIndexSearch(accountID, projectID, deadline)
	localMetricIDs, err := is.searchMetricIDs(qtChild, tfss, tr, maxMetrics)
	db.putIndexSearch(is)
	if err != nil {
		return nil, fmt.Errorf("error when searching for metricIDs in the current indexdb: %w", err)
	}
	qtChild.Done()

	var extMetricIDs []uint64
	db.doExtDB(func(extDB *readOnlyIndexDB) {
		qtChild := qt.NewChild("search for metricIDs in the previous indexdb")
		defer qtChild.Done()

		tfKeyExtBuf := tagFiltersKeyBufPool.Get()
		defer tagFiltersKeyBufPool.Put(tfKeyExtBuf)

		// Data in extDB cannot be changed, so use unversioned keys for tag cache.
		tfKeyExtBuf.B = marshalTagFiltersKey(tfKeyExtBuf.B[:0], tfss, tr, false)
		is := extDB.getIndexSearch(accountID, projectID, deadline)
		extMetricIDs, err = is.searchMetricIDs(qtChild, tfss, tr, maxMetrics)
		extDB.putIndexSearch(is)
	})
	if err != nil {
		return nil, fmt.Errorf("error when searching for metricIDs in the previous indexdb: %w", err)
	}

	// Merge localMetricIDs with extMetricIDs.
	metricIDs = mergeSortedMetricIDs(localMetricIDs, extMetricIDs)
	qt.Printf("merge %d metricIDs from the current indexdb with %d metricIDs from the previous indexdb; result: %d metricIDs",
		len(localMetricIDs), len(extMetricIDs), len(metricIDs))

	return metricIDs, nil
}

func (db *readOnlyIndexDB) getTSIDsFromMetricIDs(qt *querytracer.Tracer, accountID, projectID uint32, metricIDs []uint64, deadline uint64) ([]TSID, error) {
	qt = qt.NewChild("obtain tsids from %d metricIDs", len(metricIDs))
	defer qt.Done()

	if len(metricIDs) == 0 {
		return nil, nil
	}

	// Search for TSIDs in the current indexdb
	tsids := make([]TSID, len(metricIDs))
	var extMetricIDs []uint64
	i := 0
	err := func() error {
		is := db.getIndexSearch(accountID, projectID, deadline)
		defer db.putIndexSearch(is)
		for loopsPaceLimiter, metricID := range metricIDs {
			if loopsPaceLimiter&paceLimiterSlowIterationsMask == 0 {
				if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
					return err
				}
			}
			// Try obtaining TSIDs from MetricID->TSID cache. This is much faster
			// than scanning the mergeset if it contains a lot of metricIDs.
			tsid := &tsids[i]
			err := is.db.getFromMetricIDCache(tsid, metricID)
			if err == nil {
				// Fast path - the tsid for metricID is found in cache.
				i++
				continue
			}
			if err != io.EOF {
				return err
			}
			if !is.getTSIDByMetricID(tsid, metricID) {
				// Postpone searching for the missing metricID in the extDB.
				extMetricIDs = append(extMetricIDs, metricID)
				continue
			}
			is.db.putToMetricIDCache(metricID, tsid)
			i++
		}
		return nil
	}()
	if err != nil {
		return nil, fmt.Errorf("error when searching for TISDs by metricIDs in the current indexdb: %w", err)
	}
	tsidsFound := i
	qt.Printf("found %d tsids for %d metricIDs in the current indexdb", tsidsFound, len(metricIDs))

	if len(extMetricIDs) > 0 {
		// Search for extMetricIDs in the previous indexdb (aka extDB)
		db.doExtDB(func(extDB *readOnlyIndexDB) {
			is := extDB.getIndexSearch(accountID, projectID, deadline)
			defer extDB.putIndexSearch(is)
			for loopsPaceLimiter, metricID := range extMetricIDs {
				if loopsPaceLimiter&paceLimiterSlowIterationsMask == 0 {
					if err = checkSearchDeadlineAndPace(is.deadline); err != nil {
						return
					}
				}
				// There is no need in searching for TSIDs in MetricID->TSID cache, since
				// this has been already done in the loop above (the MetricID->TSID cache is global).
				tsid := &tsids[i]
				if !is.getTSIDByMetricID(tsid, metricID) {
					// Cannot find TSID for the given metricID.
					// This may be the case on incomplete indexDBReadOnly
					// due to snapshot or due to un-flushed entries.
					// Mark the metricID as deleted, so it is created again when new sample
					// for the given time series is ingested next time.
					continue
				}
				is.db.putToMetricIDCache(metricID, tsid)
				i++
			}
		})
		if err != nil {
			return nil, fmt.Errorf("error when searching for TSIDs by metricIDs in the previous indexdb: %w", err)
		}
		qt.Printf("found %d tsids for %d metricIDs in the previous indexdb", i-tsidsFound, len(extMetricIDs))
	}

	tsids = tsids[:i]
	qt.Printf("load %d tsids for %d metricIDs from both current and previous indexdb", len(tsids), len(metricIDs))

	// Sort the found tsids, since they must be passed to TSID search
	// in the sorted order.
	sort.Slice(tsids, func(i, j int) bool { return tsids[i].Less(&tsids[j]) })
	qt.Printf("sort %d tsids", len(tsids))
	return tsids, nil
}

func (is *indexSearchReadOnly) searchMetricNameWithCache(dst []byte, metricID uint64) ([]byte, bool) {
	metricName := is.db.getMetricNameFromCache(dst, metricID)
	if len(metricName) > len(dst) {
		return metricName, true
	}
	var ok bool
	dst, ok = is.searchMetricName(dst, metricID)
	if ok {
		// There is no need in verifying whether the given metricID is deleted,
		// since the filtering must be performed before calling this func.
		is.db.putMetricNameToCache(metricID, dst)
		return dst, true
	}
	return dst, false
}

func (is *indexSearchReadOnly) searchMetricName(dst []byte, metricID uint64) ([]byte, bool) {
	ts := &is.ts
	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixMetricIDToMetricName)
	kb.B = encoding.MarshalUint64(kb.B, metricID)
	if err := ts.FirstItemWithPrefix(kb.B); err != nil {
		if err == io.EOF {
			return dst, false
		}
		logger.Panicf("FATAL: error when searching metricName by metricID; searchPrefix %q: %s", kb.B, err)
	}
	v := ts.Item[len(kb.B):]
	dst = append(dst, v...)
	return dst, true
}

func (is *indexSearchReadOnly) containsTimeRange(tr TimeRange) bool {
	if tr == globalIndexTimeRange {
		return true
	}

	db := is.db
	if db.hasExtDB() {
		// The db corresponds to the current indexDBReadOnly, which is used for storing index data for newly registered time series.
		// This means that it may contain data for the given tr with probability close to 100%.
		return true
	}
	// The db corresponds to the previous indexDBReadOnly, which is readonly.
	// So it is safe caching the minimum timestamp, which isn't covered by the db.

	// use common prefix as a key for minMissingTimestamp
	// it's needed to properly track timestamps for cluster version
	// which uses tenant labels for the index search
	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixDateToMetricID)

	return is.containsTimeRangeSlowForPrefixBuf(kb, tr)
}

func (is *indexSearchReadOnly) containsTimeRangeSlowForPrefixBuf(prefixBuf *bytesutil.ByteBuffer, tr TimeRange) bool {
	ts := &is.ts

	// Verify whether the tr.MinTimestamp is included into `ts` or is smaller than the minimum date stored in `ts`.
	// Do not check whether tr.MaxTimestamp is included into `ts` or is bigger than the max date stored in `ts` for performance reasons.
	// This means that containsTimeRangeSlow() can return true if `tr` is located below the min date stored in `ts`.
	// This is OK, since this case isn't encountered too much in practice.
	// The main practical case allows skipping searching in prev indexdb (`ts`) when `tr`
	// is located above the max date stored there.
	minDate := uint64(tr.MinTimestamp) / msecPerDay
	prefix := prefixBuf.B
	prefixBuf.B = encoding.MarshalUint64(prefixBuf.B, minDate)
	ts.Seek(prefixBuf.B)
	if !ts.NextItem() {
		if err := ts.Error(); err != nil {
			logger.Panicf("FATAL: error when searching for minDate=%d, prefix %q: %w", minDate, prefixBuf.B, err)
		}
		return false
	}
	if !bytes.HasPrefix(ts.Item, prefix) {
		// minDate exceeds max date from ts.
		return false
	}
	return true
}

func (is *indexSearchReadOnly) getTSIDByMetricID(dst *TSID, metricID uint64) bool {
	// There is no need in checking for deleted metricIDs here, since they
	// must be checked by the caller.
	ts := &is.ts
	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixMetricIDToTSID)
	kb.B = encoding.MarshalUint64(kb.B, metricID)
	if err := ts.FirstItemWithPrefix(kb.B); err != nil {
		if err == io.EOF {
			return false
		}
		logger.Panicf("FATAL: error when searching TSID by metricID=%d; searchPrefix %q: %s", metricID, kb.B, err)
	}
	v := ts.Item[len(kb.B):]
	tail, err := dst.Unmarshal(v)
	if err != nil {
		logger.Panicf("FATAL: cannot unmarshal the found TSID=%X for metricID=%d: %s", v, metricID, err)
	}
	if len(tail) > 0 {
		logger.Panicf("FATAL: unexpected non-zero tail left after unmarshaling TSID for metricID=%d: %X", metricID, tail)
	}
	return true
}

// updateMetricIDsByMetricNameMatch matches metricName values for the given srcMetricIDs against tfs
// and adds matching metrics to metricIDs.
func (is *indexSearchReadOnly) updateMetricIDsByMetricNameMatch(qt *querytracer.Tracer, metricIDs, srcMetricIDs *uint64set.Set, tfs []*tagFilter) error {
	qt = qt.NewChild("filter out %d metric ids with filters=%s", srcMetricIDs.Len(), tfs)
	defer qt.Done()

	// sort srcMetricIDs in order to speed up Seek below.
	sortedMetricIDs := srcMetricIDs.AppendTo(nil)
	qt.Printf("sort %d metric ids", len(sortedMetricIDs))

	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagToMetricIDs)
	tfs = removeCompositeTagFilters(tfs, kb.B)

	metricName := kbPool.Get()
	defer kbPool.Put(metricName)
	mn := GetMetricName()
	defer PutMetricName(mn)
	for loopsPaceLimiter, metricID := range sortedMetricIDs {
		if loopsPaceLimiter&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		var ok bool
		metricName.B, ok = is.searchMetricNameWithCache(metricName.B[:0], metricID)
		if !ok {
			// It is likely the metricID->metricName entry didn't propagate to inverted index yet.
			// Skip this metricID for now.
			continue
		}
		if err := mn.Unmarshal(metricName.B); err != nil {
			logger.Panicf("FATAL: cannot unmarshal metricName %q: %s", metricName.B, err)
		}

		// Match the mn against tfs.
		ok, err := matchTagFilters(mn, tfs, &is.kb)
		if err != nil {
			return fmt.Errorf("cannot match MetricName %s against tagFilters: %w", mn, err)
		}
		if !ok {
			continue
		}
		metricIDs.Add(metricID)
	}
	qt.Printf("apply filters %s; resulting metric ids: %d", tfs, metricIDs.Len())
	return nil
}

func (is *indexSearchReadOnly) searchMetricIDsWithFiltersOnDate(qt *querytracer.Tracer, tfss []*TagFilters, date uint64, maxMetrics int) (*uint64set.Set, error) {
	if len(tfss) == 0 {
		return nil, nil
	}

	var tr TimeRange
	if date == globalIndexDate {
		tr = globalIndexTimeRange
	} else {
		tr = TimeRange{
			MinTimestamp: int64(date) * msecPerDay,
			MaxTimestamp: int64(date+1)*msecPerDay - 1,
		}
	}

	metricIDs, err := is.searchMetricIDsInternal(qt, tfss, tr, maxMetrics)
	if err != nil {
		return nil, err
	}
	return metricIDs, nil
}

// searchMetricIDs returns metricIDs for the given tfss and tr.
//
// The returned metricIDs are sorted.
func (is *indexSearchReadOnly) searchMetricIDs(qt *querytracer.Tracer, tfss []*TagFilters, tr TimeRange, maxMetrics int) ([]uint64, error) {
	metricIDs, err := is.searchMetricIDsInternal(qt, tfss, tr, maxMetrics)
	if err != nil {
		return nil, err
	}
	if metricIDs.Len() == 0 {
		// Nothing found
		return nil, nil
	}

	sortedMetricIDs := metricIDs.AppendTo(nil)
	qt.Printf("sort %d matching metric ids", len(sortedMetricIDs))

	return sortedMetricIDs, nil
}

func (is *indexSearchReadOnly) searchMetricIDsInternal(qt *querytracer.Tracer, tfss []*TagFilters, tr TimeRange, maxMetrics int) (*uint64set.Set, error) {
	qt = qt.NewChild("search for metric ids: filters=%s, timeRange=%s, maxMetrics=%d", tfss, &tr, maxMetrics)
	defer qt.Done()

	metricIDs := &uint64set.Set{}

	// Always returns (true, nil) for zero time range used to indicate global
	// index search.
	if !is.containsTimeRange(tr) {
		qt.Printf("indexdb doesn't contain data for the given timeRange=%s", &tr)
		return metricIDs, nil
	}

	if tr.MinTimestamp >= is.db.s.minTimestampForCompositeIndex {
		tfss = convertToCompositeTagFilterss(tfss)
		qt.Printf("composite filters=%s", tfss)
	}

	for _, tfs := range tfss {
		if len(tfs.tfs) == 0 {
			// An empty filters must be equivalent to `{__name__!=""}`
			tfs = NewTagFilters(tfs.accountID, tfs.projectID)
			if err := tfs.Add(nil, nil, true, false); err != nil {
				logger.Panicf(`BUG: cannot add {__name__!=""} filter: %s`, err)
			}
		}
		qtChild := qt.NewChild("update metric ids: filters=%s, timeRange=%s", tfs, &tr)
		prevMetricIDsLen := metricIDs.Len()
		err := is.updateMetricIDsForTagFilters(qtChild, metricIDs, tfs, tr, maxMetrics+1)
		qtChild.Donef("updated %d metric ids", metricIDs.Len()-prevMetricIDsLen)
		if err != nil {
			return nil, err
		}
		if metricIDs.Len() > maxMetrics {
			return nil, errTooManyTimeseries(maxMetrics)
		}
	}
	return metricIDs, nil
}

func (is *indexSearchReadOnly) updateMetricIDsForTagFilters(qt *querytracer.Tracer, metricIDs *uint64set.Set, tfs *TagFilters, tr TimeRange, maxMetrics int) error {
	if tr != globalIndexTimeRange {
		// Fast path - search metricIDs by date range in the per-day inverted
		// index.
		qt.Printf("search metric ids in the per-day index")
		is.db.dateRangeSearchCalls.Add(1)
		minDate, maxDate := tr.DateRange()
		return is.updateMetricIDsForDateRange(qt, metricIDs, tfs, minDate, maxDate, maxMetrics)
	}

	// Slow path - search metricIDs in the global inverted index.
	qt.Printf("search metric ids in the global index")
	is.db.globalSearchCalls.Add(1)
	m, err := is.getMetricIDsForDateAndFilters(qt, globalIndexDate, tfs, maxMetrics)
	if err != nil {
		return err
	}
	metricIDs.UnionMayOwn(m)
	return nil
}

func (is *indexSearchReadOnly) getMetricIDsForTagFilter(qt *querytracer.Tracer, tf *tagFilter, maxMetrics int, maxLoopsCount int64) (*uint64set.Set, int64, error) {
	if tf.isNegative {
		logger.Panicf("BUG: isNegative must be false")
	}
	metricIDs := &uint64set.Set{}
	if len(tf.orSuffixes) > 0 {
		// Fast path for orSuffixes - seek for rows for each value from orSuffixes.
		loopsCount, err := is.updateMetricIDsForOrSuffixes(tf, metricIDs, maxMetrics, maxLoopsCount)
		qt.Printf("found %d metric ids for filter={%s} using exact search; spent %d loops", metricIDs.Len(), tf, loopsCount)
		if err != nil {
			return nil, loopsCount, fmt.Errorf("error when searching for metricIDs for tagFilter in fast path: %w; tagFilter=%s", err, tf)
		}
		return metricIDs, loopsCount, nil
	}

	// Slow path - scan for all the rows with the given prefix.
	loopsCount, err := is.getMetricIDsForTagFilterSlow(tf, metricIDs.Add, maxLoopsCount)
	qt.Printf("found %d metric ids for filter={%s} using prefix search; spent %d loops", metricIDs.Len(), tf, loopsCount)
	if err != nil {
		return nil, loopsCount, fmt.Errorf("error when searching for metricIDs for tagFilter in slow path: %w; tagFilter=%s", err, tf)
	}
	return metricIDs, loopsCount, nil
}

func (is *indexSearchReadOnly) getMetricIDsForTagFilterSlow(tf *tagFilter, f func(metricID uint64), maxLoopsCount int64) (int64, error) {
	if len(tf.orSuffixes) > 0 {
		logger.Panicf("BUG: the getMetricIDsForTagFilterSlow must be called only for empty tf.orSuffixes; got %s", tf.orSuffixes)
	}

	// Scan all the rows with tf.prefix and call f on every tf match.
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	var prevMatchingSuffix []byte
	var prevMatch bool
	var loopsCount int64
	loopsPaceLimiter := 0
	prefix := tf.prefix
	ts.Seek(prefix)
	for ts.NextItem() {
		if loopsPaceLimiter&paceLimiterMediumIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return loopsCount, err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			return loopsCount, nil
		}
		tail := item[len(prefix):]
		n := bytes.IndexByte(tail, tagSeparatorChar)
		if n < 0 {
			return loopsCount, fmt.Errorf("invalid tag->metricIDs line %q: cannot find tagSeparatorChar=%d", item, tagSeparatorChar)
		}
		suffix := tail[:n+1]
		tail = tail[n+1:]
		if err := mp.InitOnlyTail(item, tail); err != nil {
			return loopsCount, err
		}
		mp.ParseMetricIDs()
		loopsCount += int64(mp.MetricIDsLen())
		if loopsCount > maxLoopsCount {
			return loopsCount, errTooManyLoops
		}
		if prevMatch && string(suffix) == string(prevMatchingSuffix) {
			// Fast path: the same tag value found.
			// There is no need in checking it again with potentially
			// slow tf.matchSuffix, which may call regexp.
			for _, metricID := range mp.MetricIDs {
				f(metricID)
			}
			continue
		}
		// Slow path: need tf.matchSuffix call.
		ok, err := tf.matchSuffix(suffix)
		// Assume that tf.matchSuffix call needs 10x more time than a single metric scan iteration.
		loopsCount += 10 * int64(tf.matchCost)
		if err != nil {
			return loopsCount, fmt.Errorf("error when matching %s against suffix %q: %w", tf, suffix, err)
		}
		if !ok {
			prevMatch = false
			if mp.MetricIDsLen() < maxMetricIDsPerRow/2 {
				// If the current row contains non-full metricIDs list,
				// then it is likely the next row contains the next tag value.
				// So skip seeking for the next tag value, since it will be slower than just ts.NextItem call.
				continue
			}
			// Optimization: skip all the metricIDs for the given tag value
			kb.B = append(kb.B[:0], item[:len(item)-len(tail)]...)
			// The last char in kb.B must be tagSeparatorChar. Just increment it
			// in order to jump to the next tag value.
			if len(kb.B) == 0 || kb.B[len(kb.B)-1] != tagSeparatorChar || tagSeparatorChar >= 0xff {
				return loopsCount, fmt.Errorf("data corruption: the last char in k=%X must be %X", kb.B, tagSeparatorChar)
			}
			kb.B[len(kb.B)-1]++
			ts.Seek(kb.B)
			// Assume that a seek cost is equivalent to 1000 ordinary loops.
			loopsCount += 1000
			continue
		}
		prevMatch = true
		prevMatchingSuffix = append(prevMatchingSuffix[:0], suffix...)
		for _, metricID := range mp.MetricIDs {
			f(metricID)
		}
	}
	if err := ts.Error(); err != nil {
		return loopsCount, fmt.Errorf("error when searching for tag filter prefix %q: %w", prefix, err)
	}
	return loopsCount, nil
}

func (is *indexSearchReadOnly) updateMetricIDsForOrSuffixes(tf *tagFilter, metricIDs *uint64set.Set, maxMetrics int, maxLoopsCount int64) (int64, error) {
	if tf.isNegative {
		logger.Panicf("BUG: isNegative must be false")
	}
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	var loopsCount int64
	for _, orSuffix := range tf.orSuffixes {
		kb.B = append(kb.B[:0], tf.prefix...)
		kb.B = append(kb.B, orSuffix...)
		kb.B = append(kb.B, tagSeparatorChar)
		lc, err := is.updateMetricIDsForOrSuffix(kb.B, metricIDs, maxMetrics, maxLoopsCount-loopsCount)
		loopsCount += lc
		if err != nil {
			return loopsCount, err
		}
		if metricIDs.Len() >= maxMetrics {
			return loopsCount, nil
		}
	}
	return loopsCount, nil
}

func (is *indexSearchReadOnly) updateMetricIDsForOrSuffix(prefix []byte, metricIDs *uint64set.Set, maxMetrics int, maxLoopsCount int64) (int64, error) {
	ts := &is.ts
	mp := &is.mp
	var loopsCount int64
	loopsPaceLimiter := 0
	ts.Seek(prefix)
	for metricIDs.Len() < maxMetrics && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return loopsCount, err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			return loopsCount, nil
		}
		if err := mp.InitOnlyTail(item, item[len(prefix):]); err != nil {
			return loopsCount, err
		}
		loopsCount += int64(mp.MetricIDsLen())
		if loopsCount > maxLoopsCount {
			return loopsCount, errTooManyLoops
		}
		mp.ParseMetricIDs()
		metricIDs.AddMulti(mp.MetricIDs)
	}
	if err := ts.Error(); err != nil {
		return loopsCount, fmt.Errorf("error when searching for tag filter prefix %q: %w", prefix, err)
	}
	return loopsCount, nil
}

func (is *indexSearchReadOnly) updateMetricIDsForDateRange(qt *querytracer.Tracer, metricIDs *uint64set.Set, tfs *TagFilters, minDate, maxDate uint64, maxMetrics int) error {
	if minDate == maxDate {
		// Fast path - query only a single date.
		m, err := is.getMetricIDsForDateAndFilters(qt, minDate, tfs, maxMetrics)
		if err != nil {
			return err
		}
		metricIDs.UnionMayOwn(m)
		is.db.dateRangeSearchHits.Add(1)
		return nil
	}

	// Slower path - search for metricIDs for each day in parallel.
	qt = qt.NewChild("parallel search for metric ids in per-day index: filters=%s, dayRange=[%d..%d]", tfs, minDate, maxDate)
	defer qt.Done()
	wg := getWaitGroup()
	var errGlobal error
	var mu sync.Mutex // protects metricIDs + errGlobal vars from concurrent access below
	for minDate <= maxDate {
		qtChild := qt.NewChild("parallel thread for date=%s", dateToString(minDate))
		wg.Add(1)
		go func(date uint64) {
			defer func() {
				qtChild.Done()
				wg.Done()
			}()
			isLocal := is.db.getIndexSearch(is.accountID, is.projectID, is.deadline)
			m, err := isLocal.getMetricIDsForDateAndFilters(qtChild, date, tfs, maxMetrics)
			is.db.putIndexSearch(isLocal)
			mu.Lock()
			defer mu.Unlock()
			if errGlobal != nil {
				return
			}
			if err != nil {
				dateStr := time.Unix(int64(date*24*3600), 0)
				errGlobal = fmt.Errorf("cannot search for metricIDs at %s: %w", dateStr, err)
				return
			}
			if metricIDs.Len() < maxMetrics {
				metricIDs.UnionMayOwn(m)
			}
		}(minDate)
		minDate++
	}
	wg.Wait()
	putWaitGroup(wg)
	if errGlobal != nil {
		return errGlobal
	}
	is.db.dateRangeSearchHits.Add(1)
	return nil
}

func (is *indexSearchReadOnly) getMetricIDsForDateAndFilters(qt *querytracer.Tracer, date uint64, tfs *TagFilters, maxMetrics int) (*uint64set.Set, error) {
	if qt.Enabled() {
		qt = qt.NewChild("search for metric ids on a particular day: filters=%s, date=%s, maxMetrics=%d", tfs, dateToString(date), maxMetrics)
		defer qt.Done()
	}

	// Sort tfs by loopsCount needed for performing each filter.
	// This stats is usually collected from the previous queries.
	// This way we limit the amount of work below by applying fast filters at first.
	type tagFilterWithWeight struct {
		tf               *tagFilter
		loopsCount       int64
		filterLoopsCount int64
	}
	tfws := make([]tagFilterWithWeight, len(tfs.tfs))
	currentTime := fasttime.UnixTimestamp()
	for i := range tfs.tfs {
		tf := &tfs.tfs[i]
		loopsCount, filterLoopsCount, timestamp := is.getLoopsCountAndTimestampForDateFilter(date, tf)
		if currentTime > timestamp+3600 {
			// Update stats once per hour for relatively fast tag filters.
			// There is no need in spending CPU resources on updating stats for heavy tag filters.
			if loopsCount <= 10e6 {
				loopsCount = 0
			}
			if filterLoopsCount <= 10e6 {
				filterLoopsCount = 0
			}
		}
		tfws[i] = tagFilterWithWeight{
			tf:               tf,
			loopsCount:       loopsCount,
			filterLoopsCount: filterLoopsCount,
		}
	}
	sort.Slice(tfws, func(i, j int) bool {
		a, b := &tfws[i], &tfws[j]
		if a.loopsCount != b.loopsCount {
			return a.loopsCount < b.loopsCount
		}
		return a.tf.Less(b.tf)
	})
	getFirstPositiveLoopsCount := func(tfws []tagFilterWithWeight) int64 {
		for i := range tfws {
			if n := tfws[i].loopsCount; n > 0 {
				return n
			}
		}
		return int64Max
	}
	storeLoopsCount := func(tfw *tagFilterWithWeight, loopsCount int64) {
		if loopsCount != tfw.loopsCount {
			tfw.loopsCount = loopsCount
			is.storeLoopsCountForDateFilter(date, tfw.tf, tfw.loopsCount, tfw.filterLoopsCount)
		}
	}

	// Populate metricIDs for the first non-negative filter with the smallest cost.
	qtChild := qt.NewChild("search for the first non-negative filter with the smallest cost")
	var metricIDs *uint64set.Set
	tfwsRemaining := tfws[:0]
	maxDateMetrics := intMax
	if maxMetrics < intMax/50 {
		maxDateMetrics = maxMetrics * 50
	}
	for i, tfw := range tfws {
		tf := tfw.tf
		if tf.isNegative || tf.isEmptyMatch {
			tfwsRemaining = append(tfwsRemaining, tfw)
			continue
		}
		maxLoopsCount := getFirstPositiveLoopsCount(tfws[i+1:])
		m, loopsCount, err := is.getMetricIDsForDateTagFilter(qtChild, tf, date, tfs.commonPrefix, maxDateMetrics, maxLoopsCount)
		if err != nil {
			if errors.Is(err, errTooManyLoops) {
				// The tf took too many loops compared to the next filter. Postpone applying this filter.
				qtChild.Printf("the filter={%s} took more than %d loops; postpone it", tf, maxLoopsCount)
				storeLoopsCount(&tfw, 2*loopsCount)
				tfwsRemaining = append(tfwsRemaining, tfw)
				continue
			}
			// Move failing filter to the end of filter list.
			storeLoopsCount(&tfw, int64Max)
			return nil, err
		}
		if m.Len() >= maxDateMetrics {
			// Too many time series found by a single tag filter. Move the filter to the end of list.
			qtChild.Printf("the filter={%s} matches at least %d series; postpone it", tf, maxDateMetrics)
			storeLoopsCount(&tfw, int64Max-1)
			tfwsRemaining = append(tfwsRemaining, tfw)
			continue
		}
		storeLoopsCount(&tfw, loopsCount)
		metricIDs = m
		tfwsRemaining = append(tfwsRemaining, tfws[i+1:]...)
		qtChild.Printf("the filter={%s} matches less than %d series (actually %d series); use it", tf, maxDateMetrics, metricIDs.Len())
		break
	}
	qtChild.Done()
	tfws = tfwsRemaining

	if metricIDs == nil {
		// All the filters in tfs are negative or match too many time series.
		// Populate all the metricIDs for the given (date),
		// so later they can be filtered out with negative filters.
		qt.Printf("all the filters are negative or match more than %d time series; fall back to searching for all the metric ids", maxDateMetrics)
		m, err := is.getMetricIDsForDate(date, maxDateMetrics)
		if err != nil {
			return nil, fmt.Errorf("cannot obtain all the metricIDs: %w", err)
		}
		if m.Len() >= maxDateMetrics {
			// Too many time series found for the given (date). Fall back to global search.
			return nil, errTooManyTimeseries(maxDateMetrics)
		}
		metricIDs = m
		qt.Printf("found %d metric ids", metricIDs.Len())
	}

	sort.Slice(tfws, func(i, j int) bool {
		a, b := &tfws[i], &tfws[j]
		if a.filterLoopsCount != b.filterLoopsCount {
			return a.filterLoopsCount < b.filterLoopsCount
		}
		return a.tf.Less(b.tf)
	})
	getFirstPositiveFilterLoopsCount := func(tfws []tagFilterWithWeight) int64 {
		for i := range tfws {
			if n := tfws[i].filterLoopsCount; n > 0 {
				return n
			}
		}
		return int64Max
	}
	storeFilterLoopsCount := func(tfw *tagFilterWithWeight, filterLoopsCount int64) {
		if filterLoopsCount != tfw.filterLoopsCount {
			is.storeLoopsCountForDateFilter(date, tfw.tf, tfw.loopsCount, filterLoopsCount)
		}
	}

	// Intersect metricIDs with the rest of filters.
	//
	// Do not run these tag filters in parallel, since this may result in CPU and RAM waste
	// when the initial tag filters significantly reduce the number of found metricIDs,
	// so the remaining filters could be performed via much faster metricName matching instead
	// of slow selecting of matching metricIDs.
	qtChild = qt.NewChild("intersect the remaining %d filters with the found %d metric ids", len(tfws), metricIDs.Len())
	var tfsPostponed []*tagFilter
	for i, tfw := range tfws {
		tf := tfw.tf
		metricIDsLen := metricIDs.Len()
		if metricIDsLen == 0 {
			// There is no need in applying the remaining filters to an empty set.
			break
		}
		if tfw.filterLoopsCount > int64(metricIDsLen)*loopsCountPerMetricNameMatch {
			// It should be faster performing metricName match on the remaining filters
			// instead of scanning big number of entries in the inverted index for these filters.
			for _, tfw := range tfws[i:] {
				tfsPostponed = append(tfsPostponed, tfw.tf)
			}
			break
		}
		maxLoopsCount := getFirstPositiveFilterLoopsCount(tfws[i+1:])
		if maxLoopsCount == int64Max {
			maxLoopsCount = int64(metricIDsLen) * loopsCountPerMetricNameMatch
		}
		m, filterLoopsCount, err := is.getMetricIDsForDateTagFilter(qtChild, tf, date, tfs.commonPrefix, intMax, maxLoopsCount)
		if err != nil {
			if errors.Is(err, errTooManyLoops) {
				// Postpone tf, since it took more loops than the next filter may need.
				qtChild.Printf("postpone filter={%s}, since it took more than %d loops", tf, maxLoopsCount)
				storeFilterLoopsCount(&tfw, 2*filterLoopsCount)
				tfsPostponed = append(tfsPostponed, tf)
				continue
			}
			// Move failing tf to the end of filter list
			storeFilterLoopsCount(&tfw, int64Max)
			return nil, err
		}
		storeFilterLoopsCount(&tfw, filterLoopsCount)
		if tf.isNegative || tf.isEmptyMatch {
			metricIDs.Subtract(m)
			qtChild.Printf("subtract %d metric ids from the found %d metric ids for filter={%s}; resulting metric ids: %d", m.Len(), metricIDsLen, tf, metricIDs.Len())
		} else {
			metricIDs.Intersect(m)
			qtChild.Printf("intersect %d metric ids with the found %d metric ids for filter={%s}; resulting metric ids: %d", m.Len(), metricIDsLen, tf, metricIDs.Len())
		}
	}
	qtChild.Done()
	if metricIDs.Len() == 0 {
		// There is no need in applying tfsPostponed, since the result is empty.
		qt.Printf("found zero metric ids")
		return nil, nil
	}
	if len(tfsPostponed) > 0 {
		// Apply the postponed filters via metricName match.
		qt.Printf("apply postponed filters=%s to %d metrics ids", tfsPostponed, metricIDs.Len())
		var m uint64set.Set
		if err := is.updateMetricIDsByMetricNameMatch(qt, &m, metricIDs, tfsPostponed); err != nil {
			return nil, err
		}
		return &m, nil
	}
	qt.Printf("found %d metric ids", metricIDs.Len())
	return metricIDs, nil
}

func (is *indexSearchReadOnly) getMetricIDsForDateTagFilter(qt *querytracer.Tracer, tf *tagFilter, date uint64, commonPrefix []byte,
	maxMetrics int, maxLoopsCount int64,
) (*uint64set.Set, int64, error) {
	if qt.Enabled() {
		qt = qt.NewChild("get metric ids for filter and date: filter={%s}, date=%s, maxMetrics=%d, maxLoopsCount=%d", tf, dateToString(date), maxMetrics, maxLoopsCount)
		defer qt.Done()
	}

	if !bytes.HasPrefix(tf.prefix, commonPrefix) {
		logger.Panicf("BUG: unexpected tf.prefix %q; must start with commonPrefix %q", tf.prefix, commonPrefix)
	}
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	kb.B = is.marshalCommonPrefixForDate(kb.B[:0], date)
	prefix := kb.B
	kb.B = append(kb.B, tf.prefix[len(commonPrefix):]...)
	tfNew := *tf
	tfNew.isNegative = false // isNegative for the original tf is handled by the caller.
	tfNew.prefix = kb.B
	metricIDs, loopsCount, err := is.getMetricIDsForTagFilter(qt, &tfNew, maxMetrics, maxLoopsCount)
	if err != nil {
		return nil, loopsCount, err
	}
	if tf.isNegative || !tf.isEmptyMatch {
		return metricIDs, loopsCount, nil
	}
	// The tag filter, which matches empty label such as {foo=~"bar|"}
	// Convert it to negative filter, which matches {foo=~".+",foo!~"bar|"}.
	// This fixes https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1601
	// See also https://github.com/VictoriaMetrics/VictoriaMetrics/issues/395
	maxLoopsCount -= loopsCount
	var tfGross tagFilter
	if err := tfGross.Init(prefix, tf.key, []byte(".+"), false, true); err != nil {
		logger.Panicf(`BUG: cannot init tag filter: {%q=~".+"}: %s`, tf.key, err)
	}
	m, lc, err := is.getMetricIDsForTagFilter(qt, &tfGross, maxMetrics, maxLoopsCount)
	loopsCount += lc
	if err != nil {
		return nil, loopsCount, err
	}
	mLen := m.Len()
	m.Subtract(metricIDs)
	qt.Printf("subtract %d metric ids for filter={%s} from %d metric ids for filter={%s}", metricIDs.Len(), &tfNew, mLen, &tfGross)
	qt.Printf("found %d metric ids, spent %d loops", m.Len(), loopsCount)
	return m, loopsCount, nil
}

func (is *indexSearchReadOnly) getLoopsCountAndTimestampForDateFilter(date uint64, tf *tagFilter) (int64, int64, uint64) {
	is.kb.B = appendDateTagFilterCacheKey(is.kb.B[:0], is.db.name, date, tf, is.accountID, is.projectID)
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	kb.B = is.db.loopsPerDateTagFilterCache.Get(kb.B[:0], is.kb.B)
	if len(kb.B) != 3*8 {
		return 0, 0, 0
	}
	loopsCount := encoding.UnmarshalInt64(kb.B)
	filterLoopsCount := encoding.UnmarshalInt64(kb.B[8:])
	timestamp := encoding.UnmarshalUint64(kb.B[16:])
	return loopsCount, filterLoopsCount, timestamp
}

func (is *indexSearchReadOnly) storeLoopsCountForDateFilter(date uint64, tf *tagFilter, loopsCount, filterLoopsCount int64) {
	currentTimestamp := fasttime.UnixTimestamp()
	is.kb.B = appendDateTagFilterCacheKey(is.kb.B[:0], is.db.name, date, tf, is.accountID, is.projectID)
	kb := kbPool.Get()
	kb.B = encoding.MarshalInt64(kb.B[:0], loopsCount)
	kb.B = encoding.MarshalInt64(kb.B, filterLoopsCount)
	kb.B = encoding.MarshalUint64(kb.B, currentTimestamp)
	is.db.loopsPerDateTagFilterCache.Set(is.kb.B, kb.B)
	kbPool.Put(kb)
}

func (is *indexSearchReadOnly) getMetricIDsForDate(date uint64, maxMetrics int) (*uint64set.Set, error) {
	// Extract all the metricIDs from (date, __name__=value)->metricIDs entries.
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	kb.B = is.marshalCommonPrefixForDate(kb.B[:0], date)
	kb.B = marshalTagValue(kb.B, nil)
	var metricIDs uint64set.Set
	if err := is.updateMetricIDsForPrefix(kb.B, &metricIDs, maxMetrics); err != nil {
		return nil, err
	}
	return &metricIDs, nil
}

func (is *indexSearchReadOnly) updateMetricIDsForPrefix(prefix []byte, metricIDs *uint64set.Set, maxMetrics int) error {
	ts := &is.ts
	mp := &is.mp
	loopsPaceLimiter := 0
	ts.Seek(prefix)
	for ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			return nil
		}
		tail := item[len(prefix):]
		n := bytes.IndexByte(tail, tagSeparatorChar)
		if n < 0 {
			return fmt.Errorf("invalid tag->metricIDs line %q: cannot find tagSeparatorChar %d", item, tagSeparatorChar)
		}
		tail = tail[n+1:]
		if err := mp.InitOnlyTail(item, tail); err != nil {
			return err
		}
		mp.ParseMetricIDs()
		metricIDs.AddMulti(mp.MetricIDs)
		if metricIDs.Len() >= maxMetrics {
			return nil
		}
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for all metricIDs by prefix %q: %w", prefix, err)
	}
	return nil
}

func (is *indexSearchReadOnly) marshalCommonPrefix(dst []byte, nsPrefix byte) []byte {
	return marshalCommonPrefix(dst, nsPrefix, is.accountID, is.projectID)
}

func (is *indexSearchReadOnly) marshalCommonPrefixForDate(dst []byte, date uint64) []byte {
	if date == globalIndexDate {
		// Global index
		return is.marshalCommonPrefix(dst, nsPrefixTagToMetricIDs)
	}
	// Per-day index
	dst = is.marshalCommonPrefix(dst, nsPrefixDateTagToMetricIDs)
	return encoding.MarshalUint64(dst, date)
}
