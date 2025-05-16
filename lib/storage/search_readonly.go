package storage

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
)

// ReadOnlySearch is a search for time series.
type ReadOnlySearch struct {
	// MetricBlockRef is updated with each Search.NextMetricBlock call.
	MetricBlockRef MetricBlockRef

	// idb is used for MetricName lookup for the found data blocks.
	idb *readOnlyIndexDB

	// retentionDeadline is used for filtering out blocks outside the configured retention.
	retentionDeadline int64

	ts readOnlyTableSearch

	// tr contains time range used in the search.
	tr TimeRange

	// tfss contains tag filters used in the search.
	tfss []*TagFilters

	// deadline in unix timestamp seconds for the current search.
	deadline uint64

	err error

	needClosing bool

	loops int

	prevMetricID uint64

	// metricGroupBuf holds metricGroup used for metric names tracker
	metricGroupBuf []byte
}

func (s *ReadOnlySearch) reset() {
	s.MetricBlockRef.MetricName = s.MetricBlockRef.MetricName[:0]
	s.MetricBlockRef.BlockRef = nil

	s.idb = nil
	s.retentionDeadline = 0
	s.ts.reset()
	s.tr = TimeRange{}
	s.tfss = nil
	s.deadline = 0
	s.err = nil
	s.needClosing = false
	s.loops = 0
	s.prevMetricID = 0
	s.metricGroupBuf = nil
}

// Init initializes s from the given storage, tfss and tr.
//
// MustClose must be called when the search is done.
//
// Init returns the upper bound on the number of found time series.
func (s *ReadOnlySearch) Init(qt *querytracer.Tracer, storage *ReadOnlyStorage, tfss []*TagFilters, tr TimeRange, maxMetrics int, deadline uint64) int {
	qt = qt.NewChild("init series search: filters=%s, timeRange=%s", tfss, &tr)
	defer qt.Done()

	indexTR := storage.adjustTimeRange(tr)
	dataTR := tr

	if s.needClosing {
		logger.Panicf("BUG: missing MustClose call before the next call to Init")
	}
	retentionDeadline := int64(fasttime.UnixTimestamp()*1e3) - storage.retentionMsecs

	s.reset()

	idbPath := filepath.Join(storage.storagePath, indexdbDirname)
	s.idb = openReadOnlyCurrentIndexDB(idbPath, storage)
	s.retentionDeadline = retentionDeadline
	s.tr = tr
	s.tfss = tfss
	s.deadline = deadline
	s.needClosing = true

	var tsids []TSID
	metricIDs, err := s.idb.searchMetricIDs(qt, tfss, indexTR, maxMetrics, deadline)
	if err == nil && len(metricIDs) > 0 && len(tfss) > 0 {
		accountID := tfss[0].accountID
		projectID := tfss[0].projectID
		tsids, err = s.idb.getTSIDsFromMetricIDs(qt, accountID, projectID, metricIDs, deadline)
		if err == nil {
			err = storage.prefetchMetricNames(qt, s.idb, accountID, projectID, metricIDs, deadline)
		}
	}
	// It is ok to call Init on non-nil err.
	// Init must be called before returning because it will fail
	// on Search.MustClose otherwise.
	var initErr error
	for range 5 {
		err := func() (err error) {
			defer func() {
				if perr := recover(); perr != nil {
					err = fmt.Errorf("panic at s.ts.init: %v", err)
				}
			}()
			s.ts.Init(storage.tb, tsids, dataTR)
			return nil
		}()
		if err != nil {
			logger.Errorf("Retrying due to an error in table search init: %v", err)
			initErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		initErr = nil
		break
	}
	if initErr != nil {
		s.err = initErr
		return 0
	}

	qt.Printf("search for parts with data for %d series", len(tsids))
	if err != nil {
		s.err = err
		return 0
	}
	return len(tsids)
}

// MustClose closes the Search.
func (s *ReadOnlySearch) MustClose() {
	if !s.needClosing {
		logger.Panicf("BUG: missing Init call before MustClose")
	}
	s.reset()
}

// Error returns the last error from s.
func (s *ReadOnlySearch) Error() error {
	if s.err == io.EOF || s.err == nil {
		return nil
	}
	return fmt.Errorf("error when searching for tagFilters=%s on the time range %s: %w", s.tfss, s.tr.String(), s.err)
}

// NextMetricBlock proceeds to the next MetricBlockRef.
func (s *ReadOnlySearch) NextMetricBlock() bool {
	if s.err != nil {
		return false
	}
	for s.ts.NextBlock() {
		if s.loops&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(s.deadline); err != nil {
				s.err = err
				return false
			}
		}
		s.loops++
		tsid := &s.ts.BlockRef.bh.TSID
		if tsid.MetricID != s.prevMetricID {
			if s.ts.BlockRef.bh.MaxTimestamp < s.retentionDeadline {
				// Skip the block, since it contains only data outside the configured retention.
				continue
			}
			var ok bool
			s.MetricBlockRef.MetricName, ok = s.idb.searchMetricName(s.MetricBlockRef.MetricName[:0], tsid.MetricID, tsid.AccountID, tsid.ProjectID, false)
			if !ok {
				// Skip missing metricName for tsid.MetricID.
				// It should be automatically fixed. See indexDB.searchMetricNameWithCache for details.
				continue
			}
			// for performance reasons parse metricGroup conditionally
			if s.idb.s.metricsTracker != nil {
				var err error
				// MetricName must be sorted and marshalled with MetricName.Marshal()
				// it guarantees that first tag is metricGroup
				if len(s.MetricBlockRef.MetricName) < 8 {
					s.err = fmt.Errorf("BUG: unexpected MetricBlockRef.MetricName len: %d, want at least 8", len(s.MetricBlockRef.MetricName))
					return false
				}
				_, s.metricGroupBuf, err = unmarshalTagValue(s.metricGroupBuf[:0], s.MetricBlockRef.MetricName[8:])
				if err != nil {
					s.err = fmt.Errorf("cannot unmarshal metricGroup from MetricBlockRef.MetricName: %w", err)
					return false
				}
				s.idb.s.metricsTracker.RegisterQueryRequest(tsid.AccountID, tsid.ProjectID, s.metricGroupBuf)
			}
			s.prevMetricID = tsid.MetricID
		}
		s.MetricBlockRef.BlockRef = s.ts.BlockRef
		return true
	}
	if err := s.ts.Error(); err != nil {
		s.err = err
		return false
	}

	s.err = io.EOF
	return false
}
