package storage

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/valyala/fastrand"
)

// table represents a single table with time series data.
type readOnlyTable struct {
	path                string
	smallPartitionsPath string
	bigPartitionsPath   string

	s *ReadOnlyStorage

	ptws     []*partitionWrapper
	ptwsLock sync.Mutex

	stopCh chan struct{}

	retentionWatcherWG  sync.WaitGroup
	finalDedupWatcherWG sync.WaitGroup
	forceMergeWG        sync.WaitGroup
}

// mustOpenTable opens a table on the given path.
//
// The table is created if it doesn't exist.
func mustOpenReadOnlyTable(path string, s *ReadOnlyStorage) *readOnlyTable {
	path = filepath.Clean(path)

	// Create directories for small and big partitions if they don't exist yet.
	smallPartitionsPath := filepath.Join(path, smallDirname)
	bigPartitionsPath := filepath.Join(path, bigDirname)

	// Open partitions.
	pts := mustOpenPartitionsReadOnly(smallPartitionsPath, bigPartitionsPath, s)

	tb := &readOnlyTable{
		path:                path,
		smallPartitionsPath: smallPartitionsPath,
		bigPartitionsPath:   bigPartitionsPath,
		s:                   s,

		stopCh: make(chan struct{}),
	}
	for _, pt := range pts {
		tb.addPartitionNolock(pt)
	}
	return tb
}

func (tb *readOnlyTable) addPartitionNolock(pt *partition) {
	ptw := &partitionWrapper{
		pt: pt,
	}
	ptw.incRef()
	tb.ptws = append(tb.ptws, ptw)
}

// MustClose closes the table.
//
// This func must be called only when there are no goroutines using the the
// table, such as ones that ingest or retrieve time series samples or index
// data.
func (tb *readOnlyTable) MustClose() {
	close(tb.stopCh)
	tb.retentionWatcherWG.Wait()
	tb.finalDedupWatcherWG.Wait()
	tb.forceMergeWG.Wait()

	tb.ptwsLock.Lock()
	ptws := tb.ptws
	tb.ptws = nil
	tb.ptwsLock.Unlock()

	for _, ptw := range ptws {
		if n := ptw.refCount.Load(); n != 1 {
			logger.Panicf("BUG: unexpected refCount=%d when closing the partition; probably there are pending searches", n)
		}
		ptw.decRef()
	}
}

// flushPendingRows flushes all the pending raw rows, so they become visible to search.
//
// This function is for debug purposes only.
func (tb *readOnlyTable) flushPendingRows() {
	ptws := tb.GetPartitions(nil)
	defer tb.PutPartitions(ptws)

	for _, ptw := range ptws {
		ptw.pt.flushPendingRows(true)
	}
}

func (tb *readOnlyTable) NotifyReadWriteMode() {
	tb.ptwsLock.Lock()
	for _, ptw := range tb.ptws {
		ptw.pt.NotifyReadWriteMode()
	}
	tb.ptwsLock.Unlock()
}

// ForceMergePartitions force-merges partitions in tb with names starting from the given partitionNamePrefix.
//
// Partitions are merged sequentially in order to reduce load on the system.
func (tb *readOnlyTable) ForceMergePartitions(partitionNamePrefix string) error {
	ptws := tb.GetPartitions(nil)
	defer tb.PutPartitions(ptws)

	tb.forceMergeWG.Add(1)
	defer tb.forceMergeWG.Done()

	for _, ptw := range ptws {
		if !strings.HasPrefix(ptw.pt.name, partitionNamePrefix) {
			continue
		}
		logger.Infof("starting forced merge for partition %q", ptw.pt.name)
		startTime := time.Now()
		if err := ptw.pt.ForceMergeAllParts(tb.stopCh); err != nil {
			return fmt.Errorf("cannot complete forced merge for partition %q: %w", ptw.pt.name, err)
		}
		logger.Infof("forced merge for partition %q has been finished in %.3f seconds", ptw.pt.name, time.Since(startTime).Seconds())
	}
	return nil
}

func (tb *readOnlyTable) getMinMaxTimestamps() (int64, int64) {
	now := int64(fasttime.UnixTimestamp() * 1000)
	minTimestamp := now - tb.s.retentionMsecs
	maxTimestamp := now + 2*24*3600*1000 // allow max +2 days from now due to timezones shit :)
	if minTimestamp < 0 {
		// Negative timestamps aren't supported by the storage.
		minTimestamp = 0
	}
	if maxTimestamp < 0 {
		maxTimestamp = (1 << 63) - 1
	}
	return minTimestamp, maxTimestamp
}

func (tb *readOnlyTable) startRetentionWatcher() {
	tb.retentionWatcherWG.Add(1)
	go func() {
		tb.retentionWatcher()
		tb.retentionWatcherWG.Done()
	}()
}

func (tb *readOnlyTable) retentionWatcher() {
	d := timeutil.AddJitterToDuration(time.Minute)
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		select {
		case <-tb.stopCh:
			return
		case <-ticker.C:
		}

		minTimestamp := int64(fasttime.UnixTimestamp()*1000) - tb.s.retentionMsecs
		var ptwsDrop []*partitionWrapper
		tb.ptwsLock.Lock()
		dst := tb.ptws[:0]
		for _, ptw := range tb.ptws {
			if ptw.pt.tr.MaxTimestamp < minTimestamp {
				ptwsDrop = append(ptwsDrop, ptw)
			} else {
				dst = append(dst, ptw)
			}
		}
		tb.ptws = dst
		tb.ptwsLock.Unlock()

		if len(ptwsDrop) == 0 {
			continue
		}

		// There are partitions to drop. Drop them.

		// Remove table references from partitions, so they will be eventually
		// closed and dropped after all the pending searches are done.
		for _, ptw := range ptwsDrop {
			ptw.scheduleToDrop()
			ptw.decRef()
		}
	}
}

func (tb *readOnlyTable) startFinalDedupWatcher() {
	tb.finalDedupWatcherWG.Add(1)
	go func() {
		tb.finalDedupWatcher()
		tb.finalDedupWatcherWG.Done()
	}()
}

func (tb *readOnlyTable) finalDedupWatcher() {
	if !isDedupEnabled() {
		// Deduplication is disabled.
		return
	}
	f := func() {
		ptws := tb.GetPartitions(nil)
		defer tb.PutPartitions(ptws)
		timestamp := timestampFromTime(time.Now())
		currentPartitionName := timestampToPartitionName(timestamp)
		var ptwsToDedup []*partitionWrapper
		for _, ptw := range ptws {
			if ptw.pt.name == currentPartitionName {
				// Do not run final dedup for the current month.
				// For the current month, the samples are countinously
				// deduplicated by the background in-memory, small, and big part
				// merge tasks. See:
				// - partition.mergeParts() in paritiont.go and
				// - Block.deduplicateSamplesDuringMerge() in block.go.
				continue
			}
			if !ptw.pt.isFinalDedupNeeded() {
				// There is no need to run final dedup for the given partition.
				continue
			}
			// mark partition with final deduplication marker
			ptw.pt.isDedupScheduled.Store(true)
			ptwsToDedup = append(ptwsToDedup, ptw)
		}
		for _, ptw := range ptwsToDedup {
			if err := ptw.pt.runFinalDedup(tb.stopCh); err != nil {
				logger.Errorf("cannot run final dedup for partition %s: %s", ptw.pt.name, err)
			}
			ptw.pt.isDedupScheduled.Store(false)
		}
	}

	// adds 25% jitter in order to prevent thundering herd problem
	// https://github.com/VictoriaMetrics/VictoriaMetrics/issues/7880
	addJitter := func(d time.Duration) time.Duration {
		dv := d / 4
		p := float64(fastrand.Uint32()) / (1 << 32)
		return d + time.Duration(p*float64(dv))
	}
	d := addJitter(finalDedupScheduleInterval)
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-tb.stopCh:
			return
		case <-t.C:
			f()
		}
	}
}

// GetPartitions appends tb's partitions snapshot to dst and returns the result.
//
// The returned partitions must be passed to PutPartitions
// when they no longer needed.
func (tb *readOnlyTable) GetPartitions(dst []*partitionWrapper) []*partitionWrapper {
	tb.ptwsLock.Lock()
	for _, ptw := range tb.ptws {
		ptw.incRef()
		dst = append(dst, ptw)
	}
	tb.ptwsLock.Unlock()

	return dst
}

// PutPartitions deregisters ptws obtained via GetPartitions.
func (tb *readOnlyTable) PutPartitions(ptws []*partitionWrapper) {
	for _, ptw := range ptws {
		ptw.decRef()
	}
}

func mustOpenPartitionsReadOnly(smallPartitionsPath, bigPartitionsPath string, s *ReadOnlyStorage) []*partition {
	// Certain partition directories in either `big` or `small` dir may be missing
	// after restoring from backup. So populate partition names from both dirs.
	ptNames := make(map[string]bool)
	mustPopulatePartitionNames(smallPartitionsPath, ptNames)
	mustPopulatePartitionNames(bigPartitionsPath, ptNames)
	var pts []*partition
	var ptsLock sync.Mutex

	// Open partitions in parallel. This should reduce the time needed for opening multiple partitions.
	var wg sync.WaitGroup
	concurrencyLimiterCh := make(chan struct{}, cgroup.AvailableCPUs())
	for ptName := range ptNames {
		wg.Add(1)
		concurrencyLimiterCh <- struct{}{}
		go func(ptName string) {
			defer func() {
				<-concurrencyLimiterCh
				wg.Done()
			}()

			smallPartsPath := filepath.Join(smallPartitionsPath, ptName)
			bigPartsPath := filepath.Join(bigPartitionsPath, ptName)
			pt, err := mustOpenPartitionReadOnly(smallPartsPath, bigPartsPath, s)
			if err != nil {
				logger.Warnf("failed to open partition read only: %w", err)
				return
			}

			ptsLock.Lock()
			pts = append(pts, pt)
			ptsLock.Unlock()
		}(ptName)
	}
	wg.Wait()

	return pts
}
