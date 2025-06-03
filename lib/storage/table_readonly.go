package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
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
func mustOpenReadOnlyTable(path string, s *ReadOnlyStorage) (*readOnlyTable, error) {
	path = filepath.Clean(path)

	// Create directories for small and big partitions if they don't exist yet.
	smallPartitionsPath := filepath.Join(path, smallDirname)
	bigPartitionsPath := filepath.Join(path, bigDirname)

	// Open partitions.
	pts, err := mustOpenPartitionsReadOnly(smallPartitionsPath, bigPartitionsPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open partitions for table at %q: %w", path, err)
	}

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
	return tb, nil
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

func mustOpenPartitionsReadOnly(smallPartitionsPath, bigPartitionsPath string) ([]*partition, error) {
	pts := make([]*partition, 0)
	pts = pts[:0]
	// Certain partition directories in either `big` or `small` dir may be missing
	// after restoring from backup. So populate partition names from both dirs.
	ptNames := make(map[string]bool)
	if err := mustPopulatePartitionNames(smallPartitionsPath, ptNames); err != nil {
		return nil, fmt.Errorf("cannot populate partition names from small partitions path %q: %w", smallPartitionsPath, err)
	}
	if err := mustPopulatePartitionNames(bigPartitionsPath, ptNames); err != nil {
		return nil, fmt.Errorf("cannot populate partition names from big partitions path %q: %w", bigPartitionsPath, err)
	}
	var ptsLock sync.Mutex

	// Open partitions in parallel. This should reduce the time needed for opening multiple partitions.
	var wg sync.WaitGroup
	errCh := make(chan error, cgroup.AvailableCPUs())
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
			pt, err := mustOpenPartitionReadOnly(smallPartsPath, bigPartsPath)
			if err != nil {
				errCh <- fmt.Errorf("cannot open partition %q: %w", ptName, err)
				return
			}

			ptsLock.Lock()
			pts = append(pts, pt)
			ptsLock.Unlock()
		}(ptName)
	}
	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	return pts, errors.Join(errs...)
}
