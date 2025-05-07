package mergeset

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// ReadOnlyTable represents mergeset table.
type ReadOnlyTable struct {
	path string

	// rawItems contains recently added items that haven't been converted to parts yet.
	//
	// rawItems are converted to inmemoryParts at least every pendingItemsFlushInterval or when rawItems becomes full.
	//
	// rawItems aren't visible for search due to performance reasons.
	rawItems rawItemsShards

	// partsLock protects inmemoryParts and fileParts.
	partsLock sync.Mutex

	// inmemoryParts contains inmemory parts, which are visible for search.
	// TODO: maybe we don't need this?
	inmemoryParts []*partWrapper

	// fileParts contains file-backed parts, which are visible for search.
	fileParts []*partWrapper

	// stopCh is used for notifying all the background workers to stop.
	//
	// It must be closed under partsLock in order to prevent from calling wg.Add()
	// after stopCh is closed.
	stopCh chan struct{}
}

// MustOpenTableReadOnly opens a table on the given path.
//
// The flushInterval is the interval for flushing pending in-memory data to disk.
//
// Optional flushCallback is called every time new data batch is flushed
// to the underlying storage and becomes visible to search.
//
// Optional prepareBlock is called during merge before flushing the prepared block
// to persistent storage.
//
// The table is created if it doesn't exist yet.
func MustOpenTableReadOnly(path string, flushInterval time.Duration, flushCallback func(), prepareBlock PrepareBlockCallback, isReadOnly *atomic.Bool) *ReadOnlyTable {
	path = filepath.Clean(path)

	if flushInterval < pendingItemsFlushInterval {
		// There is no sense in setting flushInterval to values smaller than pendingItemsFlushInterval,
		// since pending rows unconditionally remain in memory for up to pendingItemsFlushInterval.
		flushInterval = pendingItemsFlushInterval
	}

	// Open table parts.
	pws := mustOpenParts(path)

	tb := &ReadOnlyTable{
		path:      path,
		fileParts: pws,
		stopCh:    make(chan struct{}),
	}
	tb.rawItems.init()

	return tb
}

// Path returns the path to tb on the filesystem.
func (tb *ReadOnlyTable) Path() string {
	return tb.path
}

// getParts appends parts snapshot to dst and returns it.
//
// The appended parts must be released with putParts.
func (tb *ReadOnlyTable) getParts(dst []*partWrapper) []*partWrapper {
	tb.partsLock.Lock()
	for _, pw := range tb.inmemoryParts {
		pw.incRef()
	}
	for _, pw := range tb.fileParts {
		pw.incRef()
	}
	dst = append(dst, tb.inmemoryParts...)
	dst = append(dst, tb.fileParts...)
	tb.partsLock.Unlock()

	return dst
}

// putParts releases the given pws obtained via getParts.
func (tb *ReadOnlyTable) putParts(pws []*partWrapper) {
	for _, pw := range pws {
		pw.decRef()
	}
}

func (tb *ReadOnlyTable) getMaxFilePartSize() uint64 {
	n := fs.MustGetFreeSpace(tb.path)
	// Divide free space by the max number of concurrent merges for file parts.
	maxOutBytes := n / uint64(cap(filePartsConcurrencyCh))
	if maxOutBytes > maxPartSize {
		maxOutBytes = maxPartSize
	}
	return maxOutBytes
}

func (tb *ReadOnlyTable) releasePartsToMerge(pws []*partWrapper) {
	tb.partsLock.Lock()
	for _, pw := range pws {
		if !pw.isInMerge {
			logger.Panicf("BUG: missing isInMerge flag on the part %q", pw.p.path)
		}
		pw.isInMerge = false
	}
	tb.partsLock.Unlock()
}

func (tb *ReadOnlyTable) openCreatedPart(pws []*partWrapper, mpNew *inmemoryPart, dstPartPath string) *partWrapper {
	// Open the created part from disk.
	pNew := mustOpenFilePart(dstPartPath)
	pwNew := &partWrapper{
		p: pNew,
	}
	pwNew.incRef()
	return pwNew
}
