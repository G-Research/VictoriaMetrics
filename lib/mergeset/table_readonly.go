package mergeset

import (
	"path/filepath"
	"sync"
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
func MustOpenTableReadOnly(path string) *ReadOnlyTable {
	path = filepath.Clean(path)

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
