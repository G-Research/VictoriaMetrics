package panicutil

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

func ToError(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("panic: %v", r)
			}
		}
	}()

	return f()
}

func ToErrorWithRetry(tries int, f func() error) (int, error) {
	var errs []error
	for t := range tries {
		err := ToError(f)
		if err == nil {
			return t, nil
		}
		logger.Errorf("error on try %d: %v", t, err)
		// Check for context cancellation to handle graceful shutdowns.
		if errors.Is(err, context.Canceled) {
			return t, fmt.Errorf("operation canceled after %d tries: %w", t, err)
		}
		errs = append(errs, err)
		time.Sleep(100 * time.Millisecond)
	}
	return tries, fmt.Errorf("failed after %d tries: %v", tries, errors.Join(errs...))
}
