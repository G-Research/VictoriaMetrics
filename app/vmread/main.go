package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmread/servers"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/pushmetrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

var (
	storageDataPath    = flag.String("storageDataPath", "vmstorage-data", "Path to storage data")
	cacheDataPath      = flag.String("cacheDataPath", "", "Optional path the cache directory. Defaults to [storageDataPath]/cache")
	vmselectAddr       = flag.String("vmselectAddr", ":8402", "TCP address to accept connections from vmselect services")
	retentionPeriod    = flagutil.NewRetentionDuration("retentionPeriod", "1", "Data with timestamps outside the retentionPeriod is automatically deleted. The minimum retentionPeriod is 24h or 1d. See also -retentionFilter")
	disablePerDayIndex = flag.Bool("disablePerDayIndex", false, "Disable per-day index and use global index for all searches. "+
		"This may improve performance and decrease disk space usage for the use cases with fixed set of timeseries scattered across a "+
		"big time range (for example, when loading years of historical data). "+
		"See https://docs.victoriametrics.com/single-server-victoriametrics/#index-tuning")
)

func main() {
	fmt.Println(os.Args)
	flag.CommandLine.SetOutput(os.Stdout)
	// TODO: flag.Usage = usage
	envflag.Parse()
	buildinfo.Init()
	logger.Init()

	if retentionPeriod.Duration() < 24*time.Hour {
		logger.Fatalf("-retentionPeriod cannot be smaller than a day; got %s", retentionPeriod)
	}
	logger.Infof("opening storage at %q with -retentionPeriod=%s", *storageDataPath, retentionPeriod)

	if !fs.IsPathExist(*storageDataPath) {
		logger.Panicf("storage path %q must exist when vmread is created", *storageDataPath)
	}

	readOnlyStorage := storage.NewReadOnlyStorage(&storage.ReadOnlyConfig{
		Retention:          retentionPeriod.Duration(),
		CachePath:          *cacheDataPath,
		StoragePath:        *storageDataPath,
		DisablePerDayIndex: *disablePerDayIndex,
	})
	vmselectSrv, err := servers.NewVMSelectServer(*vmselectAddr, readOnlyStorage)
	if err != nil {
		logger.Fatalf("cannot create a server with -vmselectAddr=%s: %s", *vmselectAddr, err)
	}

	sig := procutil.WaitForSigterm()
	logger.Infof("service received signal %s", sig)
	pushmetrics.Stop()

	vmselectSrv.MustStop()
}
