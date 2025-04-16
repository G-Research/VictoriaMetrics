package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage/servers"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/pushmetrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

var (
	storageDataPath = flag.String("storageDataPath", "vmstorage-data", "Path to storage data")
	cacheDataPath   = flag.String("cacheDataPath", "", "Optional path the cache directory. Defaults to [storageDataPath]/cache")
	vmselectAddr    = flag.String("vmselectAddr", ":8402", "TCP address to accept connections from vmselect services")
)

func main() {
	fmt.Println(os.Args)
	flag.CommandLine.SetOutput(os.Stdout)
	// TODO: flag.Usage = usage
	envflag.Parse()
	buildinfo.Init()
	logger.Init()

	strg := storage.MustOpenStorageReadOnly(*storageDataPath, *cacheDataPath)
	vmselectSrv, err := servers.NewVMSelectServer(*vmselectAddr, strg)
	if err != nil {
		logger.Fatalf("cannot create a server with -vmselectAddr=%s: %s", *vmselectAddr, err)
	}

	sig := procutil.WaitForSigterm()
	logger.Infof("service received signal %s", sig)
	pushmetrics.Stop()

	vmselectSrv.MustStop()
	strg.CloseReadOnly()
}
