package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmread/servers"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/pushmetrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

var (
	storageDataPath  = flag.String("storageDataPath", "vmstorage-data", "Path to storage data")
	cacheDataPath    = flag.String("cacheDataPath", "", "Optional path the cache directory. Defaults to [storageDataPath]/cache")
	vmselectAddr     = flag.String("vmselectAddr", ":8402", "TCP address to accept connections from vmselect services")
	httpListenAddrs  = flagutil.NewArrayString("httpListenAddr", "Address to listen for incoming http requests. See also -httpListenAddr.useProxyProtocol")
	useProxyProtocol = flagutil.NewArrayBool("httpListenAddr.useProxyProtocol", "Whether to use proxy protocol for connections accepted at the given -httpListenAddr . "+
		"See https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt . "+
		"With enabled proxy protocol http server cannot serve regular /metrics endpoint. Use -pushmetrics.url for metrics pushing")
	forceMergeAuthKey  = flagutil.NewPassword("forceMergeAuthKey", "authKey, which must be passed in query string to /internal/force_merge pages")
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

	listenAddrs := *httpListenAddrs
	if len(listenAddrs) == 0 {
		listenAddrs = []string{":8483"}
	}
	requestHandler := newRequestHandler(readOnlyStorage)
	go httpserver.Serve(listenAddrs, requestHandler, httpserver.ServeOptions{UseProxyProtocol: useProxyProtocol})

	sig := procutil.WaitForSigterm()
	logger.Infof("service received signal %s", sig)
	pushmetrics.Stop()

	vmselectSrv.MustStop()
}

func newRequestHandler(strg *storage.ReadOnlyStorage) httpserver.RequestHandler {
	return func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/" {
			if r.Method != http.MethodGet {
				return false
			}
			w.Header().Add("Content-Type", "text/html; charset=utf-8")
			fmt.Fprintf(w, `vmstorage - a component of VictoriaMetrics cluster<br/>
			<a href="https://docs.victoriametrics.com/cluster-victoriametrics/">docs</a><br>
`)
			return true
		}
		return requestHandler(w, r, strg)
	}
}

func requestHandler(w http.ResponseWriter, r *http.Request, strg *storage.ReadOnlyStorage) bool {
	return true
}
