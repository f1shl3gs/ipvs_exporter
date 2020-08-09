package main

import (
	"net/http"
	"os"

	"github.com/f1shl3gs/ipvs_exporter/collector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
)

func main() {
	reg := prometheus.NewRegistry()

	reg.MustRegister(collector.New())

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	listen := os.Getenv("IPVS_EXPORTER_LISTEN")
	if listen == "" {
		listen = ":5389"
	}

	log.Infof("start http service, listen: %q", listen)
	err := http.ListenAndServe(listen, nil)
	if err != nil {
		log.Errorf("serve http failed, err: %s", err)
		os.Exit(1)
	}
}
