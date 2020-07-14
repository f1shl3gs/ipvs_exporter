package main

import (
	"net/http"

	"github.com/f1shl3gs/ipvsadm_exporter/collector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	reg := prometheus.NewRegistry()

	reg.MustRegister(collector.New())

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	http.ListenAndServe("localhost:5382", nil)
}
