package collector

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	readStats = func() ([]byte, error) {
		return ioutil.ReadFile("collector/stats.txt")
	}
)

type Collector struct {
	readStats func() ([]byte, error)

	vipConnections *prometheus.Desc
	vipInPkts      *prometheus.Desc
	vipOutPkts     *prometheus.Desc
	vipInBytes     *prometheus.Desc
	vipOutBytes    *prometheus.Desc

	backendConnections *prometheus.Desc
	backendInPkts      *prometheus.Desc
	backendOutPkts     *prometheus.Desc
	backendInBytes     *prometheus.Desc
	backendOutBytes    *prometheus.Desc
}

func New() prometheus.Collector {
	return &Collector{
		vipConnections: prometheus.NewDesc("ipvs_virtual_server_conns", "connections of this address",
			[]string{"protocol", "address"}, nil),
		vipInPkts: prometheus.NewDesc("ipvs_virtual_server_in_packets", "packets received",
			[]string{"protocol", "address"}, nil),
		vipOutPkts: prometheus.NewDesc("ipvs_virtual_server_out_packets", "packets send",
			[]string{"protocol", "address"}, nil),
		vipInBytes: prometheus.NewDesc("ipvs_virtual_server_in_bytes", "bytes received",
			[]string{"protocol", "address"}, nil),
		vipOutBytes: prometheus.NewDesc("ipvs_virtual_server_out_bytes", "bytes send",
			[]string{"protocol", "address"}, nil),

		backendConnections: prometheus.NewDesc("ipvs_backend_conns", "connections of backend",
			[]string{"protocol", "vip", "address"}, nil),
		backendInPkts: prometheus.NewDesc("ipvs_backend_in_packets", "packets received",
			[]string{"protocol", "vip", "address"}, nil),
		backendOutPkts: prometheus.NewDesc("ipvs_backend_out_packets", "packets send",
			[]string{"protocol", "vip", "address"}, nil),
		backendInBytes: prometheus.NewDesc("ipvs_backend_in_bytes", "bytes received",
			[]string{"protocol", "vip", "address"}, nil),
		backendOutBytes: prometheus.NewDesc("ipvs_backend_out_bytes", "bytes send",
			[]string{"protocol", "vip", "address"}, nil),
	}
}

func (c *Collector) Describe(descs chan<- *prometheus.Desc) {
	descs <- c.vipConnections
	descs <- c.vipInPkts
	descs <- c.vipOutPkts
	descs <- c.vipInBytes
	descs <- c.vipOutBytes

	descs <- c.backendConnections
	descs <- c.backendInPkts
	descs <- c.backendOutPkts
	descs <- c.backendInBytes
	descs <- c.backendOutBytes
}

func (c *Collector) Collect(metrics chan<- prometheus.Metric) {
	data, err := readStats()
	if err != nil {
		fmt.Println("read stats failed")
	}

	n := 0
	var vip string
	var protocol string
	buf := bytes.NewBuffer(data)
	for {
		line, err := buf.ReadString('\n')
		if err == io.EOF {
			break
		}

		if err != nil {
			return
		}

		n += 1
		if n < 4 {
			continue
		}

		line = strings.TrimSpace(line)

		addr, conns, inPkts, outPkts, inBytes, outBytes, err := parseLine(line)
		if err != nil {
			continue
		}

		if strings.HasPrefix(line, "TCP") {
			protocol = "TCP"
			vip = addr

			metrics <- prometheus.MustNewConstMetric(c.vipConnections, prometheus.GaugeValue, float64(conns), protocol, addr)
			metrics <- prometheus.MustNewConstMetric(c.vipInPkts, prometheus.CounterValue, float64(inPkts), protocol, addr)
			metrics <- prometheus.MustNewConstMetric(c.vipOutPkts, prometheus.CounterValue, float64(outPkts), protocol, addr)
			metrics <- prometheus.MustNewConstMetric(c.vipInBytes, prometheus.CounterValue, float64(inBytes), protocol, addr)
			metrics <- prometheus.MustNewConstMetric(c.vipOutBytes, prometheus.CounterValue, float64(outBytes), protocol, addr)
			continue
		}

		metrics <- prometheus.MustNewConstMetric(c.backendConnections, prometheus.GaugeValue, float64(conns), protocol, vip, addr)
		metrics <- prometheus.MustNewConstMetric(c.backendInPkts, prometheus.CounterValue, float64(inPkts), protocol, vip, addr)
		metrics <- prometheus.MustNewConstMetric(c.backendOutPkts, prometheus.CounterValue, float64(outPkts), protocol, vip, addr)
		metrics <- prometheus.MustNewConstMetric(c.backendInBytes, prometheus.CounterValue, float64(inBytes), protocol, vip, addr)
		metrics <- prometheus.MustNewConstMetric(c.backendOutBytes, prometheus.CounterValue, float64(outBytes), protocol, vip, addr)
	}
}

func parseLine(text string) (string, uint64, uint64, uint64, uint64, uint64, error) {
	fields := strings.Fields(text)
	if len(fields) != 7 {
		return "", 0, 0, 0, 0, 0, errors.Errorf("split stats line failed")
	}

	addr := fields[1]
	conns, err := humanize.ParseBytes(fields[2])
	if err != nil {
		return "", 0, 0, 0, 0, 0, err
	}

	inPkts, err := humanize.ParseBytes(fields[3])
	if err != nil {
		return "", 0, 0, 0, 0, 0, err
	}

	outPkts, err := humanize.ParseBytes(fields[4])
	if err != nil {
		return "", 0, 0, 0, 0, 0, err
	}

	inBytes, err := humanize.ParseBytes(fields[5])
	if err != nil {
		return "", 0, 0, 0, 0, 0, err
	}

	outBytes, err := humanize.ParseBytes(fields[6])
	if err != nil {
		return "", 0, 0, 0, 0, 0, err
	}

	return addr, conns, inPkts, outPkts, inBytes, outBytes, nil
}
