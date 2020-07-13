package collector

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	readStats func() ([]byte, error)
)

type Collector struct {
	readStats func() ([]byte, error)

	connections *prometheus.Desc
	inPkts      *prometheus.Desc
	outPkts     *prometheus.Desc
	inBytes     *prometheus.Desc
	outBytes    *prometheus.Desc
}

func New() prometheus.Collector {
	return &Collector{
		connections: prometheus.NewDesc("dpdk_conn", "connections of this address",
			[]string{"protocol", "local_addr", "remote_addr"}, nil),
		inPkts: prometheus.NewDesc("dpdk_in_packets", "packets received",
			[]string{"protocol", "local_addr", "remote_addr"}, nil),
		outPkts: prometheus.NewDesc("dpdk_out_packets", "packets send",
			[]string{"protocol", "local_addr", "remote_addr"}, nil),
		inBytes: prometheus.NewDesc("dpdk_in_bytes", "bytes received",
			[]string{"protocol", "local_addr", "remote_addr"}, nil),
		outBytes: prometheus.NewDesc("dpdk_out_bytes", "bytes send",
			[]string{"protocol", "local_addr", "remote_addr"}, nil),
	}
}

func (c *Collector) Describe(descs chan<- *prometheus.Desc) {
	descs <- c.connections
	descs <- c.inPkts
	descs <- c.outPkts
	descs <- c.inBytes
	descs <- c.outBytes
}

func (c *Collector) Collect(metrics chan<- prometheus.Metric) {
	data, err := readStats()
	if err != nil {
		fmt.Println("read stats failed")
	}

	n := 0
	var localAddress string
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
			localAddress = addr
		}

		metrics <- prometheus.MustNewConstMetric(c.connections, prometheus.GaugeValue, float64(conns), localAddress, addr)
		metrics <- prometheus.MustNewConstMetric(c.inPkts, prometheus.CounterValue, float64(inPkts), localAddress, addr)
		metrics <- prometheus.MustNewConstMetric(c.outPkts, prometheus.CounterValue, float64(outPkts), localAddress, addr)
		metrics <- prometheus.MustNewConstMetric(c.inBytes, prometheus.CounterValue, float64(inBytes), localAddress, addr)
		metrics <- prometheus.MustNewConstMetric(c.outBytes, prometheus.CounterValue, float64(outBytes), localAddress, addr)
	}
}

func parseLine(text string) (string, int64, int64, int64, int64, int64, error) {
	fields := strings.Fields(text)
	if len(fields) != 7 {
		return "", 0, 0, 0, 0, 0, errors.Errorf("split stats line failed")
	}

	addr := fields[1]
	conns, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return "", 0, 0, 0, 0, 0, err
	}

	inPkts, err := strconv.ParseInt(fields[3], 10, 64)
	if err != nil {
		return "", 0, 0, 0, 0, 0, nil
	}

	outPkts, err := strconv.ParseInt(fields[4], 10, 64)
	if err != nil {
		return "", 0, 0, 0, 0, 0, nil
	}

	inBytes, err := strconv.ParseInt(fields[5], 10, 64)
	if err != nil {
		return "", 0, 0, 0, 0, 0, nil
	}

	outBytes, err := strconv.ParseInt(fields[6], 10, 64)
	if err != nil {
		return "", 0, 0, 0, 0, 0, nil
	}

	return addr, conns, inPkts, outPkts, inBytes, outBytes, nil
}
