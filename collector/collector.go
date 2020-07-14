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

	connections *prometheus.Desc
	inPkts      *prometheus.Desc
	outPkts     *prometheus.Desc
	inBytes     *prometheus.Desc
	outBytes    *prometheus.Desc
}

func New() prometheus.Collector {
	return &Collector{
		connections: prometheus.NewDesc("ipvs_conn", "connections of this address",
			[]string{"protocol", "local_addr", "remote_addr"}, nil),
		inPkts: prometheus.NewDesc("ipvs_in_packets", "packets received",
			[]string{"protocol", "local_addr", "remote_addr"}, nil),
		outPkts: prometheus.NewDesc("ipvs_out_packets", "packets send",
			[]string{"protocol", "local_addr", "remote_addr"}, nil),
		inBytes: prometheus.NewDesc("ipvs_in_bytes", "bytes received",
			[]string{"protocol", "local_addr", "remote_addr"}, nil),
		outBytes: prometheus.NewDesc("ipvs_out_bytes", "bytes send",
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

		if addr == "" {
			panic(line)
		}

		if strings.HasPrefix(line, "TCP") {
			protocol = "TCP"
			localAddress = addr
		}

		metrics <- prometheus.MustNewConstMetric(c.connections, prometheus.GaugeValue, float64(conns), protocol, localAddress, addr)
		metrics <- prometheus.MustNewConstMetric(c.inPkts, prometheus.CounterValue, float64(inPkts), protocol, localAddress, addr)
		metrics <- prometheus.MustNewConstMetric(c.outPkts, prometheus.CounterValue, float64(outPkts), protocol, localAddress, addr)
		metrics <- prometheus.MustNewConstMetric(c.inBytes, prometheus.CounterValue, float64(inBytes), protocol, localAddress, addr)
		metrics <- prometheus.MustNewConstMetric(c.outBytes, prometheus.CounterValue, float64(outBytes), protocol, localAddress, addr)
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
