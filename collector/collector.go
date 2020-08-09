package collector

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"golang.org/x/sync/errgroup"
)

var (
	readStats = func(ctx context.Context) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "ipvsadm", "-l", "--stats", "-n")
		return cmd.CombinedOutput()
	}

	readList = func(ctx context.Context) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "ipvsadm", "-l", "-n")
		return cmd.CombinedOutput()
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

	backendConnectionsActive *prometheus.Desc
	backendConnectionsInact  *prometheus.Desc
	backendWeight            *prometheus.Desc

	scrapeError    *prometheus.Desc
	scrapeDuration *prometheus.Desc
}

func New() prometheus.Collector {
	return &Collector{
		vipConnections: prometheus.NewDesc("ipvs_connections_total",
			"the total number of connections made",
			[]string{"protocol", "address"}, nil),
		vipInPkts: prometheus.NewDesc("ipvs_incoming_packets_total",
			"the total number of packets received",
			[]string{"protocol", "address"}, nil),
		vipOutPkts: prometheus.NewDesc("ipvs_outgoing_packets_total",
			"the total number of packets sent",
			[]string{"protocol", "address"}, nil),
		vipInBytes: prometheus.NewDesc("ipvs_incoming_bytes_total",
			"the total amount of incoming data",
			[]string{"protocol", "address"}, nil),
		vipOutBytes: prometheus.NewDesc("ipvs_outgoing_bytes_total",
			"the total amount of outgoing data",
			[]string{"protocol", "address"}, nil),

		backendConnections: prometheus.NewDesc("ipvs_backend_connections_total",
			"the total number of connections made",
			[]string{"protocol", "vip", "address"}, nil),
		backendInPkts: prometheus.NewDesc("ipvs_backend_incoming_packets_total",
			"the total number of incoming packets",
			[]string{"protocol", "vip", "address"}, nil),
		backendOutPkts: prometheus.NewDesc("ipvs_backend_outgoing_packets_totoal",
			"the total number fo outgoing packets",
			[]string{"protocol", "vip", "address"}, nil),
		backendInBytes: prometheus.NewDesc("ipvs_backend_incoming_bytes_total",
			"the total amount of incoming data",
			[]string{"protocol", "vip", "address"}, nil),
		backendOutBytes: prometheus.NewDesc("ipvs_backend_outgoing_bytes_total",
			"the total amount of outgoing data",
			[]string{"protocol", "vip", "address"}, nil),

		backendConnectionsActive: prometheus.NewDesc("ipvs_backend_connections_active",
			"the current active connections by local and remote address",
			[]string{"protocol", "forward", "vip", "address", "scheduler"}, nil),
		backendConnectionsInact: prometheus.NewDesc("ipvs_backend_connections_inactive",
			"the current inactive connections by local and remote addresss",
			[]string{"protocol", "forward", "vip", "address", "scheduler"}, nil),
		backendWeight: prometheus.NewDesc("ipvs_backend_weight",
			"the current backend weight by local and remote address",
			[]string{"protocol", "forward", "vip", "address", "scheduler"}, nil),

		scrapeError: prometheus.NewDesc("ipvs_collector_scrape_error",
			"0 for error and 1 for success",
			nil, nil),
		scrapeDuration: prometheus.NewDesc("ipvs_collector_scrape_duration_seconds",
			"the time spend on scrap in seconds",
			nil, nil),
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

	descs <- c.backendConnectionsActive
	descs <- c.backendConnectionsInact
	descs <- c.backendWeight

	descs <- c.scrapeError
	descs <- c.scrapeDuration
}

func (c *Collector) gatherStats(ctx context.Context, metrics chan<- prometheus.Metric) error {
	data, err := readStats(ctx)
	if err != nil {
		log.Errorf("read stats failed, err: %s", err)
		return err
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
			return errors.Wrap(err, "read stats output failed")
		}

		n += 1
		if n < 4 {
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		addr, conns, inPkts, outPkts, inBytes, outBytes, err := parseStats(line)
		if err != nil {
			log.Warnf("parse line failed, err: %s, line: %s", err, line)
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

	return nil
}

func (c *Collector) gatherList(ctx context.Context, metrics chan<- prometheus.Metric) error {
	data, err := readList(ctx)
	if err != nil {
		return err
	}

	n := 0
	var vip string
	var protocol string
	var scheduler string
	buf := bytes.NewBuffer(data)
	for {
		line, err := buf.ReadString('\n')
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		n += 1
		if n < 4 {
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "TCP") {
			fields := strings.Fields(line)
			if len(fields) < 3 {
				return errors.Errorf("3 field of proto line is expected")
			}

			protocol = fields[0]
			vip = fields[1]
			scheduler = fields[2]

			continue
		}

		addr, forward, weight, activeConn, inActiveConn, err := parseTable(line)
		if err != nil {
			return errors.Errorf("parse backend line failed, err: %s", err)
		}

		metrics <- prometheus.MustNewConstMetric(c.backendConnectionsActive,
			prometheus.GaugeValue, float64(activeConn), protocol, forward, vip, addr, scheduler)
		metrics <- prometheus.MustNewConstMetric(c.backendConnectionsInact,
			prometheus.GaugeValue, float64(inActiveConn), protocol, forward, vip, addr, scheduler)
		metrics <- prometheus.MustNewConstMetric(c.backendWeight,
			prometheus.GaugeValue, float64(weight), protocol, forward, vip, addr, scheduler)
	}

	return nil
}

func (c *Collector) Collect(metrics chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return c.gatherList(ctx, metrics)
	})

	group.Go(func() error {
		return c.gatherStats(ctx, metrics)
	})

	err := group.Wait()
	end := time.Now()
	metrics <- prometheus.MustNewConstMetric(c.scrapeDuration,
		prometheus.GaugeValue, end.Sub(start).Seconds())

	if err != nil {
		metrics <- prometheus.MustNewConstMetric(c.scrapeError,
			prometheus.GaugeValue, 1.0)
		log.Errorf("read stats failed, err: %s", err)
		return
	}

	metrics <- prometheus.MustNewConstMetric(c.scrapeError,
		prometheus.GaugeValue, 0.0)

}

func parseStats(text string) (string, uint64, uint64, uint64, uint64, uint64, error) {
	fields := strings.Fields(text)
	if len(fields) != 7 {
		return "", 0, 0, 0, 0, 0, errors.Errorf("split stats line failed, line: %q", text)
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

func parseTable(line string) (string, string, int64, int64, int64, error) {
	fields := strings.Fields(line)
	if len(fields) != 6 {
		return "", "", 0, 0, 0, errors.New("expect 6 field for table line")
	}

	addr := fields[1]
	forward := fields[2]
	weight, err := strconv.ParseInt(fields[3], 10, 64)
	if err != nil {
		return "", "", 0, 0, 0, errors.Errorf("parse Weight %s failed", fields[3])
	}

	active, err := strconv.ParseInt(fields[4], 10, 64)
	if err != nil {
		return "", "", 0, 0, 0, errors.Errorf("parse ActiveConn %q failed", fields[4])
	}

	inActive, err := strconv.ParseInt(fields[5], 10, 64)
	if err != nil {
		return "", "", 0, 0, 0, errors.Errorf("parse InActConn %q fialed", fields[5])
	}

	return addr, forward, weight, active, inActive, nil
}
