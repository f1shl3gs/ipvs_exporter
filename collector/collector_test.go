package collector

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestParseline(t *testing.T) {
	line := `TCP  10.33.184.4:3306                    2      164     6560      100     4000`
	addr, conns, inPkts, outPkts, inBytes, outBytes, err := parseStats(line)
	require.NoError(t, err)

	require.EqualValues(t, "10.33.184.4:3306", addr)
	require.EqualValues(t, 2, conns)
	require.EqualValues(t, 164, inPkts)
	require.EqualValues(t, 6560, outPkts)
	require.EqualValues(t, 100, inBytes)
	require.EqualValues(t, 4000, outBytes)
}

func TestReadStats(t *testing.T) {
	readStats = func(ctx context.Context) ([]byte, error) {
		output := "IP Virtual Server version 1.2.1 (size=4096)\nProt LocalAddress:Port               Conns   InPkts  OutPkts  InBytes OutBytes\n  -> RemoteAddress:Port\nTCP  207.175.44.110:80                   0        0        0        0        0\n  -> 192.168.10.1:80                     0        0        0        0        0"
		return []byte(output), nil
	}

	readList = func(ctx context.Context) ([]byte, error) {
		output := "IP Virtual Server version 1.2.1 (size=4096)\nProt LocalAddress:Port Scheduler Flags\n  -> RemoteAddress:Port           Forward Weight ActiveConn InActConn\nTCP  207.175.44.110:80 rr\n  -> 192.168.10.1:80              FullNat 1      0          0"
		return []byte(output), nil
	}

	metricCh := make(chan prometheus.Metric, 100)
	collector := New()

	collector.Collect(metricCh)
	close(metricCh)

	for metric := range metricCh {
		fmt.Println(metric.Desc())
	}
}
