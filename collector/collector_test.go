package collector

import (
	"testing"

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
