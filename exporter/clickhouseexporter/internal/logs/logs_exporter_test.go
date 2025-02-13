package logs

import (
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"testing"
)

func testMap() pcommon.Map {
	attrs := pcommon.NewMap()

	attrs.PutStr("app", "telemetry-service")
	attrs.PutInt("process.pid", 12345)
	attrs.PutBool("debug.enabled", true)

	serverInfo := attrs.PutEmptyMap("server.info")
	serverInfo.PutStr("hostname", "prod-server-01")
	serverInfo.PutDouble("uptime", 156.7)
	serverMetadata := serverInfo.PutEmptyMap("metadata")
	serverMetadata.PutStr("region", "us-west-2")
	serverMetadata.PutStr("tier", "premium")

	userRoles := attrs.PutEmptySlice("user.roles")
	userRoles.AppendEmpty().SetStr("admin")
	userRoles.AppendEmpty().SetStr("editor")
	userRoles.AppendEmpty().SetStr("viewer")

	systemMetrics := attrs.PutEmptySlice("system.metrics")
	systemMetrics.AppendEmpty().SetDouble(0.75)
	systemMetrics.AppendEmpty().SetDouble(0.83)
	systemMetrics.AppendEmpty().SetDouble(0.3335)

	resourceUsage := attrs.PutEmptyMap("resource.usage")
	cpu := resourceUsage.PutEmptyMap("cpu")
	cpu.PutInt("cores", 8)
	cpuLoads := cpu.PutEmptySlice("loads")
	cpuLoads.AppendEmpty().SetDouble(1.5)
	cpuLoads.AppendEmpty().SetDouble(2.0)
	cpuLoads.AppendEmpty().SetDouble(1.8)

	memory := resourceUsage.PutEmptyMap("memory")
	memory.PutInt("total", 16384)
	memory.PutInt("used", 8192)
	memAllocs := memory.PutEmptySlice("allocations")
	memAllocs.AppendEmpty().SetInt(1024)
	memAllocs.AppendEmpty().SetInt(2048)
	memAllocs.AppendEmpty().SetInt(4096)

	return attrs
}

func TestAttributesToJSON(t *testing.T) {
	m := testMap()
	jb := JSONBuffer{buf: make([]byte, 0, 1024)}
	attributesToJSON(&jb, m)

	actual := string(jb.Bytes())
	expected := `{"app":"telemetry-service","process.pid":12345,"debug.enabled":true,"server.info":{"hostname":"prod-server-01","uptime":156.7,"metadata":{"region":"us-west-2","tier":"premium"}},"user.roles":["admin","editor","viewer"],"system.metrics":[0.75,0.83,0.3335],"resource.usage":{"cpu":{"cores":8,"loads":[1.5,2,1.8]},"memory":{"total":16384,"used":8192,"allocations":[1024,2048,4096]}}}`
	require.Equal(t, expected, actual)
}

func BenchmarkAttributesToJSON(b *testing.B) {
	b.ReportAllocs()

	m := testMap()
	jb := JSONBuffer{buf: make([]byte, 0, 1024)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jb.Reset()
		attributesToJSON(&jb, m)
	}
}
