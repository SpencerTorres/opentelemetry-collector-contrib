package clickhouseexporter

import (
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
)

var (
	once          sync.Once
	cachedVersion string
)

func getCollectorVersion() string {
	once.Do(func() {
		osInformation := runtime.GOOS[:3] + "-" + runtime.GOARCH
		unknownVersion := "unknown-" + osInformation

		info, ok := debug.ReadBuildInfo()
		if !ok {
			cachedVersion = unknownVersion
			return
		}

		for _, mod := range info.Deps {
			if mod.Path == "go.opentelemetry.io/collector" {
				// Extract the semantic version without metadata.
				semVer := strings.SplitN(mod.Version, "-", 2)[0]
				cachedVersion = semVer + "-" + osInformation
				return
			}
		}

		cachedVersion = unknownVersion
	})

	return cachedVersion
}
