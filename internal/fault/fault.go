package fault

import (
	"log/slog"
	"os"

	"wallet/internal/app"
)

const ExitCode = 86

func Hook(points []string, log *slog.Logger) app.FaultHook {
	if len(points) == 0 {
		return app.NoFaults
	}
	enabled := map[string]bool{}
	for _, p := range points {
		enabled[p] = true
	}
	log.Warn("fault injection enabled", "points", points)
	return func(point string) {
		if enabled[point] {
			log.Warn("fault injected, exiting", "point", point)
			os.Exit(ExitCode)
		}
	}
}
