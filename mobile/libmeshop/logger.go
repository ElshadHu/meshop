package libmeshop

import (
	"io"
	"log/slog"
	"strings"
)

// slogFromLogger returns a slog.Logger whose output is forwarded to the Java-side logger
func slogFromLogger(log Logger) *slog.Logger {
	if log == nil {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return slog.New(slog.NewTextHandler(&loggerWriter{log: log}, nil))
}

type loggerWriter struct {
	log Logger
}

func (w *loggerWriter) Write(p []byte) (int, error) {
	w.log.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

var _ io.Writer = (*loggerWriter)(nil)
