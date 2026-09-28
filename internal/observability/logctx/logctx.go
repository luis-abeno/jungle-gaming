package logctx

import (
	"context"
	"log/slog"
)

type key struct{}

func With(ctx context.Context, kv ...string) context.Context {
	prev, _ := ctx.Value(key{}).([]slog.Attr)
	attrs := make([]slog.Attr, len(prev), len(prev)+len(kv)/2)
	copy(attrs, prev)
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i+1] != "" {
			attrs = append(attrs, slog.String(kv[i], kv[i+1]))
		}
	}
	return context.WithValue(ctx, key{}, attrs)
}

func attrs(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}
	a, _ := ctx.Value(key{}).([]slog.Attr)
	return a
}

type Handler struct {
	slog.Handler
}

func (h Handler) Handle(ctx context.Context, r slog.Record) error {
	if a := attrs(ctx); len(a) > 0 {
		r.AddAttrs(a...)
	}
	return h.Handler.Handle(ctx, r)
}

func (h Handler) WithAttrs(a []slog.Attr) slog.Handler { return Handler{h.Handler.WithAttrs(a)} }
func (h Handler) WithGroup(name string) slog.Handler   { return Handler{h.Handler.WithGroup(name)} }
