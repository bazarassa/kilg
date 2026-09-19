package kafka

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"golang.org/x/net/proxy"
)

// rewriteDialer rewrites advertised broker addresses to actual reachable
// addresses. This handles the common case where a broker advertises an
// internal address/port that is not reachable from the client network.
type rewriteDialer struct {
	inner *net.Dialer
	rules map[string]string // advertised -> actual
}

// NewRewriteDialer builds a dialer from a comma-separated list of
// "advertised=actual" pairs.
func NewRewriteDialer(spec string, timeout time.Duration) proxy.Dialer {
	rules := map[string]string{}
	for _, pair := range strings.Split(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		if i := strings.Index(pair, "="); i > 0 {
			rules[pair[:i]] = pair[i+1:]
		}
	}
	return &rewriteDialer{
		inner: &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second},
		rules: rules,
	}
}

func (d *rewriteDialer) Dial(network, addr string) (net.Conn, error) {
	if actual, ok := d.rules[addr]; ok {
		addr = actual
	}
	return d.inner.Dial(network, addr)
}

// DialContext is used when the dialer is wrapped for context support.
func (d *rewriteDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if actual, ok := d.rules[addr]; ok {
		addr = actual
	}
	return d.inner.DialContext(ctx, network, addr)
}

// applyRewrite wires the rewrite dialer into a sarama config when spec is
// non-empty.
func applyRewrite(cfg *sarama.Config, spec string) {
	if spec == "" {
		return
	}
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = NewRewriteDialer(spec, cfg.Net.DialTimeout)
}
