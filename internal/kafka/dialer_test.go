package kafka

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestRewriteDialerRewritesAdvertisedAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	dialer := NewRewriteDialer(
		"kafka.internal:9093=127.0.0.1:"+port(listener),
		2*time.Second,
	)

	conn, err := dialer.Dial("tcp", "kafka.internal:9093")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if conn.RemoteAddr().String() != listener.Addr().String() {
		t.Errorf("remote = %q, want %q",
			conn.RemoteAddr(), listener.Addr())
	}
}

func TestRewriteDialerDialContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	dialer := NewRewriteDialer(
		"adv:9093=127.0.0.1:"+port(listener),
		2*time.Second,
	)

	rd, ok := dialer.(*rewriteDialer)
	if !ok {
		t.Fatal("dialer type mismatch")
	}

	conn, err := rd.DialContext(
		context.Background(),
		"tcp",
		"adv:9093",
	)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	defer conn.Close()
}

func TestRewriteDialerUnknownAddressFails(t *testing.T) {
	dialer := NewRewriteDialer("", time.Second)

	// No rule and an unreachable address must fail.
	if _, err := dialer.Dial("tcp", "nonexistent.invalid:9092"); err == nil {
		t.Error("expected dial error")
	}
}

func TestRewriteDialerParsesMultipleRules(t *testing.T) {
	dialer := NewRewriteDialer(
		"a:1=b:2, c:3=d:4 ,badpair,=nokey",
		time.Second,
	)

	rd, ok := dialer.(*rewriteDialer)
	if !ok {
		t.Fatal("dialer type mismatch")
	}
	if rd.rules["a:1"] != "b:2" {
		t.Errorf("rules a:1 = %q", rd.rules["a:1"])
	}
	if rd.rules["c:3"] != "d:4" {
		t.Errorf("rules c:3 = %q", rd.rules["c:3"])
	}
	if len(rd.rules) != 2 {
		t.Errorf("rules = %v, want 2 entries", rd.rules)
	}
}

func TestApplyRewrite(t *testing.T) {
	cfg := sarama.NewConfig()
	applyRewrite(cfg, "")
	if cfg.Net.Proxy.Enable {
		t.Error("empty spec must not enable proxy")
	}

	cfg2 := sarama.NewConfig()
	applyRewrite(cfg2, "a:1=b:2")
	if !cfg2.Net.Proxy.Enable {
		t.Error("non-empty spec must enable proxy")
	}
	if cfg2.Net.Proxy.Dialer == nil {
		t.Error("dialer must be set")
	}
}

func TestTopicsAll(t *testing.T) {
	t1 := Topics{
		Requests:  "llm.requests",
		Events:    "llm.events",
		Completed: "llm.completed",
		Failed:    "llm.failed",
		DLQ:       "llm.dlq",
	}

	all := t1.All()

	if len(all) != 5 {
		t.Fatalf("topics = %d, want 5", len(all))
	}
	want := []string{
		"llm.requests", "llm.events", "llm.completed", "llm.failed",
		"llm.dlq",
	}
	for i, name := range want {
		if all[i] != name {
			t.Errorf("topics[%d] = %q, want %q", i, all[i], name)
		}
	}
}

func port(l net.Listener) string {
	addr := l.Addr().(*net.TCPAddr)
	return strconv.Itoa(addr.Port)
}
