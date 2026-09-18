// Package streaming implements the SSE response writer, the per-request
// dispatcher and the bounded replay buffer.
package streaming

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
)

// SSEHeaders sets the standard SSE response headers.
func SSEHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
}

// WriteEvent writes one SSE event. If id is non-empty an "id:" line is
// included so clients can use Last-Event-ID for replay.
func WriteEvent(w http.ResponseWriter, flusher http.Flusher, id string, data string) error {
	var b strings.Builder
	if id != "" {
		fmt.Fprintf(&b, "id: %s\n", id)
	}
	for _, line := range strings.Split(data, "\n") {
		fmt.Fprintf(&b, "data: %s\n", line)
	}
	b.WriteString("\n")
	if _, err := w.Write([]byte(b.String())); err != nil {
		return err
	}
	if flusher != nil {
		flusher.Flush()
	}
	return nil
}

// WriteDone writes the terminal [DONE] marker.
func WriteDone(w http.ResponseWriter, flusher http.Flusher) error {
	return WriteEvent(w, flusher, "", "[DONE]")
}

// ParseSSE splits a raw SSE byte stream into data payloads. It understands
// multi-line data fields and ignores comments/other event fields.
func ParseSSE(r *bufio.Reader) (data string, err error) {
	var datas []string
	for {
		line, err := r.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				if len(datas) > 0 {
					return strings.Join(datas, "\n"), err
				}
				if err != nil {
					return "", err
				}
				continue
			}
			if strings.HasPrefix(line, "data:") {
				d := strings.TrimPrefix(line, "data:")
				d = strings.TrimPrefix(d, " ")
				datas = append(datas, d)
			}
			// "event:", "id:", ":comment" lines are ignored.
		}
		if err != nil {
			if len(datas) > 0 {
				return strings.Join(datas, "\n"), err
			}
			return "", err
		}
	}
}
