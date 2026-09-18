package providers

import "testing"

func TestParseChunkContent(t *testing.T) {
	info := ParseChunk(`{"choices":[{"delta":{"content":"Hello"}}]}`)
	if info.Content != "Hello" {
		t.Errorf("content = %q", info.Content)
	}
}

func TestParseChunkReasoning(t *testing.T) {
	info := ParseChunk(`{"choices":[{"delta":{"reasoning_content":"thinking hard"}}]}`)
	if info.Reasoning != "thinking hard" {
		t.Errorf("reasoning = %q", info.Reasoning)
	}
}

func TestParseChunkFinishReason(t *testing.T) {
	info := ParseChunk(`{"choices":[{"delta":{},"finish_reason":"stop"}]}`)
	if info.FinishReason != "stop" {
		t.Errorf("finish_reason = %q", info.FinishReason)
	}
}

func TestParseChunkUsage(t *testing.T) {
	info := ParseChunk(`{"choices":[{"delta":{"content":"x"}}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`)
	if info.Usage == nil {
		t.Error("usage should be present")
	}
}

func TestParseChunkInvalid(t *testing.T) {
	info := ParseChunk(`not json`)
	if info.Content != "" || info.Reasoning != "" {
		t.Errorf("expected empty info for invalid json: %+v", info)
	}
}
