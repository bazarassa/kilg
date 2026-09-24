package protocol

import "encoding/json"

// SwaggerChatMessage описывает ChatMessage для OpenAPI.
//
// Реальный ChatMessage использует json.RawMessage, потому что OpenAI-compatible
// API допускает различные формы content. Swag не умеет корректно раскрывать
// json.RawMessage, поэтому для документации используется отдельная модель.
type SwaggerChatMessage struct {
	Role    string `json:"role" example:"user"`
	Content string `json:"content" example:"Расскажи историю про программиста и кота"`
	Name    string `json:"name,omitempty" example:"user"`
}

// SwaggerChatRequest описывает входящий OpenAI-compatible request.
//
// Это исключительно документационная модель. Runtime продолжает использовать
// ChatRequest с json.RawMessage и Extra.
type SwaggerChatRequest struct {
	Model               string               `json:"model" example:"heavy"`
	Messages            []SwaggerChatMessage `json:"messages"`
	Stream              bool                 `json:"stream,omitempty" example:"false"`
	Temperature         *float64             `json:"temperature,omitempty" example:"0.7"`
	TopP                *float64             `json:"top_p,omitempty" example:"0.9"`
	MaxTokens           *int                 `json:"max_tokens,omitempty" example:"1024"`
	MaxCompletionTokens *int                 `json:"max_completion_tokens,omitempty" example:"1024"`
	Stop                string               `json:"stop,omitempty"`
	PresencePenalty     *float64             `json:"presence_penalty,omitempty" example:"0"`
	FrequencyPenalty    *float64             `json:"frequency_penalty,omitempty" example:"0"`
}

// ChatMessage is an OpenAI-compatible chat message. Extra provider-specific
// fields are preserved via Extra.
type ChatMessage struct {
	Role    string                     `json:"role"`
	Content json.RawMessage            `json:"content,omitempty"`
	Name    string                     `json:"name,omitempty"`
	Extra   map[string]json.RawMessage `json:"-"`
}

// UnmarshalJSON preserves unknown fields.
func (m *ChatMessage) UnmarshalJSON(b []byte) error {
	type alias ChatMessage
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*m = ChatMessage(a)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return err
	}
	m.Extra = map[string]json.RawMessage{}
	for k, v := range fields {
		switch k {
		case "role", "content", "name":
		default:
			m.Extra[k] = v
		}
	}
	return nil
}

// MarshalJSON emits known and preserved unknown fields.
func (m ChatMessage) MarshalJSON() ([]byte, error) {
	type alias ChatMessage
	base, err := json.Marshal(alias(m))
	if err != nil {
		return nil, err
	}
	if len(m.Extra) == 0 {
		return base, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(base, &obj); err != nil {
		return nil, err
	}
	for k, v := range m.Extra {
		if _, ok := obj[k]; !ok {
			obj[k] = v
		}
	}
	return json.Marshal(obj)
}

// ChatRequest is an OpenAI-compatible chat completion request.
// Unknown top-level fields are preserved in Extra so provider-specific
// parameters are not lost when forwarding to upstream providers.
type ChatRequest struct {
	Model               string                     `json:"model"`
	Messages            []ChatMessage              `json:"messages"`
	Stream              bool                       `json:"stream,omitempty"`
	Temperature         *float64                   `json:"temperature,omitempty"`
	TopP                *float64                   `json:"top_p,omitempty"`
	MaxTokens           *int                       `json:"max_tokens,omitempty"`
	MaxCompletionTokens *int                       `json:"max_completion_tokens,omitempty"`
	Stop                json.RawMessage            `json:"stop,omitempty"`
	PresencePenalty     *float64                   `json:"presence_penalty,omitempty"`
	FrequencyPenalty    *float64                   `json:"frequency_penalty,omitempty"`
	ResponseFormat      json.RawMessage            `json:"response_format,omitempty"`
	StreamOptions       json.RawMessage            `json:"stream_options,omitempty"`
	Extra               map[string]json.RawMessage `json:"-"`
}

// UnmarshalJSON preserves unknown fields.
func (r *ChatRequest) UnmarshalJSON(b []byte) error {
	type alias ChatRequest
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*r = ChatRequest(a)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return err
	}
	r.Extra = map[string]json.RawMessage{}
	for k, v := range fields {
		switch k {
		case "model", "messages", "stream", "temperature", "top_p",
			"max_tokens", "max_completion_tokens", "stop",
			"presence_penalty", "frequency_penalty", "response_format",
			"stream_options":
		default:
			r.Extra[k] = v
		}
	}
	return nil
}

// MarshalJSON emits known and preserved unknown fields.
func (r ChatRequest) MarshalJSON() ([]byte, error) {
	type alias ChatRequest
	base, err := json.Marshal(alias(r))
	if err != nil {
		return nil, err
	}
	if len(r.Extra) == 0 {
		return base, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(base, &obj); err != nil {
		return nil, err
	}
	for k, v := range r.Extra {
		if _, ok := obj[k]; !ok {
			obj[k] = v
		}
	}
	return json.Marshal(obj)
}

// ChatResponse is a non-streaming OpenAI-compatible response.
type ChatResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

// Choice is one completion choice.
type Choice struct {
	Index        int         `json:"index"`
	Message      ChatMessage `json:"message"`
	FinishReason *string     `json:"finish_reason"`
}

// Usage reports token accounting.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ChatChunk is a streaming OpenAI-compatible chunk.
type ChatChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	Usage   *Usage        `json:"usage,omitempty"`
}

// ChunkChoice is a streaming choice with a delta.
type ChunkChoice struct {
	Index        int         `json:"index"`
	Delta        ChatMessage `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

// ModelInfo is an entry in /v1/models.
type ModelInfo struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// ModelsList is the /v1/models response.
type ModelsList struct {
	Object string      `json:"object"`
	Data   []ModelInfo `json:"data"`
}

// JobInfo is the async job status response.
type JobInfo struct {
	ID           string `json:"id"`
	Object       string `json:"object"`
	Status       string `json:"status"`
	Provider     string `json:"provider,omitempty"`
	Model        string `json:"model,omitempty"`
	Sequence     uint64 `json:"sequence"`
	CreatedAt    string `json:"created_at,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
	FinishReason string `json:"finish_reason,omitempty"`
	Error        string `json:"error,omitempty"`
}

// JobAccepted is the response for async job creation.
type JobAccepted struct {
	ID     string `json:"id"`
	Object string `json:"object"`
	Status string `json:"status"`
}
