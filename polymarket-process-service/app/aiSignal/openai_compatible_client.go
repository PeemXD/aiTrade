package aisignal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/local/polymarket-process-service/pkg/httpclient"
)

type ChatClient interface {
	ChatJSON(context.Context, ChatRequest) (string, error)
}

type ChatRequest struct {
	Model       string
	System      string
	UserJSON    any
	Temperature float64
	APIKey      string
}

type OpenAICompatibleClient struct {
	baseURL string
	client  *httpclient.Client
}

func NewOpenAICompatibleClient(baseURL string, timeout time.Duration) *OpenAICompatibleClient {
	return &OpenAICompatibleClient{baseURL: strings.TrimRight(baseURL, "/"), client: httpclient.New(timeout)}
}

func (c *OpenAICompatibleClient) ChatJSON(ctx context.Context, req ChatRequest) (string, error) {
	userBytes, _ := json.Marshal(req.UserJSON)
	body := map[string]any{
		"model": req.Model,
		"messages": []map[string]string{
			{"role": "system", "content": req.System},
			{"role": "user", "content": string(userBytes)},
		},
		"temperature": req.Temperature,
		"stream":      false,
	}
	var raw struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	headers := map[string]string{"Authorization": "Bearer " + req.APIKey}
	if err := c.client.PostJSON(ctx, c.baseURL+"/chat/completions", headers, body, &raw); err != nil {
		return "", err
	}
	if len(raw.Choices) == 0 {
		return "", fmt.Errorf("ai response has no choices")
	}
	return strings.TrimSpace(raw.Choices[0].Message.Content), nil
}
