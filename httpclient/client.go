package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tjbdwanghaibo/cube-core/security"
)

const HeaderRequestID = "X-Request-ID"

type contextKey string

const requestIDContextKey contextKey = "httpclient.request_id"

type Option func(*Client)

type Client struct {
	baseURL       string
	client        *http.Client
	timeout       time.Duration
	signHeader    string
	signSecret    string
	defaultHeader http.Header
}

type StatusError struct {
	StatusCode int
	Status     string
	Body       []byte
}

func (e *StatusError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("http client: status %s", e.Status)
}

func New(opts ...Option) *Client {
	c := &Client{
		timeout:       5 * time.Second,
		defaultHeader: make(http.Header),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	if c.client == nil {
		c.client = &http.Client{Timeout: c.timeout}
	}
	return c
}

func (c *Client) Clone(opts ...Option) *Client {
	if c == nil {
		return New(opts...)
	}
	next := &Client{
		baseURL:       c.baseURL,
		client:        c.client,
		timeout:       c.timeout,
		signHeader:    c.signHeader,
		signSecret:    c.signSecret,
		defaultHeader: cloneHeader(c.defaultHeader),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(next)
		}
	}
	if next.client == nil {
		next.client = &http.Client{Timeout: next.timeout}
	}
	return next
}

func WithBaseURL(baseURL string) Option {
	return func(c *Client) {
		c.baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		if timeout > 0 {
			c.timeout = timeout
		}
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		if client != nil {
			c.client = client
		}
	}
}

func WithSigner(header string, secret string) Option {
	return func(c *Client) {
		c.signHeader = strings.TrimSpace(header)
		c.signSecret = secret
	}
}

func WithHeader(key string, value string) Option {
	return func(c *Client) {
		if c.defaultHeader == nil {
			c.defaultHeader = make(http.Header)
		}
		c.defaultHeader.Set(key, value)
	}
}

func WithRequestID(ctx context.Context, requestID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestIDContextKey, requestID)
}

func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(requestIDContextKey).(string)
	return v
}

func (c *Client) PostJSON(ctx context.Context, path string, req any, out any) error {
	return c.DoJSON(ctx, http.MethodPost, path, req, out)
}

func (c *Client) DoJSON(ctx context.Context, method string, path string, req any, out any) error {
	if c == nil {
		return fmt.Errorf("http client: nil client")
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, c.url(path), bytes.NewReader(raw))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	for key, values := range c.defaultHeader {
		for _, value := range values {
			httpReq.Header.Add(key, value)
		}
	}
	if requestID := RequestID(ctx); requestID != "" {
		httpReq.Header.Set(HeaderRequestID, requestID)
	}
	if c.signHeader != "" && c.signSecret != "" {
		httpReq.Header.Set(c.signHeader, security.SignPayload(raw, c.signSecret))
	}
	client := c.client
	if client == nil {
		client = &http.Client{Timeout: c.timeout}
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return readErr
	}
	if out != nil && len(body) > 0 {
		if err := json.Unmarshal(body, out); err != nil {
			return err
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &StatusError{StatusCode: resp.StatusCode, Status: resp.Status, Body: body}
	}
	return nil
}

func (c *Client) url(path string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	if c.baseURL == "" {
		return path
	}
	if path == "" {
		return c.baseURL
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return c.baseURL + path
}

func cloneHeader(src http.Header) http.Header {
	out := make(http.Header, len(src))
	for key, values := range src {
		out[key] = append([]string(nil), values...)
	}
	return out
}
