package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/astprinter"
)

// Client handles GraphQL requests to the upstream server
type Client struct {
	httpClient *http.Client
	endpoint   string
}

// GraphQLPayload represents a GraphQL request payload
type GraphQLPayload struct {
	Query         string                 `json:"query"`
	OperationName string                 `json:"operationName,omitempty"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
}

// GraphQLResponse represents a GraphQL response
type GraphQLResponse struct {
	Data   interface{}   `json:"data"`
	Errors []interface{} `json:"errors"`
}

// NewClient creates a new GraphQL proxy client
func NewClient(endpoint string, timeout time.Duration) *Client {
	return &Client{
		httpClient: &http.Client{Timeout: timeout},
		endpoint:   endpoint,
	}
}

// ExecuteOperation executes a GraphQL operation with the given variables
func (c *Client) ExecuteOperation(ctx context.Context, operationDocument ast.Document, operationName string, variables map[string]interface{}) (*GraphQLResponse, error) {

	var buf bytes.Buffer
	err := astprinter.PrintIndent(&operationDocument, []byte("  "), &buf)
	if err != nil {
		return nil, fmt.Errorf("failed to print GraphQL document: %w", err)
	}

	// Create GraphQL payload
	payload := GraphQLPayload{
		Query:         buf.String(),
		OperationName: operationName,
		Variables:     variables,
	}

	// Marshal payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GraphQL payload: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Copy authorization header from context if available
	if auth := getAuthFromContext(ctx); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GraphQL request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read GraphQL response: %w", err)
	}

	// Check HTTP status
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GraphQL server returned status %d: %s", resp.StatusCode, string(responseBody))
	}

	// Parse GraphQL response
	var gqlResp GraphQLResponse
	if err := json.Unmarshal(responseBody, &gqlResp); err != nil {
		return nil, fmt.Errorf("failed to parse GraphQL response: %w", err)
	}

	return &gqlResp, nil
}

// ExecuteSubscription executes a GraphQL subscription with streaming callback
func (c *Client) ExecuteSubscription(ctx context.Context, operationDocument ast.Document, operationName string, variables map[string]interface{}, callback func(interface{}) error) error {
	fmt.Printf("🚀 PROXY CLIENT: Starting subscription execution\n")
	fmt.Printf("   Operation: %s\n", operationName)
	fmt.Printf("   Variables: %+v\n", variables)

	var buf bytes.Buffer
	err := astprinter.PrintIndent(&operationDocument, []byte("  "), &buf)
	if err != nil {
		return fmt.Errorf("failed to print GraphQL document: %w", err)
	}

	queryString := buf.String()
	fmt.Printf("📝 PROXY CLIENT: GraphQL Query:\n%s\n", queryString)

	// Create GraphQL payload
	payload := GraphQLPayload{
		Query:         queryString,
		OperationName: operationName,
		Variables:     variables,
	}

	// Marshal payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal GraphQL payload: %w", err)
	}

	fmt.Printf("📤 PROXY CLIENT: Sending to endpoint: %s\n", c.endpoint)
	fmt.Printf("📦 PROXY CLIENT: Payload: %s\n", string(payloadBytes))

	// Create HTTP request for subscription (typically SSE)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers for GraphQL subscription streaming
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	// Copy authorization header from context if available
	if auth := getAuthFromContext(ctx); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	fmt.Printf("🔗 PROXY CLIENT: Request headers: %+v\n", req.Header)

	// Execute streaming request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		fmt.Printf("❌ PROXY CLIENT: Request failed: %v\n", err)
		return fmt.Errorf("GraphQL subscription request failed: %w", err)
	}
	defer resp.Body.Close()

	fmt.Printf("📥 PROXY CLIENT: Response status: %d\n", resp.StatusCode)
	fmt.Printf("📋 PROXY CLIENT: Response headers: %+v\n", resp.Header)

	// Check HTTP status
	if resp.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(resp.Body)
		fmt.Printf("❌ PROXY CLIENT: Bad status response: %s\n", string(responseBody))
		return fmt.Errorf("GraphQL subscription server returned status %d: %s", resp.StatusCode, string(responseBody))
	}

	// Process streaming response
	fmt.Printf("🔄 PROXY CLIENT: Starting stream processing\n")
	return c.processStreamingResponse(ctx, resp, callback)
}

// processStreamingResponse handles SSE streaming responses
func (c *Client) processStreamingResponse(ctx context.Context, resp *http.Response, callback func(interface{}) error) error {
	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			line := scanner.Text()

			// Parse SSE format
			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")

				// Skip empty data lines
				if strings.TrimSpace(data) == "" {
					continue
				}

				var gqlResp GraphQLResponse
				if err := json.Unmarshal([]byte(data), &gqlResp); err != nil {
					// Skip malformed data but continue processing
					continue
				}

				// Check for GraphQL errors
				if len(gqlResp.Errors) > 0 {
					// Convert GraphQL errors to Go error
					return fmt.Errorf("GraphQL subscription error: %v", gqlResp.Errors)
				}

				// Call callback with streaming data
				if err := callback(gqlResp.Data); err != nil {
					return err
				}
			}
		}
	}

	return scanner.Err()
}

// getAuthFromContext extracts authorization header from context
// This is a placeholder - in a real implementation, you might use a more sophisticated context key
func getAuthFromContext(ctx context.Context) string {
	// For now, we'll implement this later when we handle request headers
	// In Connect RPC, we can access the original HTTP headers from the request
	return ""
}

// GraphQLErrorToConnectError converts GraphQL errors to Connect RPC errors
func GraphQLErrorToConnectError(gqlErrors []interface{}) *connect.Error {
	if len(gqlErrors) == 0 {
		return nil
	}

	// For simplicity, we'll use the first error and map it to a Connect error
	// In a production system, you might want more sophisticated error handling

	// Try to extract error message
	var message string
	if len(gqlErrors) > 0 {
		if errMap, ok := gqlErrors[0].(map[string]interface{}); ok {
			if msg, ok := errMap["message"].(string); ok {
				message = msg
			}
		}
	}

	if message == "" {
		message = "GraphQL execution error"
	}

	// Map to appropriate Connect error code
	// For now, we'll use a simple mapping - this can be enhanced later
	code := connect.CodeInternal

	// Check for common GraphQL error patterns
	if containsString(message, "not found") || containsString(message, "does not exist") {
		code = connect.CodeNotFound
	} else if containsString(message, "unauthorized") || containsString(message, "authentication") {
		code = connect.CodeUnauthenticated
	} else if containsString(message, "forbidden") || containsString(message, "permission") {
		code = connect.CodePermissionDenied
	} else if containsString(message, "invalid") || containsString(message, "validation") {
		code = connect.CodeInvalidArgument
	}

	return connect.NewError(code, fmt.Errorf("%s", message))
}

// containsString checks if a string contains a substring (case-insensitive)
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) &&
			(s[:len(substr)] == substr ||
				s[len(s)-len(substr):] == substr ||
				bytes.Contains([]byte(s), []byte(substr)))))
}
