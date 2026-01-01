package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wundergraph/cosmo/router/pkg/authentication"
)

// mockTokenDecoder is a mock implementation of authentication.TokenDecoder for testing
type mockTokenDecoder struct {
	decodeFunc func(token string) (authentication.Claims, error)
}

func (m *mockTokenDecoder) Decode(token string) (authentication.Claims, error) {
	if m.decodeFunc != nil {
		return m.decodeFunc(token)
	}
	return nil, errors.New("not implemented")
}

// getTextFromResult extracts text from the first content item in a result
func getTextFromResult(result *mcp.CallToolResult) string {
	if result == nil || len(result.Content) == 0 {
		return ""
	}
	textContent, ok := mcp.AsTextContent(result.Content[0])
	if !ok {
		return ""
	}
	return textContent.Text
}

func TestNewMCPAuthMiddleware(t *testing.T) {
	validDecoder := &mockTokenDecoder{
		decodeFunc: func(token string) (authentication.Claims, error) {
			return authentication.Claims{"sub": "user123"}, nil
		},
	}

	tests := []struct {
		name        string
		decoder     authentication.TokenDecoder
		enabled     bool
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid decoder enabled",
			decoder: validDecoder,
			enabled: true,
			wantErr: false,
		},
		{
			name:    "valid decoder disabled",
			decoder: validDecoder,
			enabled: false,
			wantErr: false,
		},
		{
			name:        "nil decoder",
			decoder:     nil,
			enabled:     true,
			wantErr:     true,
			errContains: "token decoder must be provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			middleware, err := NewMCPAuthMiddleware(tt.decoder, tt.enabled)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, middleware)
			} else {
				require.NoError(t, err)
				require.NotNil(t, middleware)
				assert.Equal(t, tt.enabled, middleware.enabled)
				assert.NotNil(t, middleware.authenticator)
			}
		})
	}
}

func TestMCPAuthMiddleware_ToolMiddleware(t *testing.T) {
	validClaims := authentication.Claims{"sub": "user123", "email": "user@example.com"}

	tests := []struct {
		name            string
		enabled         bool
		decoder         *mockTokenDecoder
		setupHeaders    func() http.Header
		wantErr         bool
		wantTextContain string
	}{
		{
			name:    "bypasses auth when disabled",
			enabled: false,
			decoder: &mockTokenDecoder{
				decodeFunc: func(token string) (authentication.Claims, error) {
					t.Fatal("should not be called")
					return nil, nil
				},
			},
			setupHeaders: func() http.Header {
				return http.Header{}
			},
			wantErr:         false,
			wantTextContain: "no authentication",
		},
		{
			name:    "valid Bearer token",
			enabled: true,
			decoder: &mockTokenDecoder{
				decodeFunc: func(token string) (authentication.Claims, error) {
					if token == "valid-token" {
						return validClaims, nil
					}
					return nil, errors.New("invalid token")
				},
			},
			setupHeaders: func() http.Header {
				h := http.Header{}
				h.Set("Authorization", "Bearer valid-token")
				return h
			},
			wantErr:         false,
			wantTextContain: "authenticated with claims",
		},
		{
			name:    "invalid token",
			enabled: true,
			decoder: &mockTokenDecoder{
				decodeFunc: func(token string) (authentication.Claims, error) {
					return nil, errors.New("token validation failed")
				},
			},
			setupHeaders: func() http.Header {
				h := http.Header{}
				h.Set("Authorization", "Bearer invalid-token")
				return h
			},
			wantErr:         true,
			wantTextContain: "Authentication failed",
		},
		{
			name:    "wrong header format",
			enabled: true,
			decoder: &mockTokenDecoder{
				decodeFunc: func(token string) (authentication.Claims, error) {
					return validClaims, nil
				},
			},
			setupHeaders: func() http.Header {
				h := http.Header{}
				h.Set("Authorization", "invalid-token")
				return h
			},
			wantErr:         true,
			wantTextContain: "Authentication failed",
		},
		{
			name:    "Bearer token with whitespace",
			enabled: true,
			decoder: &mockTokenDecoder{
				decodeFunc: func(token string) (authentication.Claims, error) {
					if token == "valid-token" {
						return validClaims, nil
					}
					return nil, fmt.Errorf("unexpected token: %s", token)
				},
			},
			setupHeaders: func() http.Header {
				h := http.Header{}
				h.Set("Authorization", "Bearer  valid-token  ")
				return h
			},
			wantErr:         false,
			wantTextContain: "authenticated with claims",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			middleware, err := NewMCPAuthMiddleware(tt.decoder, tt.enabled)
			require.NoError(t, err)

			handler := middleware.ToolMiddleware(func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				claims, ok := GetClaimsFromContext(ctx)
				if ok {
					return mcp.NewToolResultText(fmt.Sprintf("authenticated with claims: %v", claims)), nil
				}
				return mcp.NewToolResultText("no authentication"), nil
			})

			ctx := withRequestHeaders(context.Background(), tt.setupHeaders())
			result, err := handler(ctx, mcp.CallToolRequest{})

			require.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			assert.Contains(t, getTextFromResult(result), tt.wantTextContain)
		})
	}
}

func TestMCPAuthMiddleware_MissingHeaders(t *testing.T) {
	decoder := &mockTokenDecoder{
		decodeFunc: func(token string) (authentication.Claims, error) {
			return authentication.Claims{"sub": "user123"}, nil
		},
	}

	middleware, err := NewMCPAuthMiddleware(decoder, true)
	require.NoError(t, err)

	handler := middleware.ToolMiddleware(func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("success"), nil
	})

	// Context without headers
	result, err := handler(context.Background(), mcp.CallToolRequest{})
	require.NoError(t, err)
	assert.True(t, result.IsError)
	assert.Contains(t, getTextFromResult(result), "missing request headers")
}

func TestGetClaimsFromContext(t *testing.T) {
	expectedClaims := authentication.Claims{"sub": "user123", "email": "user@example.com"}

	tests := []struct {
		name       string
		setupCtx   func() context.Context
		wantOk     bool
		wantClaims authentication.Claims
	}{
		{
			name: "claims present",
			setupCtx: func() context.Context {
				return context.WithValue(context.Background(), userClaimsContextKey, expectedClaims)
			},
			wantOk:     true,
			wantClaims: expectedClaims,
		},
		{
			name: "claims absent",
			setupCtx: func() context.Context {
				return context.Background()
			},
			wantOk:     false,
			wantClaims: nil,
		},
		{
			name: "wrong type",
			setupCtx: func() context.Context {
				return context.WithValue(context.Background(), userClaimsContextKey, "not-claims")
			},
			wantOk:     false,
			wantClaims: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claims, ok := GetClaimsFromContext(tt.setupCtx())
			assert.Equal(t, tt.wantOk, ok)
			assert.Equal(t, tt.wantClaims, claims)
		})
	}
}

func TestMCPAuthProvider(t *testing.T) {
	t.Run("returns headers", func(t *testing.T) {
		headers := http.Header{}
		headers.Set("Authorization", "Bearer token")
		headers.Set("X-Custom", "value")

		provider := &mcpAuthProvider{headers: headers}
		assert.Equal(t, headers, provider.AuthenticationHeaders())
	})

	t.Run("empty headers", func(t *testing.T) {
		provider := &mcpAuthProvider{headers: http.Header{}}
		assert.Equal(t, 0, len(provider.AuthenticationHeaders()))
	})
}

func TestMCPAuthMiddleware_Integration(t *testing.T) {
	expectedClaims := authentication.Claims{"sub": "user123", "role": "admin"}

	decoder := &mockTokenDecoder{
		decodeFunc: func(token string) (authentication.Claims, error) {
			if token == "valid-token" {
				return expectedClaims, nil
			}
			return nil, errors.New("invalid token")
		},
	}

	middleware, err := NewMCPAuthMiddleware(decoder, true)
	require.NoError(t, err)

	handler := middleware.ToolMiddleware(func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		claims, ok := GetClaimsFromContext(ctx)
		if !ok {
			return mcp.NewToolResultError("no claims found"), nil
		}
		return mcp.NewToolResultText(fmt.Sprintf("user: %s, role: %s", claims["sub"], claims["role"])), nil
	})

	// Valid token
	headers := http.Header{}
	headers.Set("Authorization", "Bearer valid-token")
	ctx := withRequestHeaders(context.Background(), headers)

	result, err := handler(ctx, mcp.CallToolRequest{})
	require.NoError(t, err)
	assert.False(t, result.IsError)
	text := getTextFromResult(result)
	assert.Contains(t, text, "user: user123")
	assert.Contains(t, text, "role: admin")

	// Invalid token
	headers.Set("Authorization", "Bearer invalid-token")
	ctx = withRequestHeaders(context.Background(), headers)

	result, err = handler(ctx, mcp.CallToolRequest{})
	require.NoError(t, err)
	assert.True(t, result.IsError)
	assert.Contains(t, getTextFromResult(result), "Authentication failed")
}
