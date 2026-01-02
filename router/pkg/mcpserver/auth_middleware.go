package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/wundergraph/cosmo/router/pkg/authentication"
)

type contextKey string

const (
	userClaimsContextKey contextKey = "mcp_user_claims"
)

// mcpAuthProvider adapts MCP headers to the authentication.Provider interface
type mcpAuthProvider struct {
	headers http.Header
}

func (p *mcpAuthProvider) AuthenticationHeaders() http.Header {
	return p.headers
}

// MCPAuthMiddleware creates authentication middleware for MCP tools and resources
type MCPAuthMiddleware struct {
	authenticator       authentication.Authenticator
	enabled             bool
	resourceMetadataURL string
}

// NewMCPAuthMiddleware creates a new authentication middleware using the existing
// authentication infrastructure from the router
func NewMCPAuthMiddleware(tokenDecoder authentication.TokenDecoder, enabled bool, resourceMetadataURL string) (*MCPAuthMiddleware, error) {
	// Use the existing HttpHeaderAuthenticator with default settings (Authorization header, Bearer prefix)
	// This ensures consistency with the rest of the router's authentication logic
	authenticator, err := authentication.NewHttpHeaderAuthenticator(authentication.HttpHeaderAuthenticatorOptions{
		Name:         "mcp-auth",
		TokenDecoder: tokenDecoder,
		// HeaderSourcePrefixes defaults to {"Authorization": {"Bearer"}} when not specified
		// This can be extended in the future to support additional schemes like DPoP
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create authenticator: %w", err)
	}

	return &MCPAuthMiddleware{
		authenticator:       authenticator,
		enabled:             enabled,
		resourceMetadataURL: resourceMetadataURL,
	}, nil
}

// ToolMiddleware wraps tool handlers with authentication
func (m *MCPAuthMiddleware) ToolMiddleware(next server.ToolHandlerFunc) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if !m.enabled {
			return next(ctx, req)
		}

		// Extract and validate token
		claims, err := m.authenticateRequest(ctx)
		if err != nil {
			// Return authentication error with WWW-Authenticate challenge information
			// Per RFC 9728, we should indicate the resource metadata URL
			errorMsg := fmt.Sprintf("Authentication failed: %v", err)
			if m.resourceMetadataURL != "" {
				errorMsg = fmt.Sprintf("Authentication required. Resource metadata available at: %s. Error: %v",
					m.resourceMetadataURL, err)
			}
			return mcp.NewToolResultError(errorMsg), nil
		}

		// Add claims to context
		ctx = context.WithValue(ctx, userClaimsContextKey, claims)

		return next(ctx, req)
	}
}

// authenticateRequest extracts and validates the JWT token using the existing
// authentication infrastructure from the router
func (m *MCPAuthMiddleware) authenticateRequest(ctx context.Context) (authentication.Claims, error) {
	// Extract headers from context (passed by mcp-go HTTP transport)
	headers, err := headersFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("missing request headers: %w", err)
	}

	// Use the existing authenticator instead of manual token parsing
	// This provides better error messages and supports multiple authentication schemes
	provider := &mcpAuthProvider{headers: headers}
	claims, err := m.authenticator.Authenticate(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("authentication failed: %w", err)
	}

	// If claims are empty, treat as authentication failure
	if len(claims) == 0 {
		return nil, fmt.Errorf("authentication failed: no valid credentials provided")
	}

	return claims, nil
}

// HTTPMiddleware wraps HTTP handlers with authentication for ALL MCP operations
// Per MCP specification: "authorization MUST be included in every HTTP request from client to server"
func (m *MCPAuthMiddleware) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !m.enabled {
			next.ServeHTTP(w, r)
			return
		}

		// Create a provider from the HTTP request headers
		provider := &mcpAuthProvider{headers: r.Header}

		// Validate the token
		claims, err := m.authenticator.Authenticate(r.Context(), provider)
		if err != nil || len(claims) == 0 {
			// Return 401 with WWW-Authenticate header per RFC 9728
			w.Header().Set("Content-Type", "application/json")
			
			// Build WWW-Authenticate header with resource metadata URL
			if m.resourceMetadataURL != "" {
				w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="mcp", resource="%s"`, m.resourceMetadataURL))
			} else {
				w.Header().Set("WWW-Authenticate", `Bearer realm="mcp"`)
			}
			
			w.WriteHeader(http.StatusUnauthorized)

			// Return JSON-RPC error response
			errorResponse := map[string]any{
				"jsonrpc": "2.0",
				"error": map[string]any{
					"code":    -32001,
					"message": "Authentication required",
					"data": map[string]any{
						"resource_metadata": m.resourceMetadataURL,
					},
				},
			}

			if err := json.NewEncoder(w).Encode(errorResponse); err != nil {
				http.Error(w, "Internal server error", http.StatusInternalServerError)
			}
			return
		}

		// Add claims to request context for downstream handlers
		ctx := context.WithValue(r.Context(), userClaimsContextKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetClaimsFromContext retrieves authenticated user claims from context
func GetClaimsFromContext(ctx context.Context) (authentication.Claims, bool) {
	claims, ok := ctx.Value(userClaimsContextKey).(authentication.Claims)
	return claims, ok
}
