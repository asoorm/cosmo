package mcpserver

import (
	"context"
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

// GetClaimsFromContext retrieves authenticated user claims from context
func GetClaimsFromContext(ctx context.Context) (authentication.Claims, bool) {
	claims, ok := ctx.Value(userClaimsContextKey).(authentication.Claims)
	return claims, ok
}
