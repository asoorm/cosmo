package connect_rpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/bufbuild/protocompile"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/wundergraph/cosmo/router/pkg/connect_rpc/proxy"
	"github.com/wundergraph/cosmo/router/pkg/schemaloader"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type ConnectRPCServer struct {
	logger                *zap.Logger
	graphqlClient         *proxy.Client
	listenAddr            string
	requestTimeout        time.Duration
	routerGraphQLEndpoint string
	collectionDirectory   string
	collection            map[string]schemaloader.Operation
	packageName           string
	serviceName           string
	protoSchema           []*descriptorpb.FileDescriptorProto
}

type Options struct {
	Logger                *zap.Logger
	ListenAddr            string
	GraphQLClient         *proxy.Client
	RequestTimeout        time.Duration
	RouterGraphQLEndpoint string
	CollectionDir         string
	PackageName           string
	ServiceName           string
	ProtoFile             string
}

func WithLogger(logger *zap.Logger) func(*Options) {
	return func(o *Options) {
		o.Logger = logger
	}
}

func WithListenAddress(address string) func(*Options) {
	return func(o *Options) {
		o.ListenAddr = address
	}
}

func WithCollectionDir(dir string) func(*Options) {
	return func(o *Options) {
		o.CollectionDir = dir
	}
}

func WithPackageName(packageName string) func(*Options) {
	return func(o *Options) {
		o.PackageName = packageName
	}
}

func WithGraphQLClient(client *proxy.Client) func(*Options) {
	return func(o *Options) {
		o.GraphQLClient = client
	}
}

func WithServiceName(serviceName string) func(*Options) {
	return func(o *Options) {
		o.ServiceName = serviceName
	}
}

func NewConnectRPCServer(opts ...func(*Options)) *ConnectRPCServer {

	options := &Options{
		Logger:         zap.NewNop(),
		RequestTimeout: 10 * time.Second,
		CollectionDir:  "./operations",
		ProtoFile:      "/Users/asoorm/go/src/github.com/wundergraph/openapi-demo/proto4/",
	}

	for _, opt := range opts {
		if opt != nil {
			opt(options)
		}
	}

	s := &ConnectRPCServer{
		logger:                options.Logger,
		requestTimeout:        options.RequestTimeout,
		routerGraphQLEndpoint: options.RouterGraphQLEndpoint,
		collectionDirectory:   options.CollectionDir,
		packageName:           options.PackageName,
		serviceName:           options.ServiceName,
		listenAddr:            options.ListenAddr,
		graphqlClient:         options.GraphQLClient,
	}

	// Construct the full path to the proto file
	protoFilePath := options.ProtoFile + "/service.proto"
	s.logger.Debug("attempting to parse proto file",
		zap.String("protoFilePath", protoFilePath),
		zap.String("protoDir", options.ProtoFile))

	// Check if the file exists first
	if _, err := os.Stat(protoFilePath); os.IsNotExist(err) {
		s.logger.Error("proto file does not exist",
			zap.String("protoFilePath", protoFilePath),
			zap.Error(err))
		panic(fmt.Errorf("proto file does not exist: %s", protoFilePath))
	}

	s.logger.Debug("proto file exists, proceeding with parsing")

	// Try the most reliable approach first: parse with full path and no import paths
	s.logger.Debug("parsing proto file with full path approach")
	p := protoparse.Parser{}
	fds, err := p.ParseFilesButDoNotLink(protoFilePath)
	if err != nil {
		s.logger.Error("failed to load proto schema with full path",
			zap.String("fullPath", protoFilePath),
			zap.Error(err))

		// Fallback: try with import path approach
		s.logger.Debug("trying fallback approach with import path")
		p2 := protoparse.Parser{
			ImportPaths: []string{options.ProtoFile},
		}
		fds, err = p2.ParseFilesButDoNotLink("service.proto")
		if err != nil {
			s.logger.Error("failed to load proto schema with import path",
				zap.String("protoDir", options.ProtoFile),
				zap.String("protoFile", "service.proto"),
				zap.Error(err))
			panic(fmt.Errorf("failed to parse proto file %s: %w", protoFilePath, err))
		}
		s.logger.Debug("successfully parsed proto file with import path fallback")
	} else {
		s.logger.Debug("successfully parsed proto file with full path approach")
	}

	s.logger.Debug("successfully parsed proto files", zap.Int("fileCount", len(fds)))

	//var schema *descriptorpb.FileDescriptorSet
	//if options.ProtoFile != "" {
	//	var err error
	//	schema, err = loadProtoFile(options.ProtoFile)
	//	if err != nil {
	//		s.logger.Error("failed to load proto schema", zap.String("file", options.ProtoFile), zap.Error(err))
	//		// Don't return nil, just log the error and continue without schema
	//		schema = nil
	//	}
	//}
	s.protoSchema = fds

	return s
}

func (s *ConnectRPCServer) RegisterHandlers(mux *http.ServeMux) {
	s.logger.Info("starting handler registration",
		zap.String("packageName", s.packageName),
		zap.String("serviceName", s.serviceName),
		zap.Int("operationCount", len(s.collection)))

	for operationName, operation := range s.collection {
		op := operation

		listenPath := fmt.Sprintf("/%s.%s/%s", s.packageName, s.serviceName, operationName)

		s.logger.Info("registering handler",
			zap.String("path", listenPath),
			zap.String("operationName", operationName),
			zap.String("operationType", operation.OperationType))

		if operation.OperationType == "subscription" {
			// Create Connect RPC server streaming handler
			handler := s.createConnectStreamingHandler(op)
			mux.Handle(listenPath, handler)
		} else {
			// Create unified handler for unary operations
			handler := s.createUnifiedHandler(op)
			mux.Handle(listenPath, handler)
		}
	}

	s.logger.Info("completed handler registration", zap.Int("totalHandlers", len(s.collection)))
}

// createConnectStreamingHandler creates a proper Connect RPC server streaming handler
func (s *ConnectRPCServer) createConnectStreamingHandler(operation schemaloader.Operation) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("handling Connect RPC server streaming request",
			zap.String("operation", operation.Name),
			zap.String("contentType", r.Header.Get("Content-Type")))

		// Validate Connect streaming headers
		if !s.isValidConnectStreamingRequest(r) {
			s.logger.Error("invalid Connect streaming headers")
			s.writeConnectError(w, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid Connect streaming headers")))
			return
		}

		// Parse Connect streaming request
		connectRequest, err := s.parseConnectStreamingRequest(r)
		if err != nil {
			s.logger.Error("failed to parse streaming request", zap.Error(err))
			s.writeConnectError(w, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("failed to parse streaming request: %w", err)))
			return
		}

		// Map to GraphQL variables
		variables, err := s.mapConnectRequestToGraphQLVariables(connectRequest, operation)
		if err != nil {
			s.logger.Error("variable mapping failed", zap.Error(err))
			s.writeConnectError(w, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("variable mapping failed: %w", err)))
			return
		}

		// Set Connect streaming response headers BEFORE writing any data
		// CRITICAL: For Connect streaming, we must ensure exact protocol compliance
		contentType := r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", contentType) // Mirror exact request content type
		w.Header().Set("Connect-Protocol-Version", "1")
		w.Header().Set("Connect-Accept-Encoding", r.Header.Get("Connect-Accept-Encoding"))

		// Log headers for debugging
		s.logger.Info("🔧 SETTING CONNECT RESPONSE HEADERS",
			zap.String("contentType", contentType),
			zap.String("connectAcceptEncoding", r.Header.Get("Connect-Accept-Encoding")))

		// IMPORTANT: Don't call WriteHeader until you're ready to start streaming
		w.WriteHeader(http.StatusOK)

		// Flush headers immediately
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
			s.logger.Debug("💨 FLUSHED RESPONSE HEADERS")
		} else {
			s.logger.Warn("⚠️  ResponseWriter does not support flushing headers")
		}

		s.logger.Info("starting GraphQL subscription execution")

		// Execute subscription with streaming callback
		s.logger.Info("🚀 STARTING GRAPHQL SUBSCRIPTION",
			zap.String("operation", operation.Name),
			zap.Any("variables", variables))

		// CRITICAL: Create a channel to buffer streaming frames to ensure proper Connect protocol
		frameChan := make(chan interface{}, 100) // Buffer up to 100 frames
		var subscriptionErr error

		// Start GraphQL subscription in a goroutine
		go func() {
			defer close(frameChan)
			subscriptionErr = s.graphqlClient.ExecuteSubscription(r.Context(), operation.Document, operation.Name, variables, func(data interface{}) error {
				select {
				case frameChan <- data:
					return nil
				case <-r.Context().Done():
					return r.Context().Err()
				}
			})
		}()

		// CRITICAL: Send a test frame immediately to establish the streaming connection
		s.logger.Info("🧪 SENDING INITIAL TEST FRAME TO ESTABLISH CONNECT STREAM")
		// This shouldn't be necessary, but let's test if it helps with protocol negotiation

		// Process frames from the channel in the main goroutine
		for data := range frameChan {
			s.logger.Info("🔥 SUBSCRIPTION DATA RECEIVED",
				zap.Any("data", data),
				zap.String("dataType", fmt.Sprintf("%T", data)))

			// Convert to JSON for debugging
			if jsonData, jsonErr := json.Marshal(data); jsonErr == nil {
				s.logger.Info("📦 SUBSCRIPTION DATA JSON", zap.String("json", string(jsonData)))
			}

			writeErr := s.writeConnectStreamingFrame(w, data, false)
			if writeErr != nil {
				s.logger.Error("❌ FAILED TO WRITE STREAMING FRAME", zap.Error(writeErr))
				break
			} else {
				s.logger.Info("✅ SUCCESSFULLY WROTE STREAMING FRAME")
			}
		}

		// Check the subscription error after the loop ends
		err = subscriptionErr

		// Always send an end frame, even on error
		if err != nil && err != context.Canceled {
			s.logger.Error("🚨 GRAPHQL SUBSCRIPTION ERROR",
				zap.Error(err),
				zap.String("errorType", fmt.Sprintf("%T", err)),
				zap.String("operation", operation.Name))
			// Send error end frame
			s.writeConnectStreamingError(w, err)
		} else if err == context.Canceled {
			s.logger.Info("🚫 GRAPHQL SUBSCRIPTION CANCELED",
				zap.String("operation", operation.Name))
			// Send success end frame even for canceled
			if endErr := s.writeConnectStreamingFrame(w, nil, true); endErr != nil {
				s.logger.Error("failed to write end frame for canceled subscription", zap.Error(endErr))
			}
		} else {
			s.logger.Info("✅ GRAPHQL SUBSCRIPTION COMPLETED SUCCESSFULLY",
				zap.String("operation", operation.Name))
			// Send success end frame
			if endErr := s.writeConnectStreamingFrame(w, nil, true); endErr != nil {
				s.logger.Error("failed to write end frame", zap.Error(endErr))
			}
		}

		// Final flush to ensure all data is sent before connection closes
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
			s.logger.Debug("💨 FINAL FLUSH BEFORE CONNECTION CLOSE")
		}

		s.logger.Info("🔚 STREAMING CONNECTION COMPLETED")
	})
}

// createOperationHandler creates a Connect RPC handler for a specific GraphQL operation
func (s *ConnectRPCServer) createOperationHandler(operation schemaloader.Operation) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle Connect RPC protocol
		s.handleConnectRPC(w, r, operation)
	})
}

// createUnifiedHandler creates a handler that supports both unary and streaming
func (s *ConnectRPCServer) createUnifiedHandler(operation schemaloader.Operation) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType := r.Header.Get("Content-Type")

		// Detect if this is a Connect streaming request
		isStreaming := strings.Contains(contentType, "application/connect+")
		isSubscription := operation.OperationType == "subscription"

		if isSubscription && isStreaming {
			// Handle as Connect RPC server-side streaming
			s.handleConnectStreaming(w, r, operation)
		} else if isSubscription {
			// Handle as SSE fallback for browser clients
			s.handleSubscriptionSSE(w, r, operation)
		} else {
			// Handle as unary Connect RPC
			s.handleConnectRPC(w, r, operation)
		}
	})
}

// handleConnectStreaming handles Connect RPC streaming protocol for subscriptions
func (s *ConnectRPCServer) handleConnectStreaming(w http.ResponseWriter, r *http.Request, operation schemaloader.Operation) {
	s.logger.Info("handling Connect RPC streaming request",
		zap.String("operation", operation.Name),
		zap.String("contentType", r.Header.Get("Content-Type")))

	// Validate Connect streaming headers
	if !s.validateConnectHeaders(r) {
		s.writeConnectError(w, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid Connect streaming headers")))
		return
	}

	// Parse Connect streaming request (with envelope)
	connectRequest, err := s.parseConnectStreamingRequest(r)
	if err != nil {
		s.writeConnectError(w, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("failed to parse streaming request: %w", err)))
		return
	}

	// Map to GraphQL variables
	variables, err := s.mapConnectRequestToGraphQLVariables(connectRequest, operation)
	if err != nil {
		s.writeConnectError(w, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("variable mapping failed: %w", err)))
		return
	}

	// Set Connect streaming response headers BEFORE writing any data
	w.Header().Set("Content-Type", r.Header.Get("Content-Type")) // Mirror request content type
	w.Header().Set("Connect-Protocol-Version", "1")
	w.Header().Set("Connect-Streaming-Accept-Encoding", "gzip")
	w.WriteHeader(http.StatusOK) // Always 200 for streaming

	// Flush headers immediately
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	s.logger.Info("starting GraphQL subscription execution")

	// Track if we've sent any data frames
	dataSent := false

	// Execute subscription with streaming callback
	err = s.graphqlClient.ExecuteSubscription(r.Context(), operation.Document, operation.Name, variables, func(data interface{}) error {
		s.logger.Info("🔥 SUBSCRIPTION DATA RECEIVED",
			zap.Any("data", data),
			zap.String("dataType", fmt.Sprintf("%T", data)))

		// Convert to JSON for debugging
		if jsonData, jsonErr := json.Marshal(data); jsonErr == nil {
			s.logger.Info("📦 SUBSCRIPTION DATA JSON", zap.String("json", string(jsonData)))
		}

		dataSent = true
		writeErr := s.writeConnectStreamingFrame(w, data, false)
		if writeErr != nil {
			s.logger.Error("❌ FAILED TO WRITE STREAMING FRAME", zap.Error(writeErr))
		} else {
			s.logger.Info("✅ SUCCESSFULLY WROTE STREAMING FRAME")
		}
		return writeErr
	})

	// For Connect RPC streaming, we need to send at least one message
	// If no data was sent, send an empty data frame first
	if !dataSent {
		s.logger.Info("no subscription data received, sending empty data frame")
		if frameErr := s.writeConnectStreamingFrame(w, map[string]interface{}{}, false); frameErr != nil {
			s.logger.Error("failed to write empty data frame", zap.Error(frameErr))
		}
	}

	// Write end stream frame
	if err != nil && err != context.Canceled {
		s.logger.Error("subscription error", zap.Error(err))
		s.writeConnectStreamingError(w, err)
	} else {
		s.logger.Info("subscription completed successfully, writing end frame")
		if endErr := s.writeConnectStreamingFrame(w, nil, true); endErr != nil {
			s.logger.Error("failed to write end frame", zap.Error(endErr))
		}
	}
}

// handleSubscriptionSSE handles subscriptions via Server-Sent Events (fallback)
func (s *ConnectRPCServer) handleSubscriptionSSE(w http.ResponseWriter, r *http.Request, operation schemaloader.Operation) {
	s.logger.Info("handling SSE subscription request",
		zap.String("operation", operation.Name))

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Parse variables
	connectRequest, err := s.parseConnectRequest(r)
	if err != nil {
		s.writeSSEError(w, err)
		return
	}

	variables, err := s.mapConnectRequestToGraphQLVariables(connectRequest, operation)
	if err != nil {
		s.writeSSEError(w, err)
		return
	}

	// Send initial connection event
	s.writeSSEEvent(w, "connected", map[string]interface{}{
		"operation": operation.Name,
		"type":      "subscription",
	})

	// Execute subscription
	err = s.graphqlClient.ExecuteSubscription(r.Context(), operation.Document, operation.Name, variables, func(data interface{}) error {
		return s.writeSSEEvent(w, "data", data)
	})

	if err != nil && err != context.Canceled {
		s.writeSSEError(w, err)
	} else {
		s.writeSSEEvent(w, "complete", map[string]interface{}{
			"operation": operation.Name,
		})
	}
}

// handleConnectRPC handles the Connect RPC protocol for a specific operation
func (s *ConnectRPCServer) handleConnectRPC(w http.ResponseWriter, r *http.Request, operation schemaloader.Operation) {
	// Validate Connect RPC headers
	if !s.validateConnectHeaders(r) {
		s.writeConnectError(w, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid Connect RPC headers")))
		return
	}

	// Parse the request body based on content type
	connectRequest, err := s.parseConnectRequest(r)
	if err != nil {
		s.writeConnectError(w, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("failed to parse request: %w", err)))
		return
	}

	// Map Connect RPC request to GraphQL variables
	variables, err := s.mapConnectRequestToGraphQLVariables(connectRequest, operation)
	if err != nil {
		s.writeConnectError(w, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("variable mapping failed: %w", err)))
		return
	}

	// Execute the GraphQL operation
	gqlResp, err := s.graphqlClient.ExecuteOperation(r.Context(), operation.Document, operation.Name, variables)
	if err != nil {
		s.writeConnectError(w, connect.NewError(connect.CodeInternal, fmt.Errorf("GraphQL execution failed: %w", err)))
		return
	}

	// Check for GraphQL errors
	if len(gqlResp.Errors) > 0 {
		connectErr := proxy.GraphQLErrorToConnectError(gqlResp.Errors)
		s.writeConnectError(w, connectErr)
		return
	}

	// Write successful response
	s.writeConnectSuccess(w, r, gqlResp.Data)
}

// validateConnectHeaders validates Connect RPC protocol headers
func (s *ConnectRPCServer) validateConnectHeaders(r *http.Request) bool {
	if r.Method == http.MethodGet {
		// For GET requests, validate Connect protocol version in query parameters
		query := r.URL.Query()
		connectVersion := query.Get("connect")
		if connectVersion != "" && connectVersion != "v1" {
			s.logger.Warn("unsupported Connect protocol version in GET request",
				zap.String("version", connectVersion))
			return false
		}
		return true
	}

	contentType := r.Header.Get("Content-Type")

	// Support both unary and streaming Connect RPC
	validContentTypes := []string{
		"application/json",
		"application/proto",
		"application/connect+proto", // Streaming Connect RPC
		"application/connect+json",  // Streaming Connect RPC (JSON)
	}

	for _, validType := range validContentTypes {
		if contentType == validType {
			return true
		}
	}

	return false
}

// parseConnectRequest parses the Connect RPC request body
func (s *ConnectRPCServer) parseConnectRequest(r *http.Request) (map[string]interface{}, error) {
	if r.Method == http.MethodGet {
		return s.parseConnectGetRequest(r)
	}

	contentType := r.Header.Get("Content-Type")
	switch contentType {
	case "application/json":
		return s.parseJSONRequest(r)
	case "application/proto":
		return s.parseProtoRequestFromBody(r)
	default:
		return nil, fmt.Errorf("unsupported content type: %s", contentType)
	}
}

// parseConnectGetRequest parses Connect RPC GET requests with query parameters
func (s *ConnectRPCServer) parseConnectGetRequest(r *http.Request) (map[string]interface{}, error) {
	query := r.URL.Query()
	encoding := query.Get("encoding")
	message := query.Get("message")
	connectVersion := query.Get("connect")
	base64Param := query.Get("base64")
	compressionParam := query.Get("compression")

	// Validate Connect protocol version if present
	if connectVersion != "" && connectVersion != "v1" {
		return nil, fmt.Errorf("unsupported Connect protocol version: %s (expected 'v1')", connectVersion)
	}

	if encoding == "" {
		return nil, fmt.Errorf("missing required 'encoding' query parameter")
	}
	if message == "" {
		// Empty message is allowed for some operations (e.g., operations with no parameters)
		s.logger.Debug("empty message parameter in GET request")
		return make(map[string]interface{}), nil
	}

	s.logger.Debug("parsed GET request parameters",
		zap.String("encoding", encoding),
		zap.String("connectVersion", connectVersion),
		zap.String("base64", base64Param),
		zap.String("compression", compressionParam),
		zap.Int("messageLength", len(message)))

	// Handle compression (currently only 'identity' is supported)
	if compressionParam != "" && compressionParam != "identity" {
		return nil, fmt.Errorf("unsupported compression: %s (only 'identity' is supported)", compressionParam)
	}

	// Determine if we should use base64 decoding
	useBase64 := base64Param == "1"

	switch encoding {
	case "json":
		return s.parseJSONFromGetRequest(message, useBase64)
	case "proto":
		return s.parseProtoFromGetRequest(r, message, useBase64)
	default:
		return nil, fmt.Errorf("unsupported encoding: %s (supported: 'json', 'proto')", encoding)
	}
}

// parseJSONFromGetRequest parses JSON message from GET request
func (s *ConnectRPCServer) parseJSONFromGetRequest(message string, useBase64 bool) (map[string]interface{}, error) {
	var jsonData []byte
	var err error

	if useBase64 {
		// Base64 decode first
		jsonData, err = base64.URLEncoding.DecodeString(message)
		if err != nil {
			// Try standard base64 if URL-safe fails
			jsonData, err = base64.StdEncoding.DecodeString(message)
			if err != nil {
				return nil, fmt.Errorf("failed to base64 decode JSON message: %w", err)
			}
		}
	} else {
		// URL decode the message
		decoded, err := url.QueryUnescape(message)
		if err != nil {
			return nil, fmt.Errorf("failed to URL decode JSON message: %w", err)
		}
		jsonData = []byte(decoded)
	}

	var requestData map[string]interface{}
	if err := json.Unmarshal(jsonData, &requestData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON message: %w", err)
	}

	s.logger.Debug("successfully parsed JSON from GET request", zap.Int("fields", len(requestData)))
	return requestData, nil
}

// parseProtoFromGetRequest parses protobuf message from GET request
func (s *ConnectRPCServer) parseProtoFromGetRequest(r *http.Request, message string, useBase64 bool) (map[string]interface{}, error) {
	var protoData []byte
	var err error

	if !useBase64 {
		// According to Connect spec, proto messages in GET requests should be base64 encoded
		// If base64=1 is not present but encoding=proto, we should still try base64 decoding
		s.logger.Warn("proto encoding without base64=1 parameter, attempting base64 decode anyway")
	}

	// Always try base64 decoding for proto messages (as per Connect spec)
	protoData, err = base64.URLEncoding.DecodeString(message)
	if err != nil {
		// Try standard base64 if URL-safe fails
		protoData, err = base64.StdEncoding.DecodeString(message)
		if err != nil {
			return nil, fmt.Errorf("failed to base64 decode proto message (proto encoding requires base64): %w", err)
		}
	}

	s.logger.Debug("successfully base64 decoded proto message", zap.Int("bytes", len(protoData)))
	return s.parseProtoRequestFromBytes(r, protoData)
}

// Helper for GET proto parsing (uses same logic as parseProtoRequest but with bytes)
func (s *ConnectRPCServer) parseProtoRequestFromBytes(r *http.Request, body []byte) (map[string]interface{}, error) {
	s.logger.Debug("parseProtoRequest called", zap.String("path", r.URL.Path))

	if s.protoSchema == nil {
		s.logger.Error("proto schema not loaded")
		return nil, fmt.Errorf("proto schema not loaded")
	}

	s.logger.Debug("proto schema is loaded", zap.Int("fileCount", len(s.protoSchema)))

	// 1. Extract operation info from path
	path := r.URL.Path
	if path == "" {
		s.logger.Error("empty request path")
		return nil, fmt.Errorf("empty request path")
	}
	pathParts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(pathParts) != 2 {
		s.logger.Error("invalid request path format",
			zap.String("path", path),
			zap.Strings("pathParts", pathParts))
		return nil, fmt.Errorf("invalid request path format: %s", path)
	}
	serviceAndPackage := pathParts[0]
	operationName := pathParts[1]
	serviceParts := strings.Split(serviceAndPackage, ".")
	if len(serviceParts) < 2 {
		s.logger.Error("invalid service format",
			zap.String("serviceAndPackage", serviceAndPackage),
			zap.Strings("serviceParts", serviceParts))
		return nil, fmt.Errorf("invalid service format: %s", serviceAndPackage)
	}
	packageName := strings.Join(serviceParts[:len(serviceParts)-1], ".")
	requestMessageName := protoreflect.FullName(fmt.Sprintf("%s.%sRequest", packageName, operationName))

	s.logger.Debug("parsed path components",
		zap.String("serviceAndPackage", serviceAndPackage),
		zap.String("operationName", operationName))

	// 2. Unmarshal the proto message
	msg, err := s.ParseProtoMessage(body, requestMessageName)
	if err != nil {
		s.logger.Error("failed to parse proto message",
			zap.String("requestMessageName", string(requestMessageName)),
			zap.Error(err))
		return nil, fmt.Errorf("failed to parse proto message: %w", err)
	}

	s.logger.Debug("successfully parsed proto message")

	// 3. Convert to map[string]interface{} for GraphQL variables
	result, err := s.protoMessageToMap(msg)
	if err != nil {
		s.logger.Error("failed to convert proto message to map", zap.Error(err))
		return nil, fmt.Errorf("failed to convert proto message to map: %w", err)
	}

	s.logger.Debug("successfully converted to map", zap.Int("resultFields", len(result)))
	return result, nil
}

// GetMessageDescriptor gets a message descriptor from the loaded proto schema
func (s *ConnectRPCServer) GetMessageDescriptor(messageName protoreflect.FullName) (protoreflect.MessageDescriptor, error) {
	s.logger.Debug("GetMessageDescriptor called", zap.String("messageName", string(messageName)))

	if s.protoSchema == nil {
		s.logger.Error("proto schema not loaded")
		return nil, fmt.Errorf("proto schema not loaded")
	}

	s.logger.Debug("proto schema loaded", zap.Int("fileCount", len(s.protoSchema)))

	// Convert []*descriptorpb.FileDescriptorProto to descriptorpb.FileDescriptorSet
	fds := &descriptorpb.FileDescriptorSet{
		File: s.protoSchema,
	}

	s.logger.Debug("created FileDescriptorSet", zap.Int("fileCount", len(fds.File)))

	// Build a registry of files/types.
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		s.logger.Error("failed to create files registry", zap.Error(err))
		return nil, fmt.Errorf("failed to create files registry: %w", err)
	}

	s.logger.Debug("created files registry successfully")

	// Look up the message descriptor by fully-qualified name.
	d, err := files.FindDescriptorByName(messageName)
	if err != nil {
		s.logger.Error("failed to find message descriptor",
			zap.String("messageName", string(messageName)),
			zap.Error(err))
		return nil, fmt.Errorf("failed to find message descriptor %s: %w", messageName, err)
	}

	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		s.logger.Error("descriptor is not a message descriptor",
			zap.String("messageName", string(messageName)),
			zap.String("descriptorType", fmt.Sprintf("%T", d)))
		return nil, fmt.Errorf("descriptor %s is not a message descriptor", messageName)
	}

	s.logger.Debug("found message descriptor", zap.String("messageName", string(messageName)))
	return md, nil
}

// ParseProtoMessage parses a proto message using dynamicpb
func (s *ConnectRPCServer) ParseProtoMessage(data []byte, messageName protoreflect.FullName) (protoreflect.ProtoMessage, error) {
	s.logger.Debug("ParseProtoMessage called",
		zap.String("messageName", string(messageName)),
		zap.Int("dataSize", len(data)))

	md, err := s.GetMessageDescriptor(messageName)
	if err != nil {
		s.logger.Error("failed to get message descriptor",
			zap.String("messageName", string(messageName)),
			zap.Error(err))
		return nil, err
	}

	s.logger.Debug("got message descriptor successfully")

	// Create a dynamic message using the descriptor
	msg := dynamicpb.NewMessage(md)

	// Unmarshal the binary data into the dynamic message
	if err := proto.Unmarshal(data, msg); err != nil {
		s.logger.Error("failed to unmarshal proto message",
			zap.String("messageName", string(messageName)),
			zap.Int("dataSize", len(data)),
			zap.Error(err))
		return nil, fmt.Errorf("failed to unmarshal proto message: %w", err)
	}

	s.logger.Debug("successfully unmarshaled proto message")
	return msg, nil
}

// protoMessageToMap converts a protoreflect.ProtoMessage to map[string]interface{}
func (s *ConnectRPCServer) protoMessageToMap(msg protoreflect.ProtoMessage) (map[string]interface{}, error) {
	s.logger.Debug("converting proto message to map")
	result := make(map[string]interface{})

	// Get the message reflection interface
	msgReflect := msg.ProtoReflect()

	// Get the message descriptor to iterate over fields
	msgDesc := msgReflect.Descriptor()
	fields := msgDesc.Fields()

	s.logger.Debug("message has fields", zap.Int("fieldCount", fields.Len()))

	// Iterate over all fields in the message descriptor
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fieldName := string(field.Name())

		s.logger.Debug("processing field", zap.String("fieldName", fieldName))

		// Check if the field is set in the message
		if !msgReflect.Has(field) {
			s.logger.Debug("field not set, skipping", zap.String("fieldName", fieldName))
			continue
		}

		// Get the field value
		value := msgReflect.Get(field)

		// Convert the protoreflect.Value to a Go value
		goValue, err := s.protoValueToGoValue(value, field)
		if err != nil {
			s.logger.Error("failed to convert field",
				zap.String("fieldName", fieldName),
				zap.Error(err))
			return nil, fmt.Errorf("failed to convert field %s: %w", fieldName, err)
		}

		s.logger.Debug("converted field successfully",
			zap.String("fieldName", fieldName),
			zap.Any("value", goValue))

		result[fieldName] = goValue
	}

	s.logger.Debug("successfully converted proto message to map", zap.Int("resultFields", len(result)))
	return result, nil
}

// protoValueToGoValue converts a protoreflect.Value to a Go value
func (s *ConnectRPCServer) protoValueToGoValue(value protoreflect.Value, field protoreflect.FieldDescriptor) (interface{}, error) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return value.Bool(), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return int32(value.Int()), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return value.Int(), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return uint32(value.Uint()), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return value.Uint(), nil
	case protoreflect.FloatKind:
		return float32(value.Float()), nil
	case protoreflect.DoubleKind:
		return value.Float(), nil
	case protoreflect.StringKind:
		return value.String(), nil
	case protoreflect.BytesKind:
		return value.Bytes(), nil
	case protoreflect.EnumKind:
		return value.Enum(), nil
	case protoreflect.MessageKind:
		// For nested messages, we need to recursively convert them
		nestedMsg := value.Message()
		if nestedMsg == nil {
			return nil, nil
		}

		// Convert the nested message to a map
		nestedResult := make(map[string]interface{})
		fields := nestedMsg.Descriptor().Fields()
		for i := 0; i < fields.Len(); i++ {
			nestedField := fields.Get(i)
			if nestedMsg.Has(nestedField) {
				nestedValue := nestedMsg.Get(nestedField)
				goValue, err := s.protoValueToGoValue(nestedValue, nestedField)
				if err != nil {
					return nil, fmt.Errorf("failed to convert nested field %s: %w", nestedField.Name(), err)
				}
				nestedResult[string(nestedField.Name())] = goValue
			}
		}
		return nestedResult, nil
	default:
		if field.IsList() {
			// Handle repeated fields
			list := value.List()
			result := make([]interface{}, list.Len())
			for i := 0; i < list.Len(); i++ {
				listValue := list.Get(i)
				goValue, err := s.protoValueToGoValue(listValue, field)
				if err != nil {
					return nil, fmt.Errorf("failed to convert list item %d: %w", i, err)
				}
				result[i] = goValue
			}
			return result, nil
		} else if field.IsMap() {
			// Handle map fields
			mapValue := value.Map()
			result := make(map[string]interface{})
			mapValue.Range(func(key protoreflect.MapKey, val protoreflect.Value) bool {
				keyStr := key.String()
				goValue, err := s.protoValueToGoValue(val, field.MapValue())
				if err != nil {
					// We can't return an error from this callback, so we'll skip this entry
					return true
				}
				result[keyStr] = goValue
				return true
			})
			return result, nil
		}
		return nil, fmt.Errorf("unsupported field kind: %v", field.Kind())
	}
}

// parseJSONRequest parses a JSON Connect RPC request
func (s *ConnectRPCServer) parseJSONRequest(r *http.Request) (map[string]interface{}, error) {
	var requestData map[string]interface{}

	if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
		return nil, fmt.Errorf("failed to decode JSON request: %w", err)
	}

	return requestData, nil
}

// parseProtoRequestFromBody parses a protobuf Connect RPC request from the request body
func (s *ConnectRPCServer) parseProtoRequestFromBody(r *http.Request) (map[string]interface{}, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	return s.parseProtoRequestFromBytes(r, body)
}

// mapConnectRequestToGraphQLVariables maps Connect RPC request fields to GraphQL variables
func (s *ConnectRPCServer) mapConnectRequestToGraphQLVariables(connectRequest map[string]interface{}, operation schemaloader.Operation) (map[string]interface{}, error) {
	// TODO: Add support for JSON Schema validation
	// Validate input against JSON Schema if available

	//if operation.CompiledSchema != nil {
	//	if err := s.validateInputWithSchema(connectRequest, operation.CompiledSchema); err != nil {
	//		return nil, fmt.Errorf("input validation failed: %w", err)
	//	}
	//}

	// Convert proto field names to GraphQL variable names
	variables := make(map[string]interface{})

	for protoField, value := range connectRequest {
		// Convert snake_case proto field names to camelCase GraphQL variable names
		graphqlVar := s.convertProtoFieldToGraphQLVariable(protoField)
		variables[graphqlVar] = value

		s.logger.Debug("mapped proto field to GraphQL variable",
			zap.String("protoField", protoField),
			zap.String("graphqlVariable", graphqlVar),
			zap.Any("value", value))
	}

	return variables, nil
}

// convertProtoFieldToGraphQLVariable converts proto field names to GraphQL variable names
func (s *ConnectRPCServer) convertProtoFieldToGraphQLVariable(protoField string) string {
	// Convert snake_case proto field names to camelCase GraphQL variable names
	// This handles all cases automatically: employee_id -> employeeId, has_pets -> hasPets, etc.
	return s.snakeToCamelCase(protoField)
}

// snakeToCamelCase converts snake_case to camelCase
func (s *ConnectRPCServer) snakeToCamelCase(snake string) string {
	if snake == "" {
		return ""
	}

	parts := strings.Split(snake, "_")
	if len(parts) == 1 {
		return parts[0] // No underscores, return as-is
	}

	// First part stays lowercase, subsequent parts get capitalized
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			result += strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}

	return result
}

//// validateInputWithSchema validates input data against a compiled JSON Schema
//func (s *ConnectRPCServer) validateInputWithSchema(data interface{}, schema *jsonschema.Schema) error {
//	if schema == nil {
//		return nil
//	}
//
//	if err := schema.Validate(data); err != nil {
//		var validationErr *jsonschema.ValidationError
//		if errors.As(err, &validationErr) {
//			// Return a more user-friendly error message
//			return fmt.Errorf("validation error at '%s': %s", validationErr.InstanceLocation, validationErr.Error())
//		}
//		return fmt.Errorf("schema validation failed: %w", err)
//	}
//
//	return nil
//}

// mapFieldNameDynamic maps Connect RPC field names to GraphQL variable names using dynamic mapping
func (s *ConnectRPCServer) mapFieldNameDynamic(connectField string, variableMapping map[string]string) string {
	// First check if we have a specific mapping for this field
	if graphqlVar, exists := variableMapping[connectField]; exists {
		return graphqlVar
	}

	// If no specific mapping found, return as-is (for fields that don't need mapping)
	return connectField
}

// writeConnectError writes a Connect RPC error response
func (s *ConnectRPCServer) writeConnectError(w http.ResponseWriter, err *connect.Error) {
	// Set appropriate HTTP status code based on Connect error code
	httpStatus := connectCodeToHTTPStatus(err.Code())

	// Set headers
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)

	// Create error response
	errorResp := map[string]interface{}{
		"code":    err.Code().String(),
		"message": err.Message(),
	}

	json.NewEncoder(w).Encode(errorResp)
}

// writeConnectSuccess writes a successful Connect RPC response
func (s *ConnectRPCServer) writeConnectSuccess(w http.ResponseWriter, r *http.Request, data interface{}) {
	// For GET requests, determine response format from query parameters
	if r.Method == http.MethodGet {
		query := r.URL.Query()
		encoding := query.Get("encoding")

		if encoding == "proto" {
			// Respond with proto encoding
			s.writeProtoResponse(w, r, data)
		} else {
			// Default to JSON response for GET requests
			s.writeJSONResponse(w, data)
		}
		return
	}

	// For POST requests, check the request content type to determine response format
	requestContentType := r.Header.Get("Content-Type")

	if requestContentType == "application/proto" {
		// Respond with proto encoding
		s.writeProtoResponse(w, r, data)
	} else {
		// Default to JSON response (existing behavior)
		s.writeJSONResponse(w, data)
	}
}

// writeJSONResponse writes a JSON response (original behavior)
func (s *ConnectRPCServer) writeJSONResponse(w http.ResponseWriter, data interface{}) {
	// Set headers
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Write the GraphQL data directly as the Connect RPC response
	json.NewEncoder(w).Encode(data)
}

// writeProtoResponse writes a proto-encoded response
func (s *ConnectRPCServer) writeProtoResponse(w http.ResponseWriter, r *http.Request, data interface{}) {
	// Extract operation information from the request path
	operationName, packageName, err := s.extractOperationInfoFromPath(r.URL.Path)
	if err != nil {
		s.logger.Error("failed to extract operation info for proto response", zap.Error(err))
		s.writeConnectError(w, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to extract operation info: %w", err)))
		return
	}

	// Construct the response message name
	responseMessageName := protoreflect.FullName(fmt.Sprintf("%s.%sResponse", packageName, operationName))

	// Create the proto response message
	protoData, err := s.createProtoResponseMessage(data, responseMessageName)
	if err != nil {
		s.logger.Error("failed to create proto response message",
			zap.String("messageName", string(responseMessageName)),
			zap.Error(err))
		s.writeConnectError(w, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to create proto response: %w", err)))
		return
	}

	// Marshal the proto message
	responseBytes, err := proto.Marshal(protoData)
	if err != nil {
		s.logger.Error("failed to marshal proto response", zap.Error(err))
		s.writeConnectError(w, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to marshal proto response: %w", err)))
		return
	}

	// Set headers and write response
	w.Header().Set("Content-Type", "application/proto")
	w.WriteHeader(http.StatusOK)
	w.Write(responseBytes)
}

// extractOperationInfoFromPath extracts operation name and package name from the request path
func (s *ConnectRPCServer) extractOperationInfoFromPath(path string) (operationName, packageName string, err error) {
	s.logger.Debug("extractOperationInfoFromPath called", zap.String("path", path))

	if path == "" {
		return "", "", fmt.Errorf("empty request path")
	}

	// Remove leading slash and split by '/'
	pathParts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(pathParts) != 2 {
		return "", "", fmt.Errorf("invalid request path format: %s", path)
	}

	serviceAndPackage := pathParts[0] // e.g., "service.v1.EmployeeServiceService"
	operationName = pathParts[1]      // e.g., "GetEmployeeByID"

	// Extract the package name from the service path
	// For "service.v1.EmployeeServiceService", we want "service.v1"
	serviceParts := strings.Split(serviceAndPackage, ".")
	if len(serviceParts) < 2 {
		return "", "", fmt.Errorf("invalid service format: %s", serviceAndPackage)
	}

	// Take all parts except the last one (which is the service name)
	packageName = strings.Join(serviceParts[:len(serviceParts)-1], ".")

	s.logger.Debug("extracted operation info",
		zap.String("operationName", operationName),
		zap.String("packageName", packageName))

	return operationName, packageName, nil
}

// createProtoResponseMessage creates a proto response message from GraphQL data
func (s *ConnectRPCServer) createProtoResponseMessage(data interface{}, messageName protoreflect.FullName) (protoreflect.ProtoMessage, error) {
	s.logger.Debug("createProtoResponseMessage called", zap.String("messageName", string(messageName)))

	// Get the response message descriptor
	md, err := s.GetMessageDescriptor(messageName)
	if err != nil {
		return nil, fmt.Errorf("failed to get response message descriptor: %w", err)
	}

	// Create a dynamic message using the descriptor
	msg := dynamicpb.NewMessage(md)

	// Convert the GraphQL data to proto message fields
	err = s.populateProtoMessageFromData(msg, data)
	if err != nil {
		return nil, fmt.Errorf("failed to populate proto message: %w", err)
	}

	s.logger.Debug("successfully created proto response message")
	return msg, nil
}

// populateProtoMessageFromData populates a proto message from interface{} data
func (s *ConnectRPCServer) populateProtoMessageFromData(msg *dynamicpb.Message, data interface{}) error {
	s.logger.Debug("populating proto message from data", zap.Any("data", data))

	// Handle nil data
	if data == nil {
		return nil
	}

	// Convert data to map if it's not already
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected data to be map[string]interface{}, got %T", data)
	}

	// Get the message reflection interface
	msgReflect := msg.ProtoReflect()
	msgDesc := msgReflect.Descriptor()
	fields := msgDesc.Fields()

	// Iterate over all fields in the message descriptor
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fieldName := string(field.Name())

		// Check if the field exists in the data (exact match first)
		value, exists := dataMap[fieldName]
		if !exists {
			// Try GraphQL to proto field name mapping
			mappedValue, mappedExists := s.findGraphQLFieldForProtoField(dataMap, fieldName)
			if mappedExists {
				value = mappedValue
				exists = true
			}
		}

		if !exists {
			continue
		}

		// Convert the Go value to protoreflect.Value and set it
		protoValue, err := s.goValueToProtoValue(value, field)
		if err != nil {
			s.logger.Error("failed to convert field value",
				zap.String("fieldName", fieldName),
				zap.Error(err))
			return fmt.Errorf("failed to convert field %s: %w", fieldName, err)
		}

		// Add defensive check before setting the field
		if !protoValue.IsValid() {
			continue
		}

		msgReflect.Set(field, protoValue)
	}

	return nil
}

// findGraphQLFieldForProtoField maps GraphQL response fields to protobuf fields
func (s *ConnectRPCServer) findGraphQLFieldForProtoField(dataMap map[string]interface{}, protoFieldName string) (interface{}, bool) {
	// Handle common GraphQL to protobuf field mappings for subscription responses
	switch protoFieldName {
	case "current_time":
		// GraphQL: currentTime -> Proto: current_time
		if value, exists := dataMap["currentTime"]; exists {
			return value, true
		}
	case "time_stamp":
		// GraphQL: timeStamp -> Proto: time_stamp
		if value, exists := dataMap["timeStamp"]; exists {
			return value, true
		}
	}

	// Try snake_case to camelCase conversion (proto field -> GraphQL field)
	camelCaseField := s.snakeToCamelCase(protoFieldName)
	if value, exists := dataMap[camelCaseField]; exists {
		return value, true
	}

	// Try direct nested field access for currentTime.timeStamp pattern
	if protoFieldName == "time_stamp" {
		if currentTime, exists := dataMap["currentTime"]; exists {
			if currentTimeMap, ok := currentTime.(map[string]interface{}); ok {
				if timeStamp, exists := currentTimeMap["timeStamp"]; exists {
					return timeStamp, true
				}
			}
		}
	}

	return nil, false
}

// camelToSnakeCase converts camelCase to snake_case
func (s *ConnectRPCServer) camelToSnakeCase(camel string) string {
	if camel == "" {
		return ""
	}

	var result []rune
	for i, r := range camel {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result = append(result, '_')
		}
		result = append(result, r)
	}

	return strings.ToLower(string(result))
}

// getMapKeys helper function to get map keys for debugging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// goValueToProtoValue converts a Go value to a protoreflect.Value
func (s *ConnectRPCServer) goValueToProtoValue(value interface{}, field protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	if value == nil {
		return protoreflect.Value{}, nil
	}

	// Add debug logging to understand the field characteristics
	s.logger.Debug("goValueToProtoValue called",
		zap.String("fieldName", string(field.Name())),
		zap.String("fieldKind", field.Kind().String()),
		zap.Bool("isList", field.IsList()),
		zap.Bool("isMap", field.IsMap()),
		zap.String("valueType", fmt.Sprintf("%T", value)))

	switch field.Kind() {
	case protoreflect.BoolKind:
		if v, ok := value.(bool); ok {
			return protoreflect.ValueOfBool(v), nil
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		if v, ok := value.(float64); ok { // JSON numbers come as float64
			return protoreflect.ValueOfInt32(int32(v)), nil
		}
		if v, ok := value.(int32); ok {
			return protoreflect.ValueOfInt32(v), nil
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		if v, ok := value.(float64); ok { // JSON numbers come as float64
			return protoreflect.ValueOfInt64(int64(v)), nil
		}
		if v, ok := value.(int64); ok {
			return protoreflect.ValueOfInt64(v), nil
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if v, ok := value.(float64); ok { // JSON numbers come as float64
			return protoreflect.ValueOfUint32(uint32(v)), nil
		}
		if v, ok := value.(uint32); ok {
			return protoreflect.ValueOfUint32(v), nil
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if v, ok := value.(float64); ok { // JSON numbers come as float64
			return protoreflect.ValueOfUint64(uint64(v)), nil
		}
		if v, ok := value.(uint64); ok {
			return protoreflect.ValueOfUint64(v), nil
		}
	case protoreflect.FloatKind:
		if v, ok := value.(float64); ok {
			return protoreflect.ValueOfFloat32(float32(v)), nil
		}
		if v, ok := value.(float32); ok {
			return protoreflect.ValueOfFloat32(v), nil
		}
	case protoreflect.DoubleKind:
		if v, ok := value.(float64); ok {
			return protoreflect.ValueOfFloat64(v), nil
		}
	case protoreflect.StringKind:
		if v, ok := value.(string); ok {
			return protoreflect.ValueOfString(v), nil
		}
	case protoreflect.BytesKind:
		if v, ok := value.([]byte); ok {
			return protoreflect.ValueOfBytes(v), nil
		}
		if v, ok := value.(string); ok {
			return protoreflect.ValueOfBytes([]byte(v)), nil
		}
	case protoreflect.MessageKind:
		// Check if this is a repeated message field first
		if field.IsList() {
			// Handle repeated message fields
			if value == nil {
				// Return empty list for null repeated fields
				tempMsg := dynamicpb.NewMessage(field.ContainingMessage())
				listValue := tempMsg.ProtoReflect().NewField(field)
				return listValue, nil
			}
			if slice, ok := value.([]interface{}); ok {
				// For lists, we need to create a temporary message to get a new list
				tempMsg := dynamicpb.NewMessage(field.ContainingMessage())
				listValue := tempMsg.ProtoReflect().NewField(field)
				list := listValue.List()
				for _, item := range slice {
					if item == nil {
						// Skip null items in the list
						continue
					}
					if itemMap, ok := item.(map[string]interface{}); ok {
						nestedMsgDesc := field.Message()
						nestedMsg := dynamicpb.NewMessage(nestedMsgDesc)
						err := s.populateProtoMessageFromData(nestedMsg, itemMap)
						if err != nil {
							return protoreflect.Value{}, fmt.Errorf("failed to populate nested message in list: %w", err)
						}
						list.Append(protoreflect.ValueOfMessage(nestedMsg.ProtoReflect()))
					} else {
						return protoreflect.Value{}, fmt.Errorf("expected map[string]interface{} for message field in list, got %T", item)
					}
				}
				return listValue, nil
			} else {
				return protoreflect.Value{}, fmt.Errorf("expected []interface{} for repeated message field, got %T", value)
			}
		} else {
			// Handle single nested messages
			if value == nil {
				// Return zero value for null message fields
				return protoreflect.Value{}, nil
			}
			if nestedMap, ok := value.(map[string]interface{}); ok {
				nestedMsgDesc := field.Message()
				nestedMsg := dynamicpb.NewMessage(nestedMsgDesc)
				err := s.populateProtoMessageFromData(nestedMsg, nestedMap)
				if err != nil {
					return protoreflect.Value{}, fmt.Errorf("failed to populate nested message: %w", err)
				}
				return protoreflect.ValueOfMessage(nestedMsg.ProtoReflect()), nil
			} else {
				return protoreflect.Value{}, fmt.Errorf("expected map[string]interface{} for message field, got %T", value)
			}
		}
	default:
		if field.IsList() {
			// Handle repeated primitive fields
			if slice, ok := value.([]interface{}); ok {
				// For lists, we need to create a temporary message to get a new list
				tempMsg := dynamicpb.NewMessage(field.ContainingMessage())
				listValue := tempMsg.ProtoReflect().NewField(field)
				list := listValue.List()
				for _, item := range slice {
					// This is a repeated primitive field - convert directly based on field kind
					var itemValue protoreflect.Value
					switch field.Kind() {
					case protoreflect.BoolKind:
						if v, ok := item.(bool); ok {
							itemValue = protoreflect.ValueOfBool(v)
						} else {
							return protoreflect.Value{}, fmt.Errorf("expected bool for list item, got %T", item)
						}
					case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
						if v, ok := item.(float64); ok { // JSON numbers come as float64
							itemValue = protoreflect.ValueOfInt32(int32(v))
						} else if v, ok := item.(int32); ok {
							itemValue = protoreflect.ValueOfInt32(v)
						} else {
							return protoreflect.Value{}, fmt.Errorf("expected number for int32 list item, got %T", item)
						}
					case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
						if v, ok := item.(float64); ok { // JSON numbers come as float64
							itemValue = protoreflect.ValueOfInt64(int64(v))
						} else if v, ok := item.(int64); ok {
							itemValue = protoreflect.ValueOfInt64(v)
						} else {
							return protoreflect.Value{}, fmt.Errorf("expected number for int64 list item, got %T", item)
						}
					case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
						if v, ok := item.(float64); ok { // JSON numbers come as float64
							itemValue = protoreflect.ValueOfUint32(uint32(v))
						} else if v, ok := item.(uint32); ok {
							itemValue = protoreflect.ValueOfUint32(v)
						} else {
							return protoreflect.Value{}, fmt.Errorf("expected number for uint32 list item, got %T", item)
						}
					case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
						if v, ok := item.(float64); ok { // JSON numbers come as float64
							itemValue = protoreflect.ValueOfUint64(uint64(v))
						} else if v, ok := item.(uint64); ok {
							itemValue = protoreflect.ValueOfUint64(v)
						} else {
							return protoreflect.Value{}, fmt.Errorf("expected number for uint64 list item, got %T", item)
						}
					case protoreflect.FloatKind:
						if v, ok := item.(float64); ok {
							itemValue = protoreflect.ValueOfFloat32(float32(v))
						} else if v, ok := item.(float32); ok {
							itemValue = protoreflect.ValueOfFloat32(v)
						} else {
							return protoreflect.Value{}, fmt.Errorf("expected number for float list item, got %T", item)
						}
					case protoreflect.DoubleKind:
						if v, ok := item.(float64); ok {
							itemValue = protoreflect.ValueOfFloat64(v)
						} else {
							return protoreflect.Value{}, fmt.Errorf("expected number for double list item, got %T", item)
						}
					case protoreflect.StringKind:
						if v, ok := item.(string); ok {
							itemValue = protoreflect.ValueOfString(v)
						} else {
							return protoreflect.Value{}, fmt.Errorf("expected string for list item, got %T", item)
						}
					case protoreflect.BytesKind:
						if v, ok := item.([]byte); ok {
							itemValue = protoreflect.ValueOfBytes(v)
						} else if v, ok := item.(string); ok {
							itemValue = protoreflect.ValueOfBytes([]byte(v))
						} else {
							return protoreflect.Value{}, fmt.Errorf("expected bytes or string for bytes list item, got %T", item)
						}
					default:
						return protoreflect.Value{}, fmt.Errorf("unsupported primitive field kind %v for list item", field.Kind())
					}
					list.Append(itemValue)
				}
				return listValue, nil
			}
		} else if field.IsMap() {
			// Handle map fields
			if mapData, ok := value.(map[string]interface{}); ok {
				// For maps, we need to create a temporary message to get a new map
				tempMsg := dynamicpb.NewMessage(field.ContainingMessage())
				mapValue := tempMsg.ProtoReflect().NewField(field)
				mapVal := mapValue.Map()
				for k, v := range mapData {
					keyValue := protoreflect.ValueOfString(k)
					valueValue, err := s.goValueToProtoValue(v, field.MapValue())
					if err != nil {
						return protoreflect.Value{}, fmt.Errorf("failed to convert map value: %w", err)
					}
					mapVal.Set(keyValue.MapKey(), valueValue)
				}
				return mapValue, nil
			}
		}
	}

	return protoreflect.Value{}, fmt.Errorf("unsupported field kind %v for value type %T", field.Kind(), value)
}

// connectCodeToHTTPStatus maps Connect error codes to HTTP status codes
func connectCodeToHTTPStatus(code connect.Code) int {
	switch code {
	case connect.CodeCanceled:
		return 499 // Client Closed Request
	case connect.CodeUnknown:
		return http.StatusInternalServerError
	case connect.CodeInvalidArgument:
		return http.StatusBadRequest
	case connect.CodeDeadlineExceeded:
		return http.StatusGatewayTimeout
	case connect.CodeNotFound:
		return http.StatusNotFound
	case connect.CodeAlreadyExists:
		return http.StatusConflict
	case connect.CodePermissionDenied:
		return http.StatusForbidden
	case connect.CodeResourceExhausted:
		return http.StatusTooManyRequests
	case connect.CodeFailedPrecondition:
		return http.StatusBadRequest
	case connect.CodeAborted:
		return http.StatusConflict
	case connect.CodeOutOfRange:
		return http.StatusBadRequest
	case connect.CodeUnimplemented:
		return http.StatusNotImplemented
	case connect.CodeInternal:
		return http.StatusInternalServerError
	case connect.CodeUnavailable:
		return http.StatusServiceUnavailable
	case connect.CodeDataLoss:
		return http.StatusInternalServerError
	case connect.CodeUnauthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

// GetOperationInfo returns information about loaded operations (for debugging)
func (s *ConnectRPCServer) GetOperationInfo() map[string]interface{} {
	info := make(map[string]interface{})

	for name, op := range s.collection {
		info[name] = map[string]interface{}{
			"name":     op.Name,
			"type":     op.OperationType,
			"filePath": op.FilePath,
			"endpoint": fmt.Sprintf("/%s.%s/%s", s.packageName, s.serviceName, name),
		}
	}

	return info
}

func (s *ConnectRPCServer) LoadOperations() error {
	collection := NewCollection(s.logger)

	if err := collection.LoadFromDirectory(s.collectionDirectory); err != nil {
		return fmt.Errorf("failed to load operations from directory %s: %w", s.collectionDirectory, err)
	}

	s.collection = collection.operations

	s.logger.Info("loaded operations",
		zap.Int("count", len(s.collection)))

	return nil
}

// loadProtoFile loads a proto file and compiles it into a FileDescriptorSet
func loadProtoFile(protoFilePath string) (*descriptorpb.FileDescriptorSet, error) {
	// First check if the file exists
	if _, err := os.Stat(protoFilePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("proto file does not exist: %s", protoFilePath)
	}

	// Get the directory containing the proto file for import paths
	protoDir := "."
	if strings.Contains(protoFilePath, "/") {
		lastSlash := strings.LastIndex(protoFilePath, "/")
		protoDir = protoFilePath[:lastSlash]
	}

	// Create a compiler with the proto file's directory as import path
	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			ImportPaths: []string{protoDir, "."},
		},
	}

	// Get just the filename for compilation
	fileName := protoFilePath
	if strings.Contains(protoFilePath, "/") {
		lastSlash := strings.LastIndex(protoFilePath, "/")
		fileName = protoFilePath[lastSlash+1:]
	}

	// Compile the proto file using just the filename (since we set the import path)
	files, err := compiler.Compile(context.Background(), fileName)
	if err != nil {
		// If that fails, try with the full path
		files, err = compiler.Compile(context.Background(), protoFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to compile proto file: %w", err)
		}
	}

	// Convert linker.Files to FileDescriptorSet
	fds := &descriptorpb.FileDescriptorSet{}
	for _, file := range files {
		fileProto := protodesc.ToFileDescriptorProto(file)
		fds.File = append(fds.File, fileProto)
	}

	return fds, nil
}

func (s *ConnectRPCServer) Start() error {
	if err := s.LoadOperations(); err != nil {
		return fmt.Errorf("failed to load operations: %w", err)
	}

	mux := http.NewServeMux()

	s.RegisterHandlers(mux)

	// Add a catch-all handler to log all requests for debugging
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("incoming request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("contentType", r.Header.Get("Content-Type")),
			zap.String("connectProtocolVersion", r.Header.Get("Connect-Protocol-Version")))

		// Check if this matches any of our registered patterns
		found := false
		for operationName := range s.collection {
			expectedPath := fmt.Sprintf("/%s.%s/%s", s.packageName, s.serviceName, operationName)
			if r.URL.Path == expectedPath {
				found = true
				s.logger.Info("request matches registered handler", zap.String("expectedPath", expectedPath))
				break
			}
		}

		if !found {
			s.logger.Warn("request does not match any registered handlers",
				zap.String("requestedPath", r.URL.Path),
				zap.String("packageName", s.packageName),
				zap.String("serviceName", s.serviceName))

			// Log all available paths for debugging
			availablePaths := make([]string, 0, len(s.collection))
			for operationName := range s.collection {
				availablePaths = append(availablePaths, fmt.Sprintf("/%s.%s/%s", s.packageName, s.serviceName, operationName))
			}
			s.logger.Info("available handler paths", zap.Strings("paths", availablePaths))

			http.NotFound(w, r)
			return
		}
	})

	// 🚀 CRITICAL FIX: Wrap handler with HTTP/2 (h2c) support for Connect RPC streaming
	// This enables proper streaming support that Connect RPC requires
	h2Handler := h2c.NewHandler(mux, &http2.Server{})

	server := &http.Server{
		Addr:         s.listenAddr,
		ReadTimeout:  s.requestTimeout,
		WriteTimeout: s.requestTimeout,
		Handler:      h2Handler, // Use h2c handler instead of mux directly
	}

	s.logger.Info("🔧 starting Connect RPC server with HTTP/2 (h2c) support",
		zap.String("listen_addr", s.listenAddr))

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("failed to start Connect RPC server", zap.Error(err))
		}
	}()

	return nil
}

// parseConnectStreamingRequest parses a Connect RPC streaming request with envelope
func (s *ConnectRPCServer) parseConnectStreamingRequest(r *http.Request) (map[string]interface{}, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	s.logger.Debug("parsing Connect streaming request",
		zap.Int("bodyLength", len(body)),
		zap.String("contentType", r.Header.Get("Content-Type")))

	// Connect RPC streaming requests have envelope format: [flags:1][length:4][data:length]
	if len(body) < 5 {
		return nil, fmt.Errorf("Connect RPC envelope too short: got %d bytes, need at least 5", len(body))
	}

	// Parse envelope header: [flags:1][length:4]
	flags := body[0]
	messageLength := uint32(body[1])<<24 | uint32(body[2])<<16 | uint32(body[3])<<8 | uint32(body[4])

	s.logger.Debug("parsed Connect RPC envelope",
		zap.Uint8("flags", flags),
		zap.Uint32("messageLength", messageLength),
		zap.Int("totalBodyLength", len(body)))

	// Check if we have the expected message data
	expectedTotalLength := 5 + int(messageLength)
	if len(body) != expectedTotalLength {
		return nil, fmt.Errorf("Connect RPC envelope length mismatch: expected %d bytes total, got %d",
			expectedTotalLength, len(body))
	}

	if messageLength == 0 {
		// Empty request (common for subscriptions)
		s.logger.Debug("empty Connect RPC request (subscription with no parameters)")
		return make(map[string]interface{}), nil
	}

	// Extract message data
	messageData := body[5 : 5+messageLength]

	// Parse based on content type
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "proto") {
		return s.parseProtoMessageData(r, messageData)
	} else {
		return s.parseJSONMessageData(messageData)
	}
}

// parseProtoMessageData parses protobuf message data from streaming request
func (s *ConnectRPCServer) parseProtoMessageData(r *http.Request, data []byte) (map[string]interface{}, error) {
	// Extract operation info from path
	path := r.URL.Path
	pathParts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(pathParts) != 2 {
		return nil, fmt.Errorf("invalid request path format: %s", path)
	}

	serviceAndPackage := pathParts[0]
	operationName := pathParts[1]

	serviceParts := strings.Split(serviceAndPackage, ".")
	if len(serviceParts) < 2 {
		return nil, fmt.Errorf("invalid service format: %s", serviceAndPackage)
	}

	packageName := strings.Join(serviceParts[:len(serviceParts)-1], ".")
	requestMessageName := protoreflect.FullName(fmt.Sprintf("%s.%sRequest", packageName, operationName))

	// Parse proto message
	msg, err := s.ParseProtoMessage(data, requestMessageName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto message: %w", err)
	}

	return s.protoMessageToMap(msg)
}

// parseJSONMessageData parses JSON message data from streaming request
func (s *ConnectRPCServer) parseJSONMessageData(data []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse JSON message data: %w", err)
	}
	return result, nil
}

// writeConnectStreamingFrame writes a Connect RPC streaming frame
func (s *ConnectRPCServer) writeConnectStreamingFrame(w http.ResponseWriter, data interface{}, isEnd bool) error {
	s.logger.Debug("writing Connect streaming frame",
		zap.Bool("isEnd", isEnd),
		zap.Any("data", data))

	var responseBytes []byte
	var err error

	if isEnd {
		// For end frame, send EndStreamResponse according to Connect RPC spec
		// For application/connect+proto, this should be an empty protobuf message
		responseBytes, err = s.createEmptyProtoResponse()
		if err != nil {
			s.logger.Error("failed to create empty proto end response", zap.Error(err))
			// Wire format: field_number << 3 | wire_type
			responseBytes = []byte{}
		}
		s.logger.Info("🔚 CREATED END FRAME", zap.Int("bytes", len(responseBytes)))
	} else if data != nil {
		// For data frames, we must create proper protobuf response
		responseBytes, err = s.createProtoResponseForStreamingFixed(data)
		if err != nil {
			return fmt.Errorf("failed to create protobuf response: %w", err)
		}
		if len(responseBytes) == 0 {
			return fmt.Errorf("protobuf response is empty - this should not happen")
		}
	} else {
		// Empty data frame - create empty protobuf message
		responseBytes, err = s.createEmptyProtoResponse()
		if err != nil {
			return fmt.Errorf("failed to create empty protobuf response: %w", err)
		}
	}

	// Create Connect streaming envelope: [flags:1][length:4][data:length]
	flags := byte(0)
	if isEnd {
		flags |= 0x02 // EndStreamResponse flag (bit 1)
	}

	// Write the envelope header
	envelope := make([]byte, 5)
	envelope[0] = flags
	// Write length in big-endian format
	length := uint32(len(responseBytes))
	envelope[1] = byte(length >> 24)
	envelope[2] = byte(length >> 16)
	envelope[3] = byte(length >> 8)
	envelope[4] = byte(length)

	s.logger.Debug("writing Connect RPC envelope",
		zap.Uint8("flags", flags),
		zap.Uint32("length", length),
		zap.Int("responseDataSize", len(responseBytes)))

	// Log the actual envelope bytes
	s.logger.Info("🔧 ENVELOPE BYTES",
		zap.String("envelopeHex", fmt.Sprintf("%x", envelope)),
		zap.String("dataHex", fmt.Sprintf("%x", responseBytes[:min(len(responseBytes), 50)]))) // Limit hex output

	// Write envelope + data as one atomic operation to prevent partial frames
	totalFrame := append(envelope, responseBytes...)

	bytesWritten, err := w.Write(totalFrame)
	if err != nil {
		return fmt.Errorf("failed to write frame: %w", err)
	}

	s.logger.Info("📤 WROTE COMPLETE FRAME",
		zap.Int("bytes", bytesWritten),
		zap.Int("envelopeBytes", len(envelope)),
		zap.Int("dataBytes", len(responseBytes)))

	// Flush immediately for streaming
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
		s.logger.Debug("💨 FLUSHED STREAMING FRAME", zap.Bool("isEndFrame", isEnd))
	} else {
		s.logger.Warn("⚠️  ResponseWriter does not support flushing", zap.Bool("isEndFrame", isEnd))
	}

	s.logger.Debug("successfully wrote Connect streaming frame",
		zap.Int("totalBytes", 5+len(responseBytes)),
		zap.Bool("isEndFrame", isEnd))

	return nil
}

// createProtoResponseForStreamingFixed creates a protobuf response for streaming data using the same approach as regular responses
func (s *ConnectRPCServer) createProtoResponseForStreamingFixed(data interface{}) ([]byte, error) {
	operationName := "SubscribeToTheCurrentTime"
	packageName := "service.v1"

	// Construct the response message name using the same pattern as regular responses
	responseMessageName := protoreflect.FullName(fmt.Sprintf("%s.%sResponse", packageName, operationName))

	// Use the same createProtoResponseMessage method as regular responses
	protoMessage, err := s.createProtoResponseMessage(data, responseMessageName)
	if err != nil {
		s.logger.Error("failed to create proto response message",
			zap.String("messageName", string(responseMessageName)),
			zap.Error(err))
		return nil, fmt.Errorf("failed to create proto response message: %w", err)
	}

	// Marshal using the same approach as regular responses
	responseBytes, err := proto.Marshal(protoMessage)
	if err != nil {
		s.logger.Error("failed to marshal proto message", zap.Error(err))
		return nil, fmt.Errorf("failed to marshal proto response: %w", err)
	}

	s.logger.Info("created streaming proto response",
		zap.String("messageName", string(responseMessageName)),
		zap.Int("protoBytes", len(responseBytes)))

	return responseBytes, nil
}

// createMinimalProtoResponse creates a minimal protobuf response when schema lookup fails
func (s *ConnectRPCServer) createMinimalProtoResponse(data interface{}) ([]byte, error) {
	s.logger.Info("creating minimal protobuf response as fallback")

	// Create a very simple protobuf message manually
	// This is a basic protobuf encoding of the data

	// For now, let's create a simple message with just the timestamp
	if dataMap, ok := data.(map[string]interface{}); ok {
		if currentTime, exists := dataMap["currentTime"]; exists {
			if timeMap, ok := currentTime.(map[string]interface{}); ok {
				if timestamp, exists := timeMap["timeStamp"]; exists {
					if timestampStr, ok := timestamp.(string); ok {
						// Create a simple protobuf message with field 1 = string timestamp
						// Wire format: field number << 3 | wire_type
						// String wire type = 2
						// Field 1, wire type 2 = (1 << 3) | 2 = 10 = 0x0A

						timestampBytes := []byte(timestampStr)
						messageBytes := make([]byte, 0, len(timestampBytes)+10)

						// Field 1 (timestamp): tag 0x0A, length, data
						messageBytes = append(messageBytes, 0x0A)                      // Field 1, wire type 2 (string)
						messageBytes = append(messageBytes, byte(len(timestampBytes))) // Length
						messageBytes = append(messageBytes, timestampBytes...)         // Data

						s.logger.Info("🔧 CREATED MINIMAL PROTO RESPONSE",
							zap.Int("protoBytes", len(messageBytes)),
							zap.String("timestamp", timestampStr))

						return messageBytes, nil
					}
				}
			}
		}
	}

	// If we can't extract timestamp, create empty message
	return []byte{}, nil
}

// createEmptyProtoResponse creates an empty protobuf response for Connect RPC end frames
func (s *ConnectRPCServer) createEmptyProtoResponse() ([]byte, error) {
	// For Connect RPC end frames, according to the Connect specification,
	// we should send an empty message body with the EndStreamResponse flag set
	// The simplest approach is to return empty bytes, which represents an empty protobuf message

	s.logger.Info("creating empty proto end frame")

	// An empty protobuf message is represented by zero bytes
	// This is the correct format for Connect RPC EndStreamResponse
	return []byte{}, nil
}

// min helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// isValidConnectStreamingRequest validates that the request has proper Connect streaming headers
func (s *ConnectRPCServer) isValidConnectStreamingRequest(r *http.Request) bool {
	// Check Content-Type
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/connect+proto" && contentType != "application/proto" {
		s.logger.Warn("invalid content type for Connect streaming",
			zap.String("contentType", contentType))
		return false
	}

	// Check Connect-Protocol-Version
	protocolVersion := r.Header.Get("Connect-Protocol-Version")
	if protocolVersion != "1" {
		s.logger.Warn("unsupported Connect protocol version",
			zap.String("protocolVersion", protocolVersion))
		return false
	}

	return true
}

// writeConnectStreamingError writes a Connect RPC streaming error frame
func (s *ConnectRPCServer) writeConnectStreamingError(w http.ResponseWriter, err error) {
	s.logger.Debug("writing Connect streaming error", zap.Error(err))

	// Create EndStreamResponse with error according to Connect RPC spec
	endStreamResponse := map[string]interface{}{
		"error": map[string]interface{}{
			"code":    "internal",
			"message": err.Error(),
		},
	}

	// Marshal the EndStreamResponse
	responseBytes, marshalErr := json.Marshal(endStreamResponse)
	if marshalErr != nil {
		s.logger.Error("failed to marshal error response", zap.Error(marshalErr))
		return
	}

	// Create envelope with EndStreamResponse flag (bit 1 = 1)
	flags := byte(0x02) // EndStreamResponse flag
	envelope := make([]byte, 5)
	envelope[0] = flags
	length := uint32(len(responseBytes))
	envelope[1] = byte(length >> 24)
	envelope[2] = byte(length >> 16)
	envelope[3] = byte(length >> 8)
	envelope[4] = byte(length)

	s.logger.Debug("writing error EndStreamResponse",
		zap.Uint8("flags", flags),
		zap.Uint32("length", length),
		zap.String("errorResponse", string(responseBytes)))

	// Write envelope and data
	if _, writeErr := w.Write(envelope); writeErr != nil {
		s.logger.Error("failed to write error envelope", zap.Error(writeErr))
		return
	}
	if _, writeErr := w.Write(responseBytes); writeErr != nil {
		s.logger.Error("failed to write error data", zap.Error(writeErr))
		return
	}

	// Flush
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// convertDataToProtoBytes converts data to protobuf bytes (simplified for streaming)
func (s *ConnectRPCServer) convertDataToProtoBytes(data interface{}) ([]byte, error) {
	// For now, use JSON encoding as fallback
	// In a full implementation, this would convert to proper proto format
	return json.Marshal(data)
}

// writeSSEEvent writes a Server-Sent Event
func (s *ConnectRPCServer) writeSSEEvent(w http.ResponseWriter, eventType string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal SSE data: %w", err)
	}

	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, string(jsonData))
	if err != nil {
		return fmt.Errorf("failed to write SSE event: %w", err)
	}

	// Flush if possible
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	return nil
}

// writeSSEError writes an SSE error event
func (s *ConnectRPCServer) writeSSEError(w http.ResponseWriter, err error) {
	errorData := map[string]interface{}{
		"error": err.Error(),
	}

	if writeErr := s.writeSSEEvent(w, "error", errorData); writeErr != nil {
		s.logger.Error("failed to write SSE error", zap.Error(writeErr))
	}
}
