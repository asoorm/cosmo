package connect_rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/bufbuild/protocompile"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/wundergraph/cosmo/router/pkg/connect_rpc/proxy"
	"github.com/wundergraph/cosmo/router/pkg/schemaloader"
	"go.uber.org/zap"
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

		handler := s.createOperationHandler(op)
		mux.Handle(listenPath, handler)
	}

	s.logger.Info("completed handler registration", zap.Int("totalHandlers", len(s.collection)))
}

// createOperationHandler creates a Connect RPC handler for a specific GraphQL operation
func (s *ConnectRPCServer) createOperationHandler(operation schemaloader.Operation) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle Connect RPC protocol
		s.handleConnectRPC(w, r, operation)
	})
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
	// Check Content-Type
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" && contentType != "application/proto" {
		return false
	}

	// Connect-Protocol-Version is optional but recommended
	// we dont require it for the PoC

	return true
}

// parseConnectRequest parses the Connect RPC request body
func (s *ConnectRPCServer) parseConnectRequest(r *http.Request) (map[string]interface{}, error) {
	contentType := r.Header.Get("Content-Type")

	switch contentType {
	case "application/json":
		return s.parseJSONRequest(r)
	case "application/proto":
		return s.parseProtoRequest(r)
	default:
		return nil, fmt.Errorf("unsupported content type: %s", contentType)
	}
}

func (s *ConnectRPCServer) parseProtoRequest(r *http.Request) (map[string]interface{}, error) {
	s.logger.Debug("parseProtoRequest called", zap.String("path", r.URL.Path))

	if s.protoSchema == nil {
		s.logger.Error("proto schema not loaded")
		return nil, fmt.Errorf("proto schema not loaded")
	}

	s.logger.Debug("proto schema is loaded", zap.Int("fileCount", len(s.protoSchema)))

	// 1. Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("failed to read request body", zap.Error(err))
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	s.logger.Debug("read request body", zap.Int("bodySize", len(body)))

	// 2. Determine the message type based on the operation
	// Extract operation name from URL path (format: /package.service/operation)
	path := r.URL.Path
	if path == "" {
		s.logger.Error("empty request path")
		return nil, fmt.Errorf("empty request path")
	}

	// Remove leading slash and split by '/'
	pathParts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(pathParts) != 2 {
		s.logger.Error("invalid request path format",
			zap.String("path", path),
			zap.Strings("pathParts", pathParts))
		return nil, fmt.Errorf("invalid request path format: %s", path)
	}

	serviceAndPackage := pathParts[0] // e.g., "service.v1.EmployeeServiceService"
	operationName := pathParts[1]     // e.g., "GetEmployeeByID"

	s.logger.Debug("parsed path components",
		zap.String("serviceAndPackage", serviceAndPackage),
		zap.String("operationName", operationName))

	// Extract the package name from the service path
	// For "service.v1.EmployeeServiceService", we want "service.v1"
	serviceParts := strings.Split(serviceAndPackage, ".")
	if len(serviceParts) < 2 {
		s.logger.Error("invalid service format",
			zap.String("serviceAndPackage", serviceAndPackage),
			zap.Strings("serviceParts", serviceParts))
		return nil, fmt.Errorf("invalid service format: %s", serviceAndPackage)
	}

	// Take all parts except the last one (which is the service name)
	packageName := strings.Join(serviceParts[:len(serviceParts)-1], ".")

	// Construct the request message name (typically OperationNameRequest)
	// For your proto: "service.v1.GetEmployeeByIDRequest"
	requestMessageName := protoreflect.FullName(fmt.Sprintf("%s.%sRequest", packageName, operationName))

	s.logger.Debug("constructed message name",
		zap.String("packageName", packageName),
		zap.String("requestMessageName", string(requestMessageName)))

	// 3. Unmarshal the proto message
	msg, err := s.ParseProtoMessage(body, requestMessageName)
	if err != nil {
		s.logger.Error("failed to parse proto message",
			zap.String("requestMessageName", string(requestMessageName)),
			zap.Error(err))
		return nil, fmt.Errorf("failed to parse proto message: %w", err)
	}

	s.logger.Debug("successfully parsed proto message")

	// 4. Convert to map[string]interface{} for GraphQL variables
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

// mapConnectRequestToGraphQLVariables maps Connect RPC request fields to GraphQL variables
func (s *ConnectRPCServer) mapConnectRequestToGraphQLVariables(connectRequest map[string]interface{}, operation schemaloader.Operation) (map[string]interface{}, error) {
	// TODO: Add support for JSON Schema validation
	// Validate input against JSON Schema if available

	//if operation.CompiledSchema != nil {
	//	if err := s.validateInputWithSchema(connectRequest, operation.CompiledSchema); err != nil {
	//		return nil, fmt.Errorf("input validation failed: %w", err)
	//	}
	//}

	return connectRequest, nil
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
	// Set headers
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Write the GraphQL data directly as the Connect RPC response
	// In a real implementation with generated protos, this would be properly typed
	json.NewEncoder(w).Encode(data)
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

	server := &http.Server{
		Addr:         s.listenAddr,
		ReadTimeout:  s.requestTimeout,
		WriteTimeout: s.requestTimeout,
		Handler:      mux,
	}

	s.logger.Info("starting Connect RPC server",
		zap.String("listen_addr", s.listenAddr))

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("failed to start Connect RPC server", zap.Error(err))
		}
	}()

	return nil
}
