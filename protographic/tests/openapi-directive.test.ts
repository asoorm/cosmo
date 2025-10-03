import { describe, it, expect } from 'vitest';
import { compileOperationsToProto } from '../src/index.js';
import { enhanceSDLWithOpenApiDirective } from '../src/openapi-preprocessor.js';

describe('OpenAPI Directive Support', () => {
  const schema = `
    type Query {
      user(id: ID!): User
      users: [User!]!
    }
    
    type User {
      id: ID!
      name: String!
      email: String
    }
  `;

  it('should extract @openapi directive metadata and generate gnostic options', () => {
    const operation = `
      query GetUser($id: ID!) @openapi(
        operationId: "getUserById"
        summary: "Get user by ID"
        description: "Retrieves a single user by their unique identifier"
        tags: ["users", "query"]
        deprecated: false
      ) {
        user(id: $id) {
          id
          name
          email
        }
      }
    `;

    // Enhance SDL with @openapi directive definition
    const enhancedSDL = enhanceSDLWithOpenApiDirective(schema);

    const result = compileOperationsToProto(
      [{ name: 'GetUser', content: operation }],
      enhancedSDL,
      {
        serviceName: 'UserService',
        packageName: 'user.v1',
      }
    );

    // Verify proto contains gnostic import
    expect(result.proto).toContain('import "gnostic/openapi/v3/annotations.proto"');

    // Verify proto contains combined gnostic option with all fields
    expect(result.proto).toContain('option (gnostic.openapi.v3.operation) = {');
    expect(result.proto).toContain('operation_id: "getUserById"');
    expect(result.proto).toContain('summary: "Get user by ID"');
    expect(result.proto).toContain('description: "Retrieves a single user by their unique identifier"');
    expect(result.proto).toContain('tags: ["users", "query"]');
  });

  it('should handle operations without @openapi directive', () => {
    const operation = `
      query GetUsers {
        users {
          id
          name
        }
      }
    `;

    const result = compileOperationsToProto(
      [{ name: 'GetUsers', content: operation }],
      schema,
      {
        serviceName: 'UserService',
        packageName: 'user.v1',
      }
    );

    // Verify proto does NOT contain gnostic import when no directive is used
    expect(result.proto).not.toContain('import "gnostic/openapi/v3/annotations.proto"');
    expect(result.proto).not.toContain('gnostic.openapi.v3.operation');
  });

  it('should handle deprecated operations', () => {
    const operation = `
      query GetUserLegacy($id: ID!) @openapi(
        operationId: "getUserLegacy"
        deprecated: true
      ) {
        user(id: $id) {
          id
          name
        }
      }
    `;

    const enhancedSDL = enhanceSDLWithOpenApiDirective(schema);

    const result = compileOperationsToProto(
      [{ name: 'GetUserLegacy', content: operation }],
      enhancedSDL,
      {
        serviceName: 'UserService',
        packageName: 'user.v1',
      }
    );

    expect(result.proto).toContain('deprecated: true');
  });

  it('should handle external documentation', () => {
    const operation = `
      query GetUser($id: ID!) @openapi(
        operationId: "getUser"
        externalDocs: {
          description: "User API Documentation"
          url: "https://api.example.com/docs/users"
        }
      ) {
        user(id: $id) {
          id
          name
        }
      }
    `;

    const enhancedSDL = enhanceSDLWithOpenApiDirective(schema);

    const result = compileOperationsToProto(
      [{ name: 'GetUser', content: operation }],
      enhancedSDL,
      {
        serviceName: 'UserService',
        packageName: 'user.v1',
      }
    );

    expect(result.proto).toContain('external_docs: { description: "User API Documentation", url: "https://api.example.com/docs/users" }');
  });

  it('should handle multiple operations with different metadata', () => {
    const operation1 = `
      query GetUser($id: ID!) @openapi(
        operationId: "getUserById"
        tags: ["users"]
      ) {
        user(id: $id) {
          id
          name
        }
      }
    `;

    const operation2 = `
      query GetUsers @openapi(
        operationId: "listUsers"
        tags: ["users", "list"]
      ) {
        users {
          id
          name
        }
      }
    `;

    const enhancedSDL = enhanceSDLWithOpenApiDirective(schema);

    const result = compileOperationsToProto(
      [
        { name: 'GetUser', content: operation1 },
        { name: 'GetUsers', content: operation2 },
      ],
      enhancedSDL,
      {
        serviceName: 'UserService',
        packageName: 'user.v1',
      }
    );

    // Verify both operations have their respective metadata
    expect(result.proto).toContain('operation_id: "getUserById"');
    expect(result.proto).toContain('operation_id: "listUsers"');
    expect(result.proto).toContain('tags: ["users"]');
    expect(result.proto).toContain('tags: ["users", "list"]');
  });

  it('should generate valid proto structure with all components', () => {
    const operation = `
      query GetUser($id: ID!) @openapi(
        operationId: "getUserById"
        summary: "Get user"
        tags: ["users"]
      ) {
        user(id: $id) {
          id
          name
          email
        }
      }
    `;

    const enhancedSDL = enhanceSDLWithOpenApiDirective(schema);

    const result = compileOperationsToProto(
      [{ name: 'GetUser', content: operation }],
      enhancedSDL,
      {
        serviceName: 'UserService',
        packageName: 'user.v1',
      }
    );

    // Verify proto structure
    expect(result.proto).toContain('syntax = "proto3"');
    expect(result.proto).toContain('package user.v1');
    expect(result.proto).toContain('service UserService');
    expect(result.proto).toContain('rpc GetUser(GetUserRequest) returns (GetUserResponse)');
    expect(result.proto).toContain('message GetUserRequest');
    expect(result.proto).toContain('message GetUserResponse');
  });

  it('should properly escape multi-line descriptions', () => {
    const operation = `
      query GetUser($id: ID!) @openapi(
        operationId: "getUserById"
        description: """This is a long multi-line description
  which covers
  multiple lines"""
      ) {
        user(id: $id) {
          id
          name
        }
      }
    `;

    const enhancedSDL = enhanceSDLWithOpenApiDirective(schema);

    const result = compileOperationsToProto(
      [{ name: 'GetUser', content: operation }],
      enhancedSDL,
      {
        serviceName: 'UserService',
        packageName: 'user.v1',
      }
    );

    // Verify that newlines are escaped as \n in the proto output
    // Note: GraphQL parser trims leading whitespace from multi-line strings
    expect(result.proto).toContain('description: "This is a long multi-line description\\nwhich covers\\nmultiple lines"');
    // Verify the proto doesn't contain actual unescaped newlines in the option string
    expect(result.proto).not.toMatch(/description: "This is a long multi-line description\nwhich/);
    // Verify it's in a combined option statement
    expect(result.proto).toContain('option (gnostic.openapi.v3.operation) = {');
  });
});