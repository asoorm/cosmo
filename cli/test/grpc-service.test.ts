import { rmSync, mkdirSync, existsSync, writeFileSync, rmdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { Command } from 'commander';
import { describe, test, expect } from 'vitest';
import { createPromiseClient, createRouterTransport } from '@connectrpc/connect';
import { PlatformService } from '@wundergraph/cosmo-connect/dist/platform/v1/platform_connect';
import { dirname } from 'pathe';
import GenerateCommand from '../src/commands/grpc-service/commands/generate.js';
import GenerateFromCollectionCommand from '../src/commands/grpc-service/commands/generate-from-collection.js';
import GRPCCommands from '../src/commands/grpc-service/index.js';
import { Client } from '../src/core/client/client.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

export const mockPlatformTransport = () =>
  createRouterTransport(({ service }) => {
    service(PlatformService, {});
  });

describe('gRPC Generate Command', () => {
  test('should generate proto and mapping files', async (testContext) => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();
    program.addCommand(GenerateCommand({ client }));

    const tmpDir = join(tmpdir(), `grpc-test-${Date.now()}`);
    mkdirSync(tmpDir, { recursive: true });

    testContext.onTestFinished(() => {
      rmdirSync(tmpDir, { recursive: true });
    });

    const schemaPath = resolve(__dirname, 'fixtures', 'full-schema.graphql');

    await program.parseAsync(
      [
        'generate',
        'testservice',
        '-i',
        schemaPath,
        '-o',
        tmpDir,
      ],
      {
        from: 'user',
      }
    );

    // Verify the output files exist
    expect(existsSync(join(tmpDir, 'mapping.json'))).toBe(true);
    expect(existsSync(join(tmpDir, 'service.proto'))).toBe(true);
    expect(existsSync(join(tmpDir, 'service.proto.lock.json'))).toBe(true);
  });

  test('should create output directory if it does not exist', async () => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();
    program.addCommand(GenerateCommand({ client }));

    const nonExistentDir = join(tmpdir(), `grpc-test-non-existent-${Date.now()}`);

    // Ensure the directory doesn't exist
    if (existsSync(nonExistentDir)) {
      rmSync(nonExistentDir, { recursive: true, force: true });
    }

    const schemaPath = resolve(__dirname, 'fixtures', 'full-schema.graphql');

    await program.parseAsync(
      [
        'generate',
        'testservice',
        '-i',
        schemaPath,
        '-o',
        nonExistentDir,
      ],
      {
        from: 'user',
      }
    );

    // Verify the output directory and files exist
    expect(existsSync(nonExistentDir)).toBe(true);
    expect(existsSync(join(nonExistentDir, 'mapping.json'))).toBe(true);
    expect(existsSync(join(nonExistentDir, 'service.proto'))).toBe(true);
    expect(existsSync(join(nonExistentDir, 'service.proto.lock.json'))).toBe(true);

    // Cleanup
    rmSync(nonExistentDir, { recursive: true, force: true });
  });

  test('should fail when input file does not exist', async (testContext) => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();
    program.addCommand(GenerateCommand({ client }));

    const tmpDir = join(tmpdir(), `grpc-test-${Date.now()}`);
    mkdirSync(tmpDir, { recursive: true });

    testContext.onTestFinished(() => {
      rmdirSync(tmpDir, { recursive: true });
    });


    const nonExistentFile = join(tmpdir(), 'non-existent-schema.graphql');

    await expect(
      program.parseAsync(
        [
          'generate',
          'testservice',
          '-i',
          nonExistentFile,
          '-o',
          tmpDir,
        ],
        {
          from: 'user',
        }
      )
    ).rejects.toThrow();
  });

  test('should fail when output path is a file', async (testContext) => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();

    program.addCommand(GenerateCommand({ client }));

    const tmpDir = join(tmpdir(), `grpc-test-${Date.now()}`);
    mkdirSync(tmpDir, { recursive: true });

    testContext.onTestFinished(() => {
      rmdirSync(tmpDir, { recursive: true });
    });

    const outputFile = join(tmpDir, 'output.txt');
    writeFileSync(outputFile, 'test');

    program.exitOverride(err => {
      expect(err.message).toContain(`Output directory ${outputFile} is not a directory`);
    });

    await expect(
      program.parseAsync(
        [
          'generate',
          'testservice',
          '-i',
          'test/fixtures/full-schema.graphql',
          '-o',
          outputFile,
        ],
        {
          from: 'user',
        }
      )).rejects.toThrow('process.exit unexpectedly called with "1"');
  });

  test('should generate all files with warnings', async (testContext) => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();
    program.addCommand(GenerateCommand({ client }));

    const tmpDir = join(tmpdir(), `grpc-test-${Date.now()}`);
    mkdirSync(tmpDir, { recursive: true });

    testContext.onTestFinished(() => {
      rmdirSync(tmpDir, { recursive: true });
    });

    const schemaPath = resolve(__dirname, 'fixtures', 'schema-with-nullable-list-items.graphql');

    // Should complete successfully despite warnings
    await program.parseAsync(
      [
        'generate',
        'testservice',
        '-i',
        schemaPath,
        '-o',
        tmpDir,
      ],
      {
        from: 'user',
      }
    );

    // Verify the output files exist (generation should continue with warnings)
    expect(existsSync(join(tmpDir, 'mapping.json'))).toBe(true);
    expect(existsSync(join(tmpDir, 'service.proto'))).toBe(true);
    expect(existsSync(join(tmpDir, 'service.proto.lock.json'))).toBe(true);
  });

  test('should fail when schema has validation errors', async (testContext) => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();
    program.addCommand(GenerateCommand({ client }));

    const tmpDir = join(tmpdir(), `grpc-test-${Date.now()}`);
    mkdirSync(tmpDir, { recursive: true });

    testContext.onTestFinished(() => {
      rmdirSync(tmpDir, { recursive: true });
    });

    const schemaPath = resolve(__dirname, 'fixtures', 'schema-with-validation-errors.graphql');

    // Should fail due to validation errors
    await expect(
      program.parseAsync(
        [
          'generate',
          'testservice',
          '-i',
          schemaPath,
          '-o',
          tmpDir,
        ],
        {
          from: 'user',
        }
      )
    ).rejects.toThrow('Schema validation failed');

    // Verify no output files were created (generation should stop on errors)
    expect(existsSync(join(tmpDir, 'mapping.json'))).toBe(false);
    expect(existsSync(join(tmpDir, 'service.proto'))).toBe(false);
    expect(existsSync(join(tmpDir, 'service.proto.lock.json'))).toBe(false);
  });

  test('should display warnings and stop on errors', async (testContext) => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();
    program.addCommand(GenerateCommand({ client }));

    const tmpDir = join(tmpdir(), `grpc-test-${Date.now()}`);
    mkdirSync(tmpDir, { recursive: true });

    testContext.onTestFinished(() => {
      rmdirSync(tmpDir, { recursive: true });
    });

    const schemaPath = resolve(__dirname, 'fixtures', 'schema-with-warnings-and-errors.graphql');

    // Should fail due to validation errors (despite having warnings)
    await expect(
      program.parseAsync(
        [
          'generate',
          'testservice',
          '-i',
          schemaPath,
          '-o',
          tmpDir,
        ],
        {
          from: 'user',
        }
      )
    ).rejects.toThrow('Schema validation failed');

    // Verify no output files were created (generation should stop on errors)
    expect(existsSync(join(tmpDir, 'mapping.json'))).toBe(false);
    expect(existsSync(join(tmpDir, 'service.proto'))).toBe(false);
    expect(existsSync(join(tmpDir, 'service.proto.lock.json'))).toBe(false);
  });
});

describe('gRPC Generate From Collection Command', () => {
  test('should generate proto from operations collection', async (testContext) => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();
    program.addCommand(GenerateFromCollectionCommand({ client }));

    const tmpDir = join(tmpdir(), `grpc-collection-test-${Date.now()}`);
    mkdirSync(tmpDir, { recursive: true });

    // Create a collection directory with test operations
    const collectionDir = join(tmpDir, 'operations');
    mkdirSync(collectionDir, { recursive: true });

    // Create test schema
    const schemaContent = `
      type Query {
        employee(id: Int!): Employee
        employees: [Employee]
      }
      
      type Mutation {
        updateEmployee(id: Int!, name: String!): Employee
      }
      
      type Employee {
        id: Int!
        name: String!
        isAvailable: Boolean!
      }
    `;
    const schemaPath = join(tmpDir, 'schema.graphql');
    writeFileSync(schemaPath, schemaContent);

    // Create test operations
    const queryOperation = `
      query GetEmployee($id: Int!) {
        employee(id: $id) {
          id
          name
          isAvailable
        }
      }
    `;
    writeFileSync(join(collectionDir, 'GetEmployee.graphql'), queryOperation);

    const mutationOperation = `
      mutation UpdateEmployee($id: Int!, $name: String!) {
        updateEmployee(id: $id, name: $name) {
          id
          name
        }
      }
    `;
    writeFileSync(join(collectionDir, 'UpdateEmployee.graphql'), mutationOperation);

    testContext.onTestFinished(() => {
      rmSync(tmpDir, { recursive: true, force: true });
    });

    await program.parseAsync(
      [
        'generate-from-collection',
        'testservice',
        '-s',
        schemaPath,
        '-c',
        collectionDir,
        '-o',
        tmpDir,
      ],
      {
        from: 'user',
      }
    );

    // Verify the output files exist
    expect(existsSync(join(tmpDir, 'service.proto'))).toBe(true);
    expect(existsSync(join(tmpDir, 'service.proto.lock.json'))).toBe(true);
  });

  test('should generate proto with idempotency annotations when flag is enabled', async (testContext) => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();
    program.addCommand(GenerateFromCollectionCommand({ client }));

    const tmpDir = join(tmpdir(), `grpc-collection-idempotent-test-${Date.now()}`);
    mkdirSync(tmpDir, { recursive: true });

    // Create a collection directory with test operations
    const collectionDir = join(tmpDir, 'operations');
    mkdirSync(collectionDir, { recursive: true });

    // Create test schema
    const schemaContent = `
      type Query {
        employee(id: Int!): Employee
        employees: [Employee]
      }
      
      type Mutation {
        updateEmployee(id: Int!, name: String!): Employee
      }
      
      type Employee {
        id: Int!
        name: String!
        isAvailable: Boolean!
      }
    `;
    const schemaPath = join(tmpDir, 'schema.graphql');
    writeFileSync(schemaPath, schemaContent);

    // Create test operations
    const queryOperation = `
      query GetEmployee($id: Int!) {
        employee(id: $id) {
          id
          name
          isAvailable
        }
      }
    `;
    writeFileSync(join(collectionDir, 'GetEmployee.graphql'), queryOperation);

    const mutationOperation = `
      mutation UpdateEmployee($id: Int!, $name: String!) {
        updateEmployee(id: $id, name: $name) {
          id
          name
        }
      }
    `;
    writeFileSync(join(collectionDir, 'UpdateEmployee.graphql'), mutationOperation);

    testContext.onTestFinished(() => {
      rmSync(tmpDir, { recursive: true, force: true });
    });

    await program.parseAsync(
      [
        'generate-from-collection',
        'testservice',
        '-s',
        schemaPath,
        '-c',
        collectionDir,
        '-o',
        tmpDir,
        '--mark-queries-idempotent',
      ],
      {
        from: 'user',
      }
    );

    // Verify the output files exist
    expect(existsSync(join(tmpDir, 'service.proto'))).toBe(true);
    
    // Read the generated proto file and verify idempotency annotations
    const protoContent = require('fs').readFileSync(join(tmpDir, 'service.proto'), 'utf8');
    
    // Query should have idempotency annotation
    expect(protoContent).toContain('rpc GetEmployee(GetEmployeeRequest) returns (GetEmployeeResponse) {');
    expect(protoContent).toContain('option idempotency_level = NO_SIDE_EFFECTS;');
    
    // Mutation should NOT have idempotency annotation
    expect(protoContent).toContain('rpc UpdateEmployee(UpdateEmployeeRequest) returns (UpdateEmployeeResponse) {}');
    
    // Count occurrences to ensure only queries get the annotation
    const idempotencyMatches = protoContent.match(/option idempotency_level = NO_SIDE_EFFECTS;/g);
    expect(idempotencyMatches).toHaveLength(1); // Only one query operation
  });

  test('should generate proto without idempotency annotations when flag is not provided', async (testContext) => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();
    program.addCommand(GenerateFromCollectionCommand({ client }));

    const tmpDir = join(tmpdir(), `grpc-collection-no-idempotent-test-${Date.now()}`);
    mkdirSync(tmpDir, { recursive: true });

    // Create a collection directory with test operations
    const collectionDir = join(tmpDir, 'operations');
    mkdirSync(collectionDir, { recursive: true });

    // Create test schema
    const schemaContent = `
      type Query {
        employee(id: Int!): Employee
      }
      
      type Employee {
        id: Int!
        name: String!
      }
    `;
    const schemaPath = join(tmpDir, 'schema.graphql');
    writeFileSync(schemaPath, schemaContent);

    // Create test query operation
    const queryOperation = `
      query GetEmployee($id: Int!) {
        employee(id: $id) {
          id
          name
        }
      }
    `;
    writeFileSync(join(collectionDir, 'GetEmployee.graphql'), queryOperation);

    testContext.onTestFinished(() => {
      rmSync(tmpDir, { recursive: true, force: true });
    });

    await program.parseAsync(
      [
        'generate-from-collection',
        'testservice',
        '-s',
        schemaPath,
        '-c',
        collectionDir,
        '-o',
        tmpDir,
      ],
      {
        from: 'user',
      }
    );

    // Verify the output files exist
    expect(existsSync(join(tmpDir, 'service.proto'))).toBe(true);
    
    // Read the generated proto file and verify NO idempotency annotations
    const protoContent = require('fs').readFileSync(join(tmpDir, 'service.proto'), 'utf8');
    
    // Should not contain any idempotency annotations
    expect(protoContent).not.toContain('option idempotency_level = NO_SIDE_EFFECTS;');
    
    // Should contain simple RPC method without options
    expect(protoContent).toContain('rpc GetEmployee(GetEmployeeRequest) returns (GetEmployeeResponse) {}');
  });

  test('should fail when collection directory is empty', async (testContext) => {
    const client: Client = {
      platform: createPromiseClient(PlatformService, mockPlatformTransport()),
    };

    const program = new Command();
    program.addCommand(GenerateFromCollectionCommand({ client }));

    const tmpDir = join(tmpdir(), `grpc-collection-empty-test-${Date.now()}`);
    mkdirSync(tmpDir, { recursive: true });

    // Create empty collection directory
    const collectionDir = join(tmpDir, 'operations');
    mkdirSync(collectionDir, { recursive: true });

    // Create test schema
    const schemaContent = `
      type Query {
        employee(id: Int!): Employee
      }
      
      type Employee {
        id: Int!
        name: String!
      }
    `;
    const schemaPath = join(tmpDir, 'schema.graphql');
    writeFileSync(schemaPath, schemaContent);

    testContext.onTestFinished(() => {
      rmSync(tmpDir, { recursive: true, force: true });
    });

    await expect(
      program.parseAsync(
        [
          'generate-from-collection',
          'testservice',
          '-s',
          schemaPath,
          '-c',
          collectionDir,
          '-o',
          tmpDir,
        ],
        {
          from: 'user',
        }
      )
    ).rejects.toThrow('No GraphQL operation files found in the collection directory');
  });
});
