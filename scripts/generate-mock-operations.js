#!/usr/bin/env node

import { parseArgs } from 'node:util';
import { mkdir, writeFile, readdir, readFile, access } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';
import { stdin, stdout } from 'node:process';
import { createInterface } from 'node:readline';
import { generateDemoJWT } from './jwt-utils.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const CLIENT_NAMES = ['apollo-client', 'relay', 'urql', 'graphql-request'];

const EMPLOYEE_FIELDS = [
  ['id', 'tag', 'currentMood'],
  ['id', 'tag', 'startDate', 'isAvailable'],
  ['id', 'currentMood', 'derivedMood'],
  ['id', 'tag', 'updatedAt', 'startDate'],
  ['id', 'isAvailable', 'currentMood', 'tag'],
  ['id', 'tag', 'notes', 'currentMood'],
];

const EMPLOYEE_WITH_DETAILS = [
  ['id', 'tag', 'details { forename surname }'],
  ['id', 'tag', 'details { forename surname hasChildren }'],
  ['id', 'details { forename location { key { name } } }'],
  ['id', 'tag', 'details { nationality maritalStatus }'],
];

const PRODUCT_FRAGMENTS = [
  `... on Consultancy { upc name }`,
  `... on Cosmo { upc name repositoryURL }`,
  `... on SDK { upc unicode clientLanguages }`,
  `... on Consultancy { upc name }\n    ... on Cosmo { upc name }`,
  `... on SDK { upc unicode }\n    ... on Cosmo { upc repositoryURL }`,
];

function randomVersion() {
  return `${Math.floor(Math.random() * 5) + 1}.${Math.floor(Math.random() * 20)}.${Math.floor(Math.random() * 10)}`;
}

function randomClientName() {
  return CLIENT_NAMES[Math.floor(Math.random() * CLIENT_NAMES.length)];
}

function randomDelay() {
  return Math.random() * 3000 + 2000; // 2-5 seconds
}

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomId() {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let result = '';
  for (let i = 0; i < 8; i++) {
    result += chars[Math.floor(Math.random() * chars.length)];
  }
  return result;
}

function generateRandomOperation() {
  const opType = Math.random() > 0.5 ? 'query' : 'mutation';

  if (opType === 'query') {
    return generateRandomQuery();
  } else {
    return generateRandomMutation();
  }
}

function generateRandomQuery() {
  const id = randomId();
  const queryType = Math.floor(Math.random() * 10);
  const useDetails = Math.random() > 0.6;
  const fields = useDetails ? randomChoice(EMPLOYEE_WITH_DETAILS) : randomChoice(EMPLOYEE_FIELDS);

  switch (queryType) {
    case 0:
      return {
        type: 'query',
        name: `GetEmployee_${id}`,
        operation: `query GetEmployee_${id}($id: Int!) {
  employee(id: $id) {
    ${fields.join('\n    ')}
  }
}`,
        variables: { id: Math.floor(Math.random() * 100) + 1 },
        requiresAuth: false,
      };

    case 1:
      return {
        type: 'query',
        name: `GetEmployees_${id}`,
        operation: `query GetEmployees_${id} {
  employees {
    ${randomChoice(EMPLOYEE_FIELDS).join('\n    ')}
  }
}`,
        variables: {},
        requiresAuth: false,
      };

    case 2:
      return {
        type: 'query',
        name: `GetProducts_${id}`,
        operation: `query GetProducts_${id} {
  products {
    ${randomChoice(PRODUCT_FRAGMENTS)}
  }
}`,
        variables: {},
        requiresAuth: false,
      };

    case 3:
      return {
        type: 'query',
        name: `GetTeammates_${id}`,
        operation: `query GetTeammates_${id}($team: Department!) {
  teammates(team: $team) {
    ${randomChoice(EMPLOYEE_FIELDS).join('\n    ')}
  }
}`,
        variables: {
          team: randomChoice(['ENGINEERING', 'MARKETING', 'OPERATIONS']),
        },
        requiresAuth: false,
      };

    case 4:
      return {
        type: 'query',
        name: `GetFirstEmployee_${id}`,
        operation: `query GetFirstEmployee_${id} {
  firstEmployee {
    ${fields.join('\n    ')}
  }
}`,
        variables: {},
        requiresAuth: false,
      };

    case 5:
      const criteriaType = Math.random();
      let criteria;
      if (criteriaType < 0.33) {
        criteria = { id: Math.floor(Math.random() * 100) + 1 };
      } else if (criteriaType < 0.66) {
        criteria = {
          department: randomChoice(['ENGINEERING', 'MARKETING', 'OPERATIONS']),
        };
      } else {
        criteria = { title: randomChoice(['Engineer', 'Manager', 'Director']) };
      }
      return {
        type: 'query',
        name: `FindEmployeesBy_${id}`,
        operation: `query FindEmployeesBy_${id}($criteria: FindEmployeeCriteria!) {
  findEmployeesBy(criteria: $criteria) {
    ${randomChoice(EMPLOYEE_FIELDS).join('\n    ')}
  }
}`,
        variables: { criteria },
        requiresAuth: false,
      };

    case 6:
      const hasCriteria = Math.random() > 0.3;
      const criteriaValue = hasCriteria
        ? {
          hasPets: Math.random() > 0.5,
          nationality: randomChoice(['AMERICAN', 'DUTCH', 'ENGLISH', 'GERMAN', 'INDIAN', 'SPANISH']),
        }
        : null;
      return {
        type: 'query',
        name: `FindEmployees_${id}`,
        operation: `query FindEmployees_${id}${hasCriteria ? '($criteria: SearchInput)' : ''} {
  findEmployees${hasCriteria ? '(criteria: $criteria)' : ''} {
    ${randomChoice(EMPLOYEE_FIELDS).join('\n    ')}
  }
}`,
        variables: hasCriteria ? { criteria: criteriaValue } : {},
        requiresAuth: false,
      };

    case 7:
      return {
        type: 'query',
        name: `GetProductTypes_${id}`,
        operation: `query GetProductTypes_${id} {
  productTypes {
    ${randomChoice(PRODUCT_FRAGMENTS)}
  }
}`,
        variables: {},
        requiresAuth: false,
      };

    case 8:
      // Authenticated operation: requires read:fact or read:all scopes
      return {
        type: 'query',
        name: `GetTopSecretFacts_${id}`,
        operation: `query GetTopSecretFacts_${id} {
  topSecretFederationFacts {
    description
    factType
  }
}`,
        variables: {},
        requiresAuth: true,
      };

    case 9:
      return {
        type: 'query',
        name: `GetSharedThings_${id}`,
        operation: `query GetSharedThings_${id}($numOfA: Int!, $numOfB: Int!) {
  sharedThings(numOfA: $numOfA, numOfB: $numOfB) {
    a
  }
}`,
        variables: {
          numOfA: Math.floor(Math.random() * 10) + 1,
          numOfB: Math.floor(Math.random() * 10) + 1,
        },
        requiresAuth: false,
      };

    default:
      return generateRandomQuery();
  }
}

function generateRandomMutation() {
  const id = randomId();
  const mutationType = Math.floor(Math.random() * 6);

  switch (mutationType) {
    case 0:
      return {
        type: 'mutation',
        name: `UpdateEmployeeTag_${id}`,
        operation: `mutation UpdateEmployeeTag_${id}($id: Int!, $tag: String!) {
  updateEmployeeTag(id: $id, tag: $tag) {
    id
    tag
    ${randomChoice(['updatedAt', 'currentMood', 'isAvailable'])}
  }
}`,
        variables: {
          id: Math.floor(Math.random() * 100) + 1,
          tag: `tag-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        },
        requiresAuth: false,
      };

    case 1:
      // Authenticated operation: requires write:fact or write:all scopes
      return {
        type: 'mutation',
        name: `AddFact_${id}`,
        operation: `mutation AddFact_${id}($fact: TopSecretFactInput!) {
  addFact(fact: $fact) {
    description
    factType
  }
}`,
        variables: {
          fact: {
            title: `Fact-${Date.now()}-${id}`,
            description: `Description-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
            factType: randomChoice(['DIRECTIVE', 'ENTITY', 'MISCELLANEOUS']),
          },
        },
        requiresAuth: true,
      };

    case 2:
      return {
        type: 'mutation',
        name: `UpdateAvailability_${id}`,
        operation: `mutation UpdateAvailability_${id}($employeeID: Int!, $isAvailable: Boolean!) {
  updateAvailability(employeeID: $employeeID, isAvailable: $isAvailable) {
    id
    isAvailable
    ${randomChoice(['currentMood', 'tag', 'updatedAt'])}
  }
}`,
        variables: {
          employeeID: Math.floor(Math.random() * 100) + 1,
          isAvailable: Math.random() > 0.5,
        },
        requiresAuth: false,
      };

    case 3:
      return {
        type: 'mutation',
        name: `UpdateMood_${id}`,
        operation: `mutation UpdateMood_${id}($employeeID: Int!, $mood: Mood!) {
  updateMood(employeeID: $employeeID, mood: $mood) {
    id
    currentMood
    derivedMood
    ${randomChoice(['tag', 'isAvailable'])}
  }
}`,
        variables: {
          employeeID: Math.floor(Math.random() * 100) + 1,
          mood: randomChoice(['HAPPY', 'SAD']),
        },
        requiresAuth: false,
      };

    case 4:
      return {
        type: 'mutation',
        name: `UpdateEmployeeKafka_${id}`,
        operation: `mutation UpdateEmployeeKafka_${id}($employeeID: Int!, $update: UpdateEmployeeInput!) {
  updateEmployeeMyKafka(employeeID: $employeeID, update: $update) {
    success
  }
}`,
        variables: {
          employeeID: Math.floor(Math.random() * 100) + 1,
          update: {
            name: `Name-${Date.now()}-${id}`,
            email: `email-${Date.now()}-${id}@example.com`,
          },
        },
        requiresAuth: false,
      };

    case 5:
      return {
        type: 'mutation',
        name: `UpdateEmployeeNats_${id}`,
        operation: `mutation UpdateEmployeeNats_${id}($id: Int!, $update: UpdateEmployeeInput!) {
  updateEmployeeMyNats(id: $id, update: $update) {
    success
  }
}`,
        variables: {
          id: Math.floor(Math.random() * 100) + 1,
          update: {
            name: `Name-${Date.now()}-${id}`,
            email: `email-${Date.now()}-${id}@example.com`,
          },
        },
        requiresAuth: false,
      };

    default:
      return generateRandomMutation();
  }
}

async function checkExistingOperations() {
  const outputDir = join(__dirname, 'mock-operations');
  try {
    await access(outputDir);
    const files = await readdir(outputDir).then((f) => f.filter((name) => name.endsWith('.json')));
    if (files.length > 0) {
      console.log(`!  Found ${files.length} existing operation files in scripts/mock-operations/`);
      console.log('Press R to regenerate, ENTER/SPACE to replay existing...');

      return new Promise((resolve) => {
        const rl = createInterface({ input: stdin, output: stdout });
        stdin.setRawMode(true);
        stdin.resume();

        stdin.once('data', (key) => {
          stdin.setRawMode(false);
          rl.close();

          if (key[0] === 13 || key[0] === 32) {
            // ENTER or SPACE
            console.log('Replaying existing operations...\n');
            resolve({ shouldRegenerate: false, existingCount: files.length });
          } else if (key[0] === 114 || key[0] === 82) {
            // 'r' or 'R'
            console.log('Regenerating operations...\n');
            resolve({ shouldRegenerate: true, existingCount: files.length });
          } else if (key[0] === 3) {
            // Ctrl+C
            console.log('\nAborted.');
            process.exit(0);
          } else {
            console.log('\nInvalid key. Aborting.');
            process.exit(1);
          }
        });
      });
    }
  } catch (err) {
    // Directory doesn't exist, will be created
  }
  return { shouldRegenerate: true, existingCount: 0 };
}

async function loadExistingOperations(directory = 'mock-operations') {
  const outputDir = join(__dirname, directory);
  const files = await readdir(outputDir);
  const jsonFiles = files.filter((f) => f.endsWith('.json')).sort();

  const operations = [];
  for (const file of jsonFiles) {
    const filepath = join(outputDir, file);
    const content = await readFile(filepath, 'utf-8');
    const data = JSON.parse(content);
    operations.push({ filepath, data });
  }

  return operations;
}

async function generateOperationFiles(count) {
  const outputDir = join(__dirname, 'mock-operations');
  await mkdir(outputDir, { recursive: true });

  const operations = [];
  for (let i = 0; i < count; i++) {
    const op = generateRandomOperation();
    const filename = `${String(i + 1).padStart(3, '0')}-${op.name}.json`;
    const filepath = join(outputDir, filename);

    const operationData = {
      name: op.name,
      type: op.type,
      operation: op.operation,
      variables: op.variables,
      clientName: randomClientName(),
      clientVersion: randomVersion(),
      requiresAuth: op.requiresAuth || false,
    };

    await writeFile(filepath, JSON.stringify(operationData, null, 2));
    operations.push({ filepath, data: operationData });
  }

  return operations;
}

async function executeOperations({
  endpoint,
  token,
  operations,
  repeat = 1,
  demoMode = false,
  failRate = 0,
  repeatDelay = null,
  variableRequestCount = null,
}) {
  // Generate JWT token for demo mode (injected into all requests)
  const jwtToken = demoMode ? generateDemoJWT() : null;
  if (demoMode) {
    console.log('Generated JWT token for authenticated demo operations\n');
  }

  // Calculate round-robin period for failure simulation
  const failurePeriod = failRate > 0 ? Math.round(1 / failRate) : 0;

  for (let iteration = 0; iteration < repeat; iteration++) {
    if (repeat > 1) {
      console.log(`\n=== Iteration ${iteration + 1}/${repeat} ===`);
    }

    // Round-robin: skip auth header every N iterations based on failRate
    const shouldSkipAuth = failurePeriod > 0 && iteration % failurePeriod !== 0;

    const batches = [];
    for (let i = 0; i < operations.length; i += 15) {
      batches.push(operations.slice(i, i + 15));
    }

    console.log(`Executing ${operations.length} operations in ${batches.length} batches...`);

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      console.log(`\nBatch ${batchIndex + 1}/${batches.length} (${batch.length} operations)`);

      const promises = batch.map(async ({ filepath, data }) => {
        // Determine how many times to fire this operation
        const requestCount = variableRequestCount ? Math.floor(Math.random() * variableRequestCount) + 1 : 1;

        const requests = [];
        for (let reqIndex = 0; reqIndex < requestCount; reqIndex++) {
          requests.push(
            (async () => {
              try {
                // Use provided client info or randomize if missing
                const clientName = data.clientName || randomClientName();
                const clientVersion = data.clientVersion || randomVersion();

                const headers = {
                  'Content-Type': 'application/json',
                  'X-WG-Token': token,
                  'GraphQL-Client-Name': clientName,
                  'GraphQL-Client-Version': clientVersion,
                };

                // Inject Authorization header for demo mode with failure simulation
                // Only skip auth if: operation requires auth AND round-robin says to skip
                if (demoMode && jwtToken && !(data.requiresAuth && shouldSkipAuth)) {
                  headers['Authorization'] = `Bearer ${jwtToken}`;
                }

                const response = await fetch(endpoint, {
                  method: 'POST',
                  headers,
                  body: JSON.stringify({
                    query: data.operation,
                    variables: data.variables,
                  }),
                });

                const result = await response.json();
                const status = response.ok ? '✓' : '✗';
                const countSuffix = requestCount > 1 ? ` [${reqIndex + 1}/${requestCount}]` : '';
                console.log(`  ${status} ${data.name}${countSuffix} (${clientName}@${clientVersion})`);

                if (!response.ok || result.errors) {
                  console.log(`    Error: ${JSON.stringify(result.errors || result)}`);
                }

                return { filepath, success: response.ok && !result.errors, result };
              } catch (error) {
                const countSuffix = requestCount > 1 ? ` [${reqIndex + 1}/${requestCount}]` : '';
                console.log(`  ✗ ${data.name}${countSuffix} - ${error.message}`);
                return { filepath, success: false, error: error.message };
              }
            })(),
          );
        }

        return Promise.all(requests);
      });

      await Promise.allSettled(promises);

      if (batchIndex < batches.length - 1) {
        const delay = randomDelay();
        console.log(`  Waiting ${(delay / 1000).toFixed(1)}s before next batch...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }

    if (iteration < repeat - 1) {
      const delay = repeatDelay !== null ? repeatDelay * 1000 : randomDelay();
      const totalSeconds = Math.ceil(delay / 1000);

      // Countdown timer
      for (let remaining = totalSeconds; remaining > 0; remaining--) {
        process.stdout.write(`\rNext iteration in ${remaining}s...`);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
      process.stdout.write('\r\x1b[K'); // Clear line
    }
  }
}

async function main() {
  const defaultEndpoint = process.env.LISTEN_ADDR;
  const defaultToken = process.env.GRAPH_API_TOKEN;

  const { values } = parseArgs({
    options: {
      endpoint: {
        type: 'string',
        short: 'e',
        default: `http://${defaultEndpoint}/graphql`,
      },
      token: {
        type: 'string',
        short: 't',
        default: defaultToken,
      },
      amount: {
        type: 'string',
        short: 'a',
      },
      repeat: {
        type: 'string',
        short: 'r',
      },
      demo: {
        type: 'boolean',
        short: 'd',
      },
      'fail-rate': {
        type: 'string',
        short: 'f',
      },
      'repeat-delay': {
        type: 'string',
      },
      'variable-request-count': {
        type: 'string',
      },
    },
  });

  if (defaultEndpoint && !values.endpoint) {
    console.log('Default endpoint used from .router/.env LISTEN_ADDR');
  }

  if (defaultToken && !values.token) {
    console.log('Default token used from .router/.env GRAPH_API_TOKEN');
  }

  if (!values.endpoint || !values.token) {
    console.error(
      'Usage: ./generate-mock-operations.js --endpoint <url> --token <token> [--amount <number>] [--repeat <number>] [--demo] [--fail-rate <0.0-1.0>] [--repeat-delay <seconds>] [--variable-request-count <max>]',
    );
    console.error('Example: ./generate-mock-operations.js -e http://localhost:3002/graphql -t mytoken -a 50');
    console.error('  --amount is required only when generating new operations');
    console.error('  --repeat N will replay operations N times (only when replaying existing or demo ops)');
    console.error('  --demo uses predefined demo operations from scripts/demo-mock-operations/ with authentication');
    console.error(
      '  --fail-rate X simulates auth failures using round-robin pattern (0.0-1.0, only with --demo and --repeat)',
    );
    console.error('  --repeat-delay X fixed delay in seconds between iterations (only with --repeat)');
    console.error('  --variable-request-count X fires each operation 1-X times per batch');
    process.exit(1);
  }

  // Validate and parse fail-rate
  let failRate = 0;
  if (values['fail-rate']) {
    failRate = parseFloat(values['fail-rate']);
    if (isNaN(failRate) || failRate < 0 || failRate > 1) {
      console.error('Error: fail-rate must be a float between 0.0 and 1.0');
      process.exit(1);
    }
  }

  // Validate and parse repeat-delay
  let repeatDelay = null;
  if (values['repeat-delay']) {
    repeatDelay = parseFloat(values['repeat-delay']);
    if (isNaN(repeatDelay) || repeatDelay < 0) {
      console.error('Error: repeat-delay must be a non-negative number (seconds)');
      process.exit(1);
    }
  }

  // Validate variable-request-count
  let variableRequestCount = null;
  if (values['variable-request-count']) {
    variableRequestCount = parseFloat(values['variable-request-count']);
    if (isNaN(variableRequestCount) || !Number.isInteger(variableRequestCount) || variableRequestCount < 1) {
      console.error('Error: variable-request-count must be a positive integer');
      process.exit(1);
    }
  }

  let operations;
  let repeat = 1;

  // Demo mode: load predefined operations from demo-mock-operations/
  if (values.demo) {
    console.log('Demo mode: Loading predefined operations from scripts/demo-mock-operations/...');
    operations = await loadExistingOperations('demo-mock-operations');
    console.log(`✓ Loaded ${operations.length} demo operation files\n`);

    if (values.repeat) {
      repeat = parseInt(values.repeat, 10);
      if (isNaN(repeat) || repeat <= 0) {
        console.error('Error: repeat must be a positive number');
        process.exit(1);
      }
    }
  } else {
    // Regular mode: check for existing operations or generate new ones
    const checkResult = await checkExistingOperations();

    if (checkResult.shouldRegenerate) {
      if (!values.amount) {
        console.error('Error: --amount is required when generating new operations');
        process.exit(1);
      }

      const amount = parseInt(values.amount, 10);
      if (isNaN(amount) || amount <= 0) {
        console.error('Error: amount must be a positive number');
        process.exit(1);
      }

      console.log(`Generating ${amount} randomized operations...`);
      operations = await generateOperationFiles(amount);
      console.log(`✓ Generated ${operations.length} operation files in scripts/mock-operations/\n`);
    } else {
      console.log(`Loading ${checkResult.existingCount} existing operations...`);
      operations = await loadExistingOperations();
      console.log(`✓ Loaded ${operations.length} operation files\n`);

      if (values.repeat) {
        repeat = parseInt(values.repeat, 10);
        if (isNaN(repeat) || repeat <= 0) {
          console.error('Error: repeat must be a positive number');
          process.exit(1);
        }
      }
    }
  }

  console.log(`Executing operations against ${values.endpoint}...`);
  await executeOperations({
    endpoint: values.endpoint,
    token: values.token,
    operations,
    repeat,
    demoMode: values.demo || false,
    failRate,
    repeatDelay,
    variableRequestCount,
  });

  console.log('\n✓ Done!');
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
