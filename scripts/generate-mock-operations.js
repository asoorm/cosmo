#!/usr/bin/env node

import { parseArgs } from 'node:util';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';
import { generateDemoJWT } from './jwt-utils.js';

const rootDirectory = dirname(fileURLToPath(import.meta.url));

const CLIENT_VERSIONS = [
  { name: 'apollo-client', version: '3.7.2' },
  { name: 'apollo-client', version: '3.8.10' },
  { name: 'apollo-client', version: '3.11.4' },
  { name: 'apollo-client', version: '4.0.0-beta.5' },
  { name: 'apollo-client', version: '2.6.10' },
  { name: 'relay', version: '12.0.0' },
  { name: 'relay', version: '13.2.0' },
  { name: 'relay', version: '14.1.0' },
  { name: 'relay', version: '15.0.0' },
  { name: 'urql', version: '2.3.6' },
  { name: 'urql', version: '3.0.3' },
  { name: 'urql', version: '4.0.5' },
  { name: 'urql', version: '4.1.0' },
  { name: 'graphql-request', version: '4.3.0' },
  { name: 'graphql-request', version: '5.2.0' },
  { name: 'graphql-request', version: '6.0.0' },
  { name: 'graphql-request', version: '6.1.0' },
  { name: 'graphql-request', version: '7.0.1' },
  { name: '@apollo/client', version: '3.9.5' },
  { name: '@apollo/client', version: '3.10.1' },
];

function randomClient() {
  return CLIENT_VERSIONS[Math.floor(Math.random() * CLIENT_VERSIONS.length)];
}

function randomDelay() {
  return Math.random() * 3000 + 2000; // 2-5 seconds
}

async function loadExistingOperations(directory = 'mock-operations') {
  const outputDir = join(rootDirectory, directory);
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

function preprocessFailures({ batches, variableRequestCount, failRate }) {
  const batchRequestCounts = [];
  let totalRequests = 0;

  for (const batch of batches) {
    const batchCounts = [];
    for (const _ of batch) {
      const requestCount = variableRequestCount ? Math.floor(Math.random() * variableRequestCount) + 1 : 1;
      batchCounts.push(requestCount);
      totalRequests += requestCount;
    }
    batchRequestCounts.push(batchCounts);
  }

  const failCount = Math.floor(totalRequests * failRate);
  const failingIndices = new Set();

  if (failCount > 0) {
    const allIndices = Array.from({ length: totalRequests }, (_, i) => i);
    // random shuffling
    for (let i = allIndices.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [allIndices[i], allIndices[j]] = [allIndices[j], allIndices[i]];
    }

    for (let i = 0; i < failCount; i++) {
      failingIndices.add(allIndices[i]);
    }
  }

  return {
    batchRequestCounts,
    totalRequests,
    failCount,
    failingIndices,
  };
}

async function executeOperations({
  endpoint,
  token,
  operations,
  repeat = 1,
  failRate = 0,
  repeatDelay = null,
  variableRequestCount = null,
}) {
  const jwtToken = generateDemoJWT();

  for (let iteration = 0; iteration < repeat; iteration++) {
    if (repeat > 1) {
      console.log(`\n=== Iteration ${iteration + 1}/${repeat} ===`);
    }

    const batches = [];
    for (let i = 0; i < operations.length; i += 15) {
      batches.push(operations.slice(i, i + 15));
    }

    const { batchRequestCounts, totalRequests, failCount, failingIndices } = preprocessFailures({
      batches,
      variableRequestCount,
      failRate,
    });

    console.log(
      `Executing ${operations.length} operations in ${batches.length} batches (${totalRequests} total requests, ${failCount} will fail)...`,
    );

    let globalRequestIndex = 0;

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      console.log(`\nBatch ${batchIndex + 1}/${batches.length} (${batch.length} operations)`);

      const promises = batch.map(async ({ filepath, data }, opIndex) => {
        const requestCount = batchRequestCounts[batchIndex][opIndex];

        if (variableRequestCount || failRate > 0) {
          const failingCount = Array.from({ length: requestCount }, (_, i) => globalRequestIndex + i).filter((idx) =>
            failingIndices.has(idx),
          ).length;
          console.log(
            `  → ${data.name}: scheduling ${requestCount} request(s)${
              failingCount > 0 ? ` (${failingCount} will fail)` : ''
            }`,
          );
        }

        const requests = [];
        for (let reqIndex = 0; reqIndex < requestCount; reqIndex++) {
          const currentGlobalIndex = globalRequestIndex++;
          const shouldSkipAuth = failingIndices.has(currentGlobalIndex);
          requests.push(
            (async () => {
              try {
                const client = randomClient();
                const clientName = data.clientName || client.name;
                const clientVersion = data.clientVersion || client.version;

                const headers = {
                  'Content-Type': 'application/json',
                  'X-WG-Token': token,
                  'GraphQL-Client-Name': clientName,
                  'GraphQL-Client-Version': clientVersion,
                };

                if (!(data.requiresAuth && shouldSkipAuth)) {
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

                return {
                  filepath,
                  success: response.ok && !result.errors,
                  result,
                };
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

      // shows timer for the next batch start
      for (let remaining = totalSeconds; remaining > 0; remaining--) {
        process.stdout.write(`\rNext iteration in ${remaining}s...`);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
      process.stdout.write('\r\x1b[K'); // clear output
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
      repeat: {
        type: 'string',
        short: 'r',
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
      'Usage: ./generate-mock-operations.js [--endpoint <url>] [--token <token>] [--repeat <number>] [--fail-rate <0.0-1.0>] [--repeat-delay <seconds>] [--variable-request-count <max>]',
    );
    console.error('Example: ./generate-mock-operations.js -e http://localhost:3002/graphql -t mytoken -a 50');
    console.error('  --endpoint=S\tspecifies the target endpoint for the operations (default: loads from env file)');
    console.error('  --token=S\tspecifies the API token to use for the requests (default: loads from env file)');
    console.error('  --repeat=N\thow many batches to execute (default: 1)');
    console.error(
      '  --fail-rate=D\tsimulates auth failures using round-robin pattern where D is value between 0.0 and 1.0 (default: 0.0)',
    );
    console.error('  --repeat-delay=N\tdelay between batches in N seconds (default: random 2-5s)');
    console.error('  --variable-request-count=N\trepeat each operation in a batch (N = max number) (default: null)');
    process.exit(1);
  }

  let failRate = 0;
  if (values['fail-rate']) {
    failRate = parseFloat(values['fail-rate']);
    if (isNaN(failRate) || failRate < 0 || failRate > 1) {
      console.error('Error: fail-rate must be a float between 0.0 and 1.0');
      process.exit(1);
    }
  }

  let repeatDelay = null;
  if (values['repeat-delay']) {
    repeatDelay = parseFloat(values['repeat-delay']);
    if (isNaN(repeatDelay) || repeatDelay < 0) {
      console.error('Error: repeat-delay must be a non-negative number (seconds)');
      process.exit(1);
    }
  }

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

  console.log('Loading predefined operations from scripts/demo-mock-operations/...');
  operations = await loadExistingOperations('demo-mock-operations');
  console.log(`✓ Loaded ${operations.length} demo operation files\n`);

  if (values.repeat) {
    repeat = parseInt(values.repeat, 10);
    if (isNaN(repeat) || repeat <= 0) {
      console.error('Error: repeat must be a positive number');
      process.exit(1);
    }
  }

  console.log(`Executing operations against ${values.endpoint}...`);
  await executeOperations({
    endpoint: values.endpoint,
    token: values.token,
    operations,
    repeat,
    failRate,
    repeatDelay,
    variableRequestCount,
  });

console.log('\n✓ Done!');
}

(async () => {
  try {
    await main();
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
})();
