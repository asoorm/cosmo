#!/usr/bin/env node

import { readFileSync } from 'node:fs';
import { createHmac } from 'node:crypto';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Base64URL encode a buffer
 */
function base64UrlEncode(buffer) {
  return buffer.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

/**
 * Read JWT secret from controlplane/.env file
 */
function readJWTSecret() {
  const envPath = join(__dirname, '../controlplane/.env');
  try {
    const envContent = readFileSync(envPath, 'utf-8');
    const match = envContent.match(/AUTH_JWT_SECRET="?([^"\n]+)"?/);
    if (match && match[1]) {
      return match[1];
    }
    throw new Error('AUTH_JWT_SECRET not found in controlplane/.env');
  } catch (error) {
    if (error.code === 'ENOENT') {
      throw new Error('controlplane/.env file not found. Please ensure you have set up the development environment.');
    }
    throw error;
  }
}

/**
 * Generate a JWT token for demo operations with all required scopes
 *
 * @returns {string} JWT token with all demo scopes
 */
export function generateDemoJWT() {
  const secret = readJWTSecret();

  // JWT Header
  const header = {
    alg: 'HS256',
    typ: 'JWT',
    kid: 'default',
  };

  // JWT Payload with all scopes needed for demo operations
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    scope:
      'read:fact read:all write:fact write:all read:employee read:private read:entity read:miscellaneous read:scalar read:secret',
    iat: now,
    exp: now + 365 * 24 * 60 * 60, // 1 year expiration
  };

  // Encode header and payload
  const encodedHeader = base64UrlEncode(Buffer.from(JSON.stringify(header)));
  const encodedPayload = base64UrlEncode(Buffer.from(JSON.stringify(payload)));

  // Create signature
  const signatureInput = `${encodedHeader}.${encodedPayload}`;
  const signature = createHmac('sha256', secret).update(signatureInput).digest();
  const encodedSignature = base64UrlEncode(signature);

  // Return complete JWT
  return `${signatureInput}.${encodedSignature}`;
}
