[<img src="./site/logo_medium.png" alt="Memcache Logo" align="center">](https://memcachejs.org)

[![codecov](https://codecov.io/gh/jaredwray/memcache/graph/badge.svg?token=4DUANNWiIE)](https://codecov.io/gh/jaredwray/memcache)
[![tests](https://github.com/jaredwray/memcache/actions/workflows/tests.yml/badge.svg)](https://github.com/jaredwray/memcache/actions/workflows/tests.yml)
[![npm](https://img.shields.io/npm/v/memcache)](https://www.npmjs.com/package/memcache)
[![npm](https://img.shields.io/npm/dm/memcache)](https://www.npmjs.com/package/memcache)
[![license](https://img.shields.io/github/license/jaredwray/memcache)](https://github.com/jaredwray/memcache/blob/main/LICENSE)

# Memcache
Nodejs Memcache Client

# Table of Contents

- [Getting Started](#getting-started)
  - [Installation](#installation)
  - [Basic Usage](#basic-usage)
  - [Custom Connection](#custom-connection)
- [API](#api)
  - [Constructor](#constructor)
  - [Properties](#properties)
  - [Connection Management](#connection-management)
  - [Node Management](#node-management)
  - [Data Storage Operations](#data-storage-operations)
  - [String Modification Operations](#string-modification-operations)
  - [Deletion & Expiration](#deletion--expiration)
  - [Numeric Operations](#numeric-operations)
  - [Server Management & Statistics](#server-management--statistics)
  - [Validation](#validation)
  - [Helper Functions](#helper-functions)
- [Hooks and Events](#hooks-and-events)
  - [Events](#events)
  - [Available Events](#available-events)
  - [Hooks](#hooks)
  - [Available Hooks](#available-hooks)
    - [get(key)](#getkey)
    - [set(key, value, exptime?, flags?)](#setkey-value-exptime-flags)
    - [gets(keys[])](#getskeys)
    - [add(key, value, exptime?, flags?)](#addkey-value-exptime-flags)
    - [replace(key, value, exptime?, flags?)](#replacekey-value-exptime-flags)
    - [append(key, value)](#appendkey-value)
    - [prepend(key, value)](#prependkey-value)
    - [delete(key)](#deletekey)
    - [incr(key, value?)](#incrkey-value)
    - [decr(key, value?)](#decrkey-value)
    - [touch(key, exptime)](#touchkey-exptime)
  - [Hook Examples](#hook-examples)
- [Contributing](#contributing)
- [License and Copyright](#license-and-copyright)

# Getting Started

## Installation

```bash
npm install memcache
```

or with pnpm:

```bash
pnpm add memcache
```

## Basic Usage

```javascript
import { Memcache } from 'memcache';

// Create a new client
const client = new Memcache();

// Set a value
await client.set('mykey', 'Hello, Memcache!');

// Get a value
const value = await client.get('mykey');
console.log(value); // ['Hello, Memcache!']

// Delete a value
await client.delete('mykey');

// Close the connection
await client.quit();
```

# API

## Constructor

```typescript
new Memcache(options?: MemcacheOptions)
```

Creates a new Memcache client instance.

### Options

- `nodes?: (string | MemcacheNode)[]` - Array of node URIs or MemcacheNode instances
  - Examples: `["localhost:11211", "memcache://192.168.1.100:11212"]`
- `timeout?: number` - Operation timeout in milliseconds (default: 5000)
- `keepAlive?: boolean` - Keep connection alive (default: true)
- `keepAliveDelay?: number` - Keep alive delay in milliseconds (default: 1000)
- `hash?: HashProvider` - Hash provider for consistent hashing (default: KetamaHash)

## Properties

### `nodes: MemcacheNode[]` (readonly)
Returns the list of all MemcacheNode instances in the cluster.

### `nodeIds: string[]` (readonly)
Returns the list of node IDs (e.g., `["localhost:11211", "127.0.0.1:11212"]`).

### `hash: HashProvider`
Get or set the hash provider used for consistent hashing distribution.

### `timeout: number`
Get or set the timeout for operations in milliseconds (default: 5000).

### `keepAlive: boolean`
Get or set the keepAlive setting. Updates all existing nodes. Requires `reconnect()` to apply changes.

### `keepAliveDelay: number`
Get or set the keep alive delay in milliseconds. Updates all existing nodes. Requires `reconnect()` to apply changes.

## Connection Management

### `connect(nodeId?: string): Promise<void>`
Connect to all Memcache servers or a specific node.

### `disconnect(): Promise<void>`
Disconnect all connections.

### `reconnect(): Promise<void>`
Reconnect all nodes by disconnecting and connecting them again.

### `quit(): Promise<void>`
Quit all connections gracefully.

### `isConnected(): boolean`
Check if any node is connected to a Memcache server.

## Node Management

### `getNodes(): MemcacheNode[]`
Get an array of all MemcacheNode instances.

### `getNode(id: string): MemcacheNode | undefined`
Get a specific node by its ID (e.g., `"localhost:11211"`).

### `addNode(uri: string | MemcacheNode, weight?: number): Promise<void>`
Add a new node to the cluster. Throws error if node already exists.

### `removeNode(uri: string): Promise<void>`
Remove a node from the cluster.

### `getNodesByKey(key: string): Promise<MemcacheNode[]>`
Get the nodes for a given key using consistent hashing. Automatically connects to nodes if not already connected.

### `parseUri(uri: string): { host: string; port: number }`
Parse a URI string into host and port. Supports formats:
- Simple: `"localhost:11211"` or `"localhost"`
- Protocol: `"memcache://localhost:11211"`, `"tcp://localhost:11211"`
- IPv6: `"[::1]:11211"` or `"memcache://[2001:db8::1]:11212"`
- Unix socket: `"/var/run/memcached.sock"` or `"unix:///var/run/memcached.sock"`

## Data Storage Operations

### `get(key: string): Promise<string | undefined>`
Get a value from the Memcache server. Returns the first successful result from replica nodes.

### `gets(keys: string[]): Promise<Map<string, string>>`
Get multiple values from the Memcache server. Returns a Map with keys to values.

### `set(key: string, value: string, exptime?: number, flags?: number): Promise<boolean>`
Set a value in the Memcache server. Returns true only if all replica nodes succeed.
- `exptime` - Expiration time in seconds (default: 0 = never expire)
- `flags` - Flags/metadata (default: 0)

### `add(key: string, value: string, exptime?: number, flags?: number): Promise<boolean>`
Add a value (only if key doesn't exist). Returns true only if all replica nodes succeed.

### `replace(key: string, value: string, exptime?: number, flags?: number): Promise<boolean>`
Replace a value (only if key exists). Returns true only if all replica nodes succeed.

### `cas(key: string, value: string, casToken: string, exptime?: number, flags?: number): Promise<boolean>`
Check-And-Set: Store a value only if it hasn't been modified since last fetch. Returns true only if all replica nodes succeed.

## String Modification Operations

### `append(key: string, value: string): Promise<boolean>`
Append a value to an existing key. Returns true only if all replica nodes succeed.

### `prepend(key: string, value: string): Promise<boolean>`
Prepend a value to an existing key. Returns true only if all replica nodes succeed.

## Deletion & Expiration

### `delete(key: string): Promise<boolean>`
Delete a value from the Memcache server. Returns true only if all replica nodes succeed.

### `touch(key: string, exptime: number): Promise<boolean>`
Update expiration time without retrieving value. Returns true only if all replica nodes succeed.

## Numeric Operations

### `incr(key: string, value?: number): Promise<number | undefined>`
Increment a value. Returns the new value or undefined on failure.
- `value` - Amount to increment (default: 1)

### `decr(key: string, value?: number): Promise<number | undefined>`
Decrement a value. Returns the new value or undefined on failure.
- `value` - Amount to decrement (default: 1)

## Server Management & Statistics

### `flush(delay?: number): Promise<boolean>`
Flush all values from all Memcache servers. Returns true if all nodes successfully flushed.
- `delay` - Optional delay in seconds before flushing

### `stats(type?: string): Promise<Map<string, MemcacheStats>>`
Get statistics from all Memcache servers. Returns a Map of node IDs to their stats.

### `version(): Promise<Map<string, string>>`
Get the Memcache server version from all nodes. Returns a Map of node IDs to version strings.

## Validation

### `validateKey(key: string): void`
Validates a Memcache key according to protocol requirements. Throws error if:
- Key is empty
- Key exceeds 250 characters
- Key contains spaces, newlines, or null characters

## Helper Functions

### `createNode(host: string, port: number, options?: MemcacheNodeOptions): MemcacheNode`
Factory function to create a new MemcacheNode instance.

```javascript
import { createNode } from 'memcache';

const node = createNode('localhost', 11211, {
  timeout: 5000,
  keepAlive: true,
  weight: 1
});
```

# Hooks and Events

The Memcache client extends [Hookified](https://github.com/jaredwray/hookified) to provide powerful hooks and events for monitoring and customizing behavior.

## Events

The client emits various events during operations that you can listen to:

```javascript
const client = new Memcache();

// Connection events
client.on('connect', () => {
  console.log('Connected to Memcache server');
});

client.on('close', () => {
  console.log('Connection closed');
});

client.on('error', (error) => {
  console.error('Error:', error);
});

client.on('timeout', () => {
  console.log('Connection timeout');
});

// Cache hit/miss events
client.on('hit', (key, value) => {
  console.log(`Cache hit for key: ${key}`);
});

client.on('miss', (key) => {
  console.log(`Cache miss for key: ${key}`);
});
```

## Available Events

- `connect` - Emitted when connection to Memcache server is established
- `close` - Emitted when connection is closed
- `error` - Emitted when an error occurs
- `timeout` - Emitted when a connection timeout occurs
- `hit` - Emitted when a key is found in cache (includes key and value)
- `miss` - Emitted when a key is not found in cache
- `quit` - Emitted when quit command is sent
- `warn` - Emitted for warning messages
- `info` - Emitted for informational messages

## Hooks

Hooks allow you to intercept and modify behavior before and after operations. Every operation supports `before` and `after` hooks.

```javascript
const client = new Memcache();

// Add a before hook for get operations
client.onHook('before:get', async ({ key }) => {
  console.log(`Getting key: ${key}`);
});

// Add an after hook for set operations
client.onHook('after:set', async ({ key, value, success }) => {
  if (success) {
    console.log(`Successfully set ${key}`);
  }
});

// Hooks can be async and modify behavior
client.onHook('before:set', async ({ key, value }) => {
  console.log(`About to set ${key} = ${value}`);
  // Perform validation, logging, etc.
});
```

## Available Hooks

All operations support before and after hooks with specific parameters:

## get(key)
- `before:get` - `{ key }`
- `after:get` - `{ key, value }` (value is array or undefined)

## set(key, value, exptime?, flags?)
- `before:set` - `{ key, value, exptime, flags }`
- `after:set` - `{ key, value, exptime, flags, success }`

## gets(keys[])
- `before:gets` - `{ keys }`
- `after:gets` - `{ keys, values }` (values is a Map)

## add(key, value, exptime?, flags?)
- `before:add` - `{ key, value, exptime, flags }`
- `after:add` - `{ key, value, exptime, flags, success }`

## replace(key, value, exptime?, flags?)
- `before:replace` - `{ key, value, exptime, flags }`
- `after:replace` - `{ key, value, exptime, flags, success }`

## append(key, value)
- `before:append` - `{ key, value }`
- `after:append` - `{ key, value, success }`

## prepend(key, value)
- `before:prepend` - `{ key, value }`
- `after:prepend` - `{ key, value, success }`

## delete(key)
- `before:delete` - `{ key }`
- `after:delete` - `{ key, success }`

## incr(key, value?)
- `before:incr` - `{ key, value }`
- `after:incr` - `{ key, value, newValue }`

## decr(key, value?)
- `before:decr` - `{ key, value }`
- `after:decr` - `{ key, value, newValue }`

## touch(key, exptime)
- `before:touch` - `{ key, exptime }`
- `after:touch` - `{ key, exptime, success }`

## Hook Examples

```javascript
const client = new Memcache();

// Log all get operations
client.onHook('before:get', async ({ key }) => {
  console.log(`[GET] Fetching key: ${key}`);
});

client.onHook('after:get', async ({ key, value }) => {
  console.log(`[GET] Key: ${key}, Found: ${value !== undefined}`);
});

// Log all set operations with timing
client.onHook('before:set', async (context) => {
  context.startTime = Date.now();
});

client.onHook('after:set', async (context) => {
  const duration = Date.now() - context.startTime;
  console.log(`[SET] Key: ${context.key}, Success: ${context.success}, Time: ${duration}ms`);
});
```

# Contributing

Please read our [Contributing Guidelines](./CONTRIBUTING.md) and also our [Code of Conduct](./CODE_OF_CONDUCT.md). 

# License and Copyright

[MIT & Copyright (c) Jared Wray](https://github.com/jaredwray/memcache/blob/main/LICENSE)