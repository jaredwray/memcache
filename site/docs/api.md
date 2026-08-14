---
title: API
description: Constructor, options, properties, and protocol operations for the Memcache client.
order: 2
---

## Constructor

```typescript
new Memcache(options?: string | MemcacheOptions)
```

Creates a new Memcache client instance. You can pass either:
- A **string** representing a single node URI (uses default settings)
- A **MemcacheOptions** object for custom configuration

**Examples:**

```javascript
// Single node as string
const client = new Memcache('localhost:11211');

// Single node with protocol
const client = new Memcache('memcache://192.168.1.100:11211');

// Multiple nodes with options
const client = new Memcache({
  nodes: ['localhost:11211', 'server2:11211'],
  timeout: 10000
});
```

### Options

- `nodes?: (string | MemcacheNode)[]` - Array of node URIs or MemcacheNode instances
  - Examples: `["localhost:11211", "memcache://192.168.1.100:11212"]`
- `timeout?: number` - Operation timeout in milliseconds (default: 5000)
- `keepAlive?: boolean` - Keep connection alive (default: true)
- `keepAliveDelay?: number` - Keep alive delay in milliseconds (default: 1000)
- `hash?: HashProvider` - Hash provider for consistent hashing (default: KetamaHash)
- `retries?: number` - Number of retry attempts for failed commands (default: 0)
- `retryDelay?: number` - Base delay in milliseconds between retries (default: 100)
- `retryBackoff?: RetryBackoffFunction` - Function to calculate backoff delay (default: fixed delay)
- `retryOnlyIdempotent?: boolean` - Only retry commands marked as idempotent (default: true)
- `lazyConnect?: boolean` - When `true`, nodes will not connect until the first command is executed. When `false`, nodes connect eagerly during construction (default: true)
- `maxKeySize?: number` - Maximum allowed key size in characters (default: 250, memcache protocol max)
- `maxValueSize?: number` - Maximum allowed value size in bytes (default: 1048576, memcached default)
- `maxExpiration?: number` - Maximum allowed expiration in seconds (default: 2592000, memcached's 30-day relative-time boundary). Values above this throw. `0` (no expiration) is always allowed. Raise this if you need to pass absolute Unix timestamps as expirations.
- `hashLargeKey?: boolean | Hashery` - When `true`, keys longer than `maxKeySize` are deterministically hashed via [`hashery`](https://github.com/jaredwray/hashery) (djb2 sync by default) before being sent to the server, instead of throwing. Pass a configured `Hashery` instance (e.g. `new Hashery({ defaultAlgorithmSync: 'fnv1' })`) to choose a different sync algorithm or plug in custom providers. When `false`, oversized keys throw a validation error (default: false). Note: hashing is one-way and can collide; two distinct long keys could map to the same hashed key.
- `autoDiscover?: AutoDiscoverOptions` - AWS ElastiCache Auto Discovery configuration (see [Auto Discovery](/docs/auto-discovery/))

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

### `retries: number`
Get or set the number of retry attempts for failed commands (default: 0).

### `retryDelay: number`
Get or set the base delay in milliseconds between retry attempts (default: 100).

### `retryBackoff: RetryBackoffFunction`
Get or set the backoff function for calculating retry delays.

### `retryOnlyIdempotent: boolean`
Get or set whether retries are restricted to idempotent commands only (default: true).

### `maxKeySize: number`
Get or set the maximum allowed key size in characters (default: 250). Memcache protocol max is 250.

### `maxValueSize: number`
Get or set the maximum allowed value size in bytes (default: 1048576). Writes (`set`, `add`, `replace`, `append`, `prepend`, `cas`) throw when the encoded value exceeds this limit. Raise it if your memcached server is started with a larger `-I` item size.

### `maxExpiration: number`
Get or set the maximum allowed expiration in seconds (default: 2592000). Writes that accept an expiration (`set`, `add`, `replace`, `cas`, `touch`) throw when `exptime` exceeds this limit. `0` (no expiration) is always allowed. Memcached treats any `exptime` greater than 2592000 as an absolute Unix timestamp, so the default guards against accidentally setting a TTL that memcached interprets as "already expired." Raise this if you need to pass Unix timestamps.

### `hashLargeKey: boolean`
Get or set whether keys exceeding `maxKeySize` are hashed instead of throwing (default: false). When enabled, oversized keys are replaced with a short, deterministic hex digest (via the [`hashery`](https://github.com/jaredwray/hashery) library, djb2 by default) before being sent to memcache, so any string length is accepted. The same input always produces the same hashed key, but distinct long keys can collide. To change algorithm or providers, configure the [`hashery`](#hashery-hashery) property.

```javascript
const client = new Memcache({ hashLargeKey: true });
const longKey = 'user:profile:' + 'x'.repeat(500);
await client.set(longKey, 'value');     // hashed automatically
await client.get(longKey);              // same hash, returns 'value'
```

### `hashery: Hashery`
Get or set the `Hashery` instance used to hash oversized keys when `hashLargeKey` is enabled. Always returns an instance, even when hashing is disabled, so you can pre-configure it (algorithm, custom providers, caching) before flipping `hashLargeKey` on. `Hashery` is re-exported from this package for convenience.

```javascript
import Memcache, { Hashery } from 'memcache';

// Simple — defaults to djb2 sync hashing
const simple = new Memcache({ hashLargeKey: true });

// Advanced — supply a Hashery preconfigured for fnv1
const advanced = new Memcache({
  hashLargeKey: new Hashery({ defaultAlgorithmSync: 'fnv1' }),
});

// Or mutate the instance after construction
simple.hashery.defaultAlgorithmSync = 'murmur';
```

### `lazyConnect: boolean` (readonly)
Whether nodes defer connecting until the first command is executed (default: true).

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

### `resolveKey(key: string): string`
Returns the key that will actually be sent to memcache. When [`hashLargeKey`](#hashlargekey-boolean) is `true` and the key length exceeds `maxKeySize`, returns a short hex digest produced by the configured [`hashery`](#hashery-hashery) instance (djb2 by default — 8 hex chars). Otherwise returns the original key unchanged. Called automatically before `validateKey` in every command, so calling it manually is only needed when you want to inspect the on-wire key.

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
