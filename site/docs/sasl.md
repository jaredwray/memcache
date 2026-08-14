---
title: SASL Authentication
description: Authenticate to SASL-enabled memcached servers with PLAIN credentials and the binary protocol.
order: 6
---

The Memcache client supports SASL (Simple Authentication and Security Layer) authentication using the PLAIN mechanism. This allows you to connect to memcached servers that require authentication.

## Enabling SASL Authentication

```javascript
import { Memcache } from 'memcache';

const client = new Memcache({
  nodes: ['localhost:11211'],
  sasl: {
    username: 'myuser',
    password: 'mypassword',
  },
});

await client.connect();
// Client is now authenticated and ready to use
```

## SASL Options

The `sasl` option accepts an object with the following properties:

- `username: string` - The username for authentication (required)
- `password: string` - The password for authentication (required)
- `mechanism?: 'PLAIN'` - The SASL mechanism to use (default: 'PLAIN')

Currently, only the PLAIN mechanism is supported.

## Binary Protocol Methods

**Important:** Memcached servers with SASL enabled (`-S` flag) require the binary protocol for all operations after authentication. The standard text-based methods (`client.get()`, `client.set()`, etc.) will not work on SASL-enabled servers.

Use the `binary*` methods on nodes for SASL-enabled servers:

```javascript
import { Memcache } from 'memcache';

const client = new Memcache({
  nodes: ['localhost:11211'],
  sasl: { username: 'user', password: 'pass' },
});

await client.connect();

// Access the node directly for binary operations
const node = client.nodes[0];

// Binary protocol operations
await node.binarySet('mykey', 'myvalue', 3600);     // Set with 1 hour expiry
const value = await node.binaryGet('mykey');         // Get value
await node.binaryDelete('mykey');                    // Delete key

// Other binary operations
await node.binaryAdd('newkey', 'value');             // Add (only if not exists)
await node.binaryReplace('existingkey', 'newvalue'); // Replace (only if exists)
await node.binaryIncr('counter', 1);                 // Increment
await node.binaryDecr('counter', 1);                 // Decrement
await node.binaryAppend('mykey', '-suffix');         // Append to value
await node.binaryPrepend('mykey', 'prefix-');        // Prepend to value
await node.binaryTouch('mykey', 7200);               // Update expiration
await node.binaryFlush();                            // Flush all
const version = await node.binaryVersion();          // Get server version
const stats = await node.binaryStats();              // Get server stats
```

## Per-Node SASL Configuration

You can also configure SASL credentials when creating individual nodes:

```javascript
import { createNode } from 'memcache';

// Create a node with SASL credentials
const node = createNode('localhost', 11211, {
  sasl: { username: 'user', password: 'pass' },
});

// Connect and use binary methods
await node.connect();
await node.binarySet('mykey', 'hello');
const value = await node.binaryGet('mykey');
```

## Authentication Events

You can listen for authentication events on both nodes and the client:

```javascript
import { Memcache, MemcacheNode } from 'memcache';

// Node-level events
const node = new MemcacheNode('localhost', 11211, {
  sasl: { username: 'user', password: 'pass' },
});

node.on('authenticated', () => {
  console.log('Node authenticated successfully');
});

node.on('error', (error) => {
  if (error.message.includes('SASL authentication failed')) {
    console.error('Authentication failed:', error.message);
  }
});

await node.connect();

// Client-level events (forwarded from nodes)
const client = new Memcache({
  nodes: ['localhost:11211'],
  sasl: { username: 'user', password: 'pass' },
});

client.on('authenticated', () => {
  console.log('Client authenticated');
});

await client.connect();
```

### Node Properties

- `node.hasSaslCredentials` - Returns `true` if SASL credentials are configured
- `node.isAuthenticated` - Returns `true` if the node has successfully authenticated

## Server Configuration

To use SASL authentication, your memcached server must be configured with SASL support:

1. **Build memcached with SASL support** - Ensure memcached was compiled with `--enable-sasl`

2. **Create SASL users** - Use `saslpasswd2` to create users:
   ```bash
   saslpasswd2 -a memcached -c username
   ```

3. **Configure SASL mechanism** - Create `/etc/sasl2/memcached.conf`:
   ```
   mech_list: plain
   ```

4. **Start memcached with SASL** - Use the `-S` flag:
   ```bash
   memcached -S -m 64 -p 11211
   ```

For more details, see the [memcached SASL documentation](https://github.com/memcached/memcached/wiki/SASLHowto).
