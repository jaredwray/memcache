---
title: Getting Started
description: Install memcache and connect to one or more Memcache servers.
order: 1
---

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

You can also just pass in the `uri` into the constructor

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

You can specify multiple Memcache nodes by passing an array of connection strings:

```javascript
import { Memcache } from 'memcache';

// Create a client with multiple nodes
const client = new Memcache({
  nodes: ['localhost:11211', '192.168.1.100:11211', 'memcache://192.168.1.101:11211']
});

// Set and get values (automatically distributed across nodes)
await client.set('mykey', 'Hello, Memcache!');
const value = await client.get('mykey');
console.log(value); // ['Hello, Memcache!']

// Close the connection
await client.quit();
```

You can also pass an array of MemcacheNode instances for advanced configuration:

```javascript
import { Memcache, createNode } from 'memcache';

// Create nodes with custom settings
const node1 = createNode('localhost', 11211, { weight: 2 });
const node2 = createNode('192.168.1.100', 11211, { weight: 1 });
const node3 = createNode('192.168.1.101', 11211, { weight: 1 });

// Create a client with MemcacheNode instances
const client = new Memcache({
  nodes: [node1, node2, node3],
  timeout: 10000
});

// node1 will receive twice as much traffic due to higher weight
await client.set('mykey', 'Hello, Memcache!');
const value = await client.get('mykey');
console.log(value); // ['Hello, Memcache!']

// Close the connection
await client.quit();
```
