---
title: Distribution Algorithms
description: Choose how keys are distributed across Memcache nodes with Ketama, modulo, or broadcast hashing.
order: 4
---

Memcache supports pluggable distribution algorithms to determine how keys are distributed across nodes. You can configure the algorithm using the `hash` option.

## KetamaHash (Default)

KetamaHash uses the Ketama consistent hashing algorithm, which minimizes key redistribution when nodes are added or removed. This is the default and recommended algorithm for production environments with dynamic scaling.

```javascript
import { Memcache } from 'memcache';

// KetamaHash is used by default
const client = new Memcache({
  nodes: ['server1:11211', 'server2:11211', 'server3:11211']
});
```

**Characteristics:**
- Minimal key redistribution (~1/n keys move when adding/removing nodes)
- Uses virtual nodes for better distribution
- Supports weighted nodes
- Best for production environments with dynamic scaling

## ModulaHash

ModulaHash uses a simple modulo-based hashing algorithm (`hash(key) % nodeCount`). This is a simpler algorithm that may redistribute all keys when nodes change.

```javascript
import { Memcache, ModulaHash } from 'memcache';

// Use ModulaHash for distribution
const client = new Memcache({
  nodes: ['server1:11211', 'server2:11211', 'server3:11211'],
  hash: new ModulaHash()
});

// With a custom hash algorithm (default is sha1)
const client2 = new Memcache({
  nodes: ['server1:11211', 'server2:11211'],
  hash: new ModulaHash('md5')
});
```

**Characteristics:**
- Simple and fast algorithm
- All keys may be redistributed when nodes are added or removed
- Supports weighted nodes (nodes with higher weight appear more in the distribution)
- Best for fixed-size clusters or testing environments

### Weighted Nodes with ModulaHash

ModulaHash supports weighted nodes, where nodes with higher weights receive proportionally more keys:

```javascript
import { Memcache, ModulaHash, createNode } from 'memcache';

// Create nodes with different weights
const node1 = createNode('server1', 11211, { weight: 3 }); // 3x traffic
const node2 = createNode('server2', 11211, { weight: 1 }); // 1x traffic

const client = new Memcache({
  nodes: [node1, node2],
  hash: new ModulaHash()
});

// server1 will receive approximately 75% of keys
// server2 will receive approximately 25% of keys
```

## BroadcastHash

BroadcastHash sends every operation to all nodes in the cluster. Instead of partitioning keys across nodes, every `getNodesByKey()` call returns all nodes, so reads and writes are broadcast to every server.

```javascript
import { Memcache, BroadcastHash } from 'memcache';

// Use BroadcastHash for full replication
const client = new Memcache({
  nodes: ['server1:11211', 'server2:11211', 'server3:11211'],
  hash: new BroadcastHash()
});

// Every set/get/delete hits all three nodes
await client.set('mykey', 'Hello!');
```

**Characteristics:**
- Every operation targets all nodes
- No key partitioning — all nodes hold the same data
- Reads return the first successful result from any node
- Writes succeed only if all nodes succeed
- Best for replication, broadcast invalidation, or small clusters where all nodes should be in sync

## Choosing an Algorithm

| Feature | KetamaHash | ModulaHash | BroadcastHash |
|---------|------------|------------|---------------|
| Key redistribution on node change | Minimal (~1/n keys) | All keys may move | N/A (all nodes always) |
| Complexity | Higher (virtual nodes) | Lower (simple modulo) | Simplest |
| Performance | Slightly slower | Faster | Depends on node count |
| Best for | Dynamic scaling | Fixed clusters | Replication |
| Weighted nodes | Yes | Yes | No |

**Use KetamaHash (default) when:**
- Your cluster size may change dynamically
- You want to minimize cache invalidation during scaling
- You're running in production

**Use ModulaHash when:**
- Your cluster size is fixed
- You prefer simplicity over minimal redistribution
- You're in a testing or development environment

**Use BroadcastHash when:**
- You want all nodes to hold the same data
- You need broadcast cache invalidation across all nodes
- You're running a small cluster where replication is more important than partitioning
