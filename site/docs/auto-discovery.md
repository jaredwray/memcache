---
title: Auto Discovery
description: Automatically detect AWS ElastiCache cluster topology changes and keep nodes in sync.
order: 7
---

The Memcache client supports AWS ElastiCache Auto Discovery, which automatically detects cluster topology changes and adds or removes nodes as needed. When enabled, the client connects to a configuration endpoint, retrieves the current list of cache nodes, and periodically polls for changes.

## Enabling Auto Discovery

```javascript
import { Memcache } from 'memcache';

const client = new Memcache({
  nodes: [],
  autoDiscover: {
    enabled: true,
    configEndpoint: 'my-cluster.cfg.use1.cache.amazonaws.com:11211',
  },
});

await client.connect();
// The client automatically discovers and connects to all cluster nodes
```

If you omit `configEndpoint`, the first node in the `nodes` array is used as the configuration endpoint:

```javascript
const client = new Memcache({
  nodes: ['my-cluster.cfg.use1.cache.amazonaws.com:11211'],
  autoDiscover: {
    enabled: true,
  },
});
```

## Auto Discovery Options

The `autoDiscover` option accepts an object with the following properties:

- `enabled: boolean` - Enable auto discovery of cluster nodes (required)
- `pollingInterval?: number` - How often to poll for topology changes, in milliseconds (default: 60000)
- `configEndpoint?: string` - The configuration endpoint to use for discovery. This is typically the `.cfg` endpoint from ElastiCache. If not specified, the first node in the `nodes` array will be used
- `useLegacyCommand?: boolean` - Use the legacy `get AmazonElastiCache:cluster` command instead of `config get cluster` (default: false)

## Auto Discovery Events

The client emits events during the auto discovery lifecycle:

```javascript
const client = new Memcache({
  nodes: [],
  autoDiscover: {
    enabled: true,
    configEndpoint: 'my-cluster.cfg.use1.cache.amazonaws.com:11211',
  },
});

// Emitted on initial discovery with the full cluster config
client.on('autoDiscover', (config) => {
  console.log('Discovered nodes:', config.nodes);
  console.log('Config version:', config.version);
});

// Emitted when polling detects a topology change
client.on('autoDiscoverUpdate', (config) => {
  console.log('Cluster topology changed:', config.nodes);
});

// Emitted when discovery encounters an error (non-fatal, retries on next poll)
client.on('autoDiscoverError', (error) => {
  console.error('Discovery error:', error.message);
});

await client.connect();
```

## Legacy Command Support

For ElastiCache engine versions older than 1.4.14, use the legacy discovery command:

```javascript
const client = new Memcache({
  nodes: [],
  autoDiscover: {
    enabled: true,
    configEndpoint: 'my-cluster.cfg.use1.cache.amazonaws.com:11211',
    useLegacyCommand: true, // Uses 'get AmazonElastiCache:cluster' instead of 'config get cluster'
  },
});
```
