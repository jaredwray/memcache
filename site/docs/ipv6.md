---
title: IPv6 Support
description: Connect to IPv6 Memcache nodes using bracket-notation URIs, including Auto Discovery.
order: 8
---

The Memcache client fully supports IPv6 addresses using standard bracket notation in URIs.

## Connecting to IPv6 Nodes

```javascript
import { Memcache } from 'memcache';

// IPv6 loopback
const client = new Memcache('[::1]:11211');

// Multiple IPv6 nodes
const client = new Memcache({
  nodes: [
    '[::1]:11211',
    '[2001:db8::1]:11211',
    'memcache://[2001:db8::2]:11212',
  ],
});

await client.connect();
```

## IPv6 in Auto Discovery

When auto discovery returns IPv6 node addresses, the client automatically brackets them for correct URI handling:

```javascript
const client = new Memcache({
  nodes: [],
  autoDiscover: {
    enabled: true,
    configEndpoint: '[2001:db8::1]:11211',
  },
});

await client.connect();
// Discovered IPv6 nodes are added as [host]:port automatically
```

## IPv6 Node IDs

Node IDs for IPv6 addresses use bracket notation to avoid ambiguity:

```javascript
const client = new Memcache({
  nodes: ['[::1]:11211', '[2001:db8::1]:11212'],
});

console.log(client.nodeIds);
// ['[::1]:11211', '[2001:db8::1]:11212']
```
