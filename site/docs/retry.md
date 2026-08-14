---
title: Retry Configuration
description: Automatic command retries with fixed, exponential, or custom backoff and idempotent safety.
order: 5
---

The Memcache client supports automatic retry of failed commands with configurable backoff strategies.

## Basic Retry Setup

Enable retries by setting the `retries` option:

```javascript
import { Memcache } from 'memcache';

const client = new Memcache({
  nodes: ['localhost:11211'],
  retries: 3,        // Retry up to 3 times
  retryDelay: 100    // 100ms between retries
});
```

You can also modify retry settings at runtime:

```javascript
client.retries = 5;
client.retryDelay = 200;
```

## Backoff Strategies

The client includes two built-in backoff functions:

### Fixed Delay (Default)

```javascript
import { Memcache, defaultRetryBackoff } from 'memcache';

const client = new Memcache({
  retries: 3,
  retryDelay: 100,
  retryBackoff: defaultRetryBackoff  // 100ms, 100ms, 100ms
});
```

### Exponential Backoff

```javascript
import { Memcache, exponentialRetryBackoff } from 'memcache';

const client = new Memcache({
  retries: 3,
  retryDelay: 100,
  retryBackoff: exponentialRetryBackoff  // 100ms, 200ms, 400ms
});
```

### Custom Backoff Function

You can provide your own backoff function:

```javascript
const client = new Memcache({
  retries: 3,
  retryDelay: 100,
  retryBackoff: (attempt, baseDelay) => {
    // Exponential backoff with jitter
    const delay = baseDelay * Math.pow(2, attempt);
    return delay + Math.random() * delay * 0.1;
  }
});
```

The backoff function receives:
- `attempt` - The current attempt number (0-indexed)
- `baseDelay` - The configured `retryDelay` value

## Idempotent Safety

**Important:** By default, retries are only performed for commands explicitly marked as idempotent. This prevents accidental double-execution of non-idempotent operations like `incr`, `decr`, `append`, and `prepend`.

### Why This Matters

If a network timeout occurs after the server applies a mutation but before the client receives the response, retrying would apply the mutation twice:
- Counter incremented twice instead of once
- Data appended twice instead of once

### Safe Usage Patterns

**For read operations (always safe to retry):**

```javascript
// Mark read operations as idempotent
await client.execute('get mykey', nodes, { idempotent: true });
```

**For idempotent writes (safe to retry):**

```javascript
// SET with the same value is idempotent
await client.execute('set mykey 0 0 5\r\nhello', nodes, { idempotent: true });
```

**Disable safety for all commands (use with caution):**

```javascript
const client = new Memcache({
  retries: 3,
  retryOnlyIdempotent: false  // Allow retries for ALL commands
});
```

### Behavior Summary

| `retryOnlyIdempotent` | `idempotent` flag | Retries enabled? |
|-----------------------|-------------------|------------------|
| `true` (default)      | `false` (default) | No               |
| `true` (default)      | `true`            | Yes              |
| `false`               | (any)             | Yes              |

## Methods Without Retry Support

The following methods do not use the retry mechanism and have their own error handling:

- `get()` - Returns `undefined` on failure
- `gets()` - Returns partial results on node failure
- `flush()` - Operates directly on nodes
- `stats()` - Operates directly on nodes
- `version()` - Operates directly on nodes

To use retries with read operations, use the `execute()` method directly:

```javascript
const nodes = await client.getNodesByKey('mykey');
const results = await client.execute('get mykey', nodes, { idempotent: true });
```
