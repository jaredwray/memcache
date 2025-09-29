[<img src="./site/logo_medium.png" alt="Memcache Logo" align="center">](https://memcachejs.org)

# Memcache
Nodejs Memcache Client

## Hooks and Events

The Memcache client extends [Hookified](https://github.com/jaredwray/hookified) to provide powerful hooks and events for monitoring and customizing behavior.

### Events

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

#### Available Events

- `connect` - Emitted when connection to Memcache server is established
- `close` - Emitted when connection is closed
- `error` - Emitted when an error occurs
- `timeout` - Emitted when a connection timeout occurs
- `hit` - Emitted when a key is found in cache (includes key and value)
- `miss` - Emitted when a key is not found in cache
- `quit` - Emitted when quit command is sent
- `warn` - Emitted for warning messages
- `info` - Emitted for informational messages

### Hooks

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

#### Available Hooks

All operations support before and after hooks with specific parameters:

##### get(key)
- `before:get` - `{ key }`
- `after:get` - `{ key, value }` (value is array or undefined)

##### set(key, value, exptime?, flags?)
- `before:set` - `{ key, value, exptime, flags }`
- `after:set` - `{ key, value, exptime, flags, success }`

##### gets(keys[])
- `before:gets` - `{ keys }`
- `after:gets` - `{ keys, values }` (values is a Map)

##### add(key, value, exptime?, flags?)
- `before:add` - `{ key, value, exptime, flags }`
- `after:add` - `{ key, value, exptime, flags, success }`

##### replace(key, value, exptime?, flags?)
- `before:replace` - `{ key, value, exptime, flags }`
- `after:replace` - `{ key, value, exptime, flags, success }`

##### append(key, value)
- `before:append` - `{ key, value }`
- `after:append` - `{ key, value, success }`

##### prepend(key, value)
- `before:prepend` - `{ key, value }`
- `after:prepend` - `{ key, value, success }`

##### delete(key)
- `before:delete` - `{ key }`
- `after:delete` - `{ key, success }`

##### incr(key, value?)
- `before:incr` - `{ key, value }`
- `after:incr` - `{ key, value, newValue }`

##### decr(key, value?)
- `before:decr` - `{ key, value }`
- `after:decr` - `{ key, value, newValue }`

##### touch(key, exptime)
- `before:touch` - `{ key, exptime }`
- `after:touch` - `{ key, exptime, success }`

### Hook Examples

#### Logging All Operations

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

#### Input Validation

```javascript
// Validate keys before operations
client.onHook('before:set', async ({ key, value }) => {
  if (key.length > 100) {
    throw new Error('Key too long');
  }
  if (typeof value === 'object') {
    // Automatically serialize objects
    return { value: JSON.stringify(value) };
  }
});

// Parse JSON on retrieval
client.onHook('after:get', async ({ value }) => {
  if (value && value[0]) {
    try {
      const parsed = JSON.parse(value[0]);
      return { value: [parsed] };
    } catch (e) {
      // Not JSON, return as-is
    }
  }
});
```

#### Removing Hooks

```javascript
// Add a hook and get the removal function
const hook = client.onHook('before:get', myHookFunction);

// Or remove by reference
client.removeHook('before:get', myHookFunction);

// Clear all hooks
client.clearHooks();
```
