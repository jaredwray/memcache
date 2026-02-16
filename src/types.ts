import type { CommandOptions, MemcacheNode } from "./node.js";

/**
 * SASL authentication credentials for connecting to a secured memcache server.
 */
export interface SASLCredentials {
	/**
	 * Username for SASL authentication
	 */
	username: string;
	/**
	 * Password for SASL authentication
	 */
	password: string;
	/**
	 * SASL mechanism to use (default: 'PLAIN')
	 * Currently only 'PLAIN' is supported.
	 */
	mechanism?: "PLAIN";
}

export enum MemcacheEvents {
	CONNECT = "connect",
	QUIT = "quit",
	HIT = "hit",
	MISS = "miss",
	ERROR = "error",
	WARN = "warn",
	INFO = "info",
	TIMEOUT = "timeout",
	CLOSE = "close",
	AUTO_DISCOVER = "autoDiscover",
	AUTO_DISCOVER_ERROR = "autoDiscoverError",
	AUTO_DISCOVER_UPDATE = "autoDiscoverUpdate",
}

/**
 * Function to calculate delay between retry attempts.
 * @param attempt - The current attempt number (0-indexed)
 * @param baseDelay - The base delay in milliseconds
 * @returns The delay in milliseconds before the next retry
 */
export type RetryBackoffFunction = (
	attempt: number,
	baseDelay: number,
) => number;

export interface HashProvider {
	name: string;
	nodes: Array<MemcacheNode>;
	addNode: (node: MemcacheNode) => void;
	removeNode: (id: string) => void;
	getNode: (id: string) => MemcacheNode | undefined;
	getNodesByKey: (key: string) => Array<MemcacheNode>;
}

export interface MemcacheOptions {
	/**
	 * Array of node URIs or MemcacheNode instances to add to the consistent hashing ring.
	 * Examples: ["localhost:11211", "memcache://192.168.1.100:11212", "server3:11213"]
	 * Can also pass MemcacheNode instances directly: [createNode("localhost", 11211), createNode("server2", 11211)]
	 */
	nodes?: (string | MemcacheNode)[];
	/**
	 * The timeout for Memcache operations.
	 * @default 5000
	 */
	timeout?: number;
	/**
	 * Whether to keep the connection alive.
	 * @default true
	 */
	keepAlive?: boolean;
	/**
	 * The delay before the connection is kept alive.
	 * @default 1000
	 */
	keepAliveDelay?: number;

	/**
	 * The hash provider used to determine the distribution on each item is placed based
	 * on the number of nodes and hashing. By default it uses KetamaHash as the provider
	 */
	hash?: HashProvider;

	/**
	 * The number of retry attempts for failed commands.
	 * Set to 0 to disable retries.
	 * @default 0
	 */
	retries?: number;

	/**
	 * The base delay in milliseconds between retry attempts.
	 * @default 100
	 */
	retryDelay?: number;

	/**
	 * Function to calculate backoff delay between retries.
	 * Receives (attempt, baseDelay) and returns delay in ms.
	 * @default defaultRetryBackoff (fixed delay)
	 */
	retryBackoff?: RetryBackoffFunction;

	/**
	 * When true, retries are only performed for commands marked as idempotent.
	 * This prevents accidental double-execution of non-idempotent operations
	 * (like incr, decr, append) if the server applies the command but the
	 * client doesn't receive the response before a timeout/disconnect.
	 * @default true
	 */
	retryOnlyIdempotent?: boolean;

	/**
	 * SASL authentication credentials for all nodes.
	 * When provided, nodes will authenticate using SASL PLAIN mechanism
	 * before accepting commands.
	 */
	sasl?: SASLCredentials;

	/**
	 * AWS ElastiCache Auto Discovery configuration.
	 * When enabled, the client will periodically poll the configuration endpoint
	 * to detect cluster topology changes and automatically update the node list.
	 */
	autoDiscover?: AutoDiscoverOptions;
}

export interface MemcacheStats {
	[key: string]: string;
}

/**
 * Configuration for AWS ElastiCache Auto Discovery.
 * When enabled, the client connects to a configuration endpoint and periodically
 * polls for cluster topology changes, automatically adding/removing nodes.
 */
export interface AutoDiscoverOptions {
	/**
	 * Enable auto discovery of cluster nodes.
	 */
	enabled: boolean;
	/**
	 * How often to poll for topology changes, in milliseconds.
	 * @default 60000
	 */
	pollingInterval?: number;
	/**
	 * The configuration endpoint to use for discovery.
	 * This is typically the .cfg endpoint from ElastiCache.
	 * If not specified, the first node in the nodes array will be used.
	 */
	configEndpoint?: string;
	/**
	 * Use the legacy `get AmazonElastiCache:cluster` command
	 * instead of `config get cluster` (for engine versions < 1.4.14).
	 * @default false
	 */
	useLegacyCommand?: boolean;
}

/**
 * Represents a discovered node from the ElastiCache cluster configuration.
 */
export interface DiscoveredNode {
	/** The hostname of the node */
	hostname: string;
	/** The IP address of the node (may be empty string) */
	ip: string;
	/** The port number of the node */
	port: number;
}

/**
 * Represents the parsed result of an ElastiCache cluster config response.
 */
export interface ClusterConfig {
	/** The config version number (increments on topology changes) */
	version: number;
	/** The list of discovered nodes */
	nodes: DiscoveredNode[];
}

export interface ExecuteOptions {
	/** Command options passed to node.command() */
	commandOptions?: CommandOptions;

	/**
	 * Override the number of retries for this specific execution.
	 * If undefined, uses the instance-level retries setting.
	 */
	retries?: number;

	/**
	 * Override the retry delay for this specific execution.
	 * If undefined, uses the instance-level retryDelay setting.
	 */
	retryDelay?: number;

	/**
	 * Override the backoff function for this specific execution.
	 * If undefined, uses the instance-level retryBackoff setting.
	 */
	retryBackoff?: RetryBackoffFunction;

	/**
	 * Mark this command as idempotent, allowing retries even when
	 * retryOnlyIdempotent is true. Set this for read operations (get, gets)
	 * or operations that are safe to repeat (set with same value).
	 * @default false
	 */
	idempotent?: boolean;
}
