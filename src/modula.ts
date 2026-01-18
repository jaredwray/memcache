import { createHash } from "node:crypto";
import type { HashProvider } from "./index.js";
import type { MemcacheNode } from "./node.js";

/**
 * Function that returns an unsigned 32-bit hash of the input.
 */
export type HashFunction = (input: Buffer) => number;

/**
 * Creates a hash function using a built-in Node.js crypto algorithm.
 * @param algorithm - The name of the hashing algorithm (e.g., "sha1", "md5")
 * @returns A HashFunction that uses the specified algorithm
 */
const hashFunctionForBuiltin =
	(algorithm: string): HashFunction =>
	(value) =>
		createHash(algorithm).update(value).digest().readUInt32BE(0);

/**
 * A distribution hash implementation using modulo-based hashing.
 * This class provides a simple key distribution strategy where keys are
 * assigned to nodes using `hash(key) % nodeCount`.
 *
 * Unlike consistent hashing (Ketama), modulo hashing redistributes all keys
 * when nodes are added or removed. This makes it suitable for:
 * - Fixed-size clusters
 * - Testing environments
 * - Scenarios where simplicity is preferred over minimal redistribution
 *
 * @example
 * ```typescript
 * const distribution = new ModulaHash();
 * distribution.addNode(node1);
 * distribution.addNode(node2);
 * const targetNode = distribution.getNodesByKey('my-key')[0];
 * ```
 */
export class ModulaHash implements HashProvider {
	/** The name of this distribution strategy */
	public readonly name = "modula";

	/** The hash function used to compute key hashes */
	private readonly hashFn: HashFunction;

	/** Map of node IDs to MemcacheNode instances */
	private nodeMap: Map<string, MemcacheNode>;

	/**
	 * Weighted list of node IDs for modulo distribution.
	 * Nodes with higher weights appear multiple times.
	 */
	private nodeList: string[];

	/**
	 * Creates a new ModulaHash instance.
	 *
	 * @param hashFn - Hash function to use (string algorithm name or custom function, defaults to "sha1")
	 *
	 * @example
	 * ```typescript
	 * // Use default SHA-1 hashing
	 * const distribution = new ModulaHash();
	 *
	 * // Use MD5 hashing
	 * const distribution = new ModulaHash('md5');
	 *
	 * // Use custom hash function
	 * const distribution = new ModulaHash((buf) => buf.readUInt32BE(0));
	 * ```
	 */
	constructor(hashFn?: string | HashFunction) {
		this.hashFn =
			typeof hashFn === "string"
				? hashFunctionForBuiltin(hashFn)
				: (hashFn ?? hashFunctionForBuiltin("sha1"));
		this.nodeMap = new Map();
		this.nodeList = [];
	}

	/**
	 * Gets all nodes in the distribution.
	 * @returns Array of all MemcacheNode instances
	 */
	public get nodes(): Array<MemcacheNode> {
		return Array.from(this.nodeMap.values());
	}

	/**
	 * Adds a node to the distribution with its weight.
	 * Weight determines how many times the node appears in the distribution list.
	 *
	 * @param node - The MemcacheNode to add
	 *
	 * @example
	 * ```typescript
	 * const node = new MemcacheNode('localhost', 11211, { weight: 2 });
	 * distribution.addNode(node);
	 * ```
	 */
	public addNode(node: MemcacheNode): void {
		// Add to internal map for lookups
		this.nodeMap.set(node.id, node);

		// Add to weighted list based on node weight
		const weight = node.weight || 1;
		for (let i = 0; i < weight; i++) {
			this.nodeList.push(node.id);
		}
	}

	/**
	 * Removes a node from the distribution by its ID.
	 *
	 * @param id - The node ID (e.g., "localhost:11211")
	 *
	 * @example
	 * ```typescript
	 * distribution.removeNode('localhost:11211');
	 * ```
	 */
	public removeNode(id: string): void {
		// Remove from internal map
		this.nodeMap.delete(id);

		// Remove all occurrences from weighted list
		this.nodeList = this.nodeList.filter((nodeId) => nodeId !== id);
	}

	/**
	 * Gets a specific node by its ID.
	 *
	 * @param id - The node ID (e.g., "localhost:11211")
	 * @returns The MemcacheNode if found, undefined otherwise
	 *
	 * @example
	 * ```typescript
	 * const node = distribution.getNode('localhost:11211');
	 * if (node) {
	 *   console.log(`Found node: ${node.uri}`);
	 * }
	 * ```
	 */
	public getNode(id: string): MemcacheNode | undefined {
		return this.nodeMap.get(id);
	}

	/**
	 * Gets the nodes responsible for a given key using modulo hashing.
	 * Uses `hash(key) % nodeCount` to determine the target node.
	 *
	 * @param key - The cache key to find the responsible node for
	 * @returns Array containing the responsible node(s), empty if no nodes available
	 *
	 * @example
	 * ```typescript
	 * const nodes = distribution.getNodesByKey('user:123');
	 * if (nodes.length > 0) {
	 *   console.log(`Key will be stored on: ${nodes[0].id}`);
	 * }
	 * ```
	 */
	public getNodesByKey(key: string): Array<MemcacheNode> {
		if (this.nodeList.length === 0) {
			return [];
		}

		// Hash the key and get unsigned 32-bit integer
		const hash = this.hashFn(Buffer.from(key));

		// Modulo to get node index from weighted list
		const index = hash % this.nodeList.length;

		// Get the node ID from the weighted list
		const nodeId = this.nodeList[index];

		// Map back to MemcacheNode
		const node = this.nodeMap.get(nodeId);
		/* v8 ignore next -- @preserve */
		return node ? [node] : [];
	}
}
