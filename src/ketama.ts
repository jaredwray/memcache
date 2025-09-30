/**
 * Orginal Work is from https://github.com/connor4312/ketama
 * Maintained in project for bug fixes and also configuration
 * Thanks connor4312!
 */
import { createHash } from "node:crypto";

/**
 * Function that returns an int32 hash of the input (a number between
 * -2147483648 and 2147483647). If your hashing library gives you a Buffer
 * back, a convenient way to get this is `buf.readInt32BE()`.
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
		createHash(algorithm).update(value).digest().readInt32BE();

/**
 * Extracts the key from a node, whether it's a string or an object with a key property.
 * @param node - The node to extract the key from
 * @returns The key as a string
 */
const keyFor = (node: string | { key: string }) =>
	typeof node === "string" ? node : node.key;

/**
 * Represents the hash clock, which is an array of [hash, node key] tuples sorted by hash value.
 * This forms the consistent hashing ring where each tuple represents a virtual node position.
 */
type HashClock = [hash: number, node: string][];

/**
 * A consistent hashing implementation using the Ketama algorithm.
 * This provides a way to distribute keys across nodes in a way that minimizes
 * redistribution when nodes are added or removed.
 *
 * @template TNode - The type of nodes in the ring (string or object with key property)
 *
 * @example
 * ```typescript
 * // Create a ring with string nodes
 * const ring = new HashRing(['server1', 'server2', 'server3']);
 * const node = ring.getNode('my-key'); // Returns the node responsible for 'my-key'
 *
 * // Create a ring with weighted nodes
 * const weightedRing = new HashRing([
 *   { node: 'server1', weight: 2 },
 *   { node: 'server2', weight: 1 }
 * ]);
 *
 * // Create a ring with object nodes
 * const objRing = new HashRing([
 *   { key: 'server1', host: 'localhost', port: 11211 },
 *   { key: 'server2', host: 'localhost', port: 11212 }
 * ]);
 * ```
 */
export class HashRing<TNode extends string | { key: string } = string> {
	/**
	 * Base weight of each node in the hash ring. Having a base weight of 1 is
	 * not very desirable, since then, due to the ketama-style "clock", it's
	 * possible to end up with a clock that's very skewed when dealing with a
	 * small number of nodes. Setting to 50 nodes seems to give a better
	 * distribution, so that load is spread roughly evenly to +/- 5%.
	 */
	public static baseWeight = 50;

	/** The hash function used to compute node positions on the ring */
	private readonly hashFn: HashFunction;

	/** The sorted array of [hash, node key] tuples representing virtual nodes on the ring */
	private clock: HashClock = [];

	/** Map of node keys to actual node objects */
	private nodes = new Map<string, TNode>();

	/**
	 * Creates a new HashRing instance.
	 *
	 * @param initialNodes - Array of nodes to add to the ring, optionally with weights
	 * @param hashFn - Hash function to use (string algorithm name or custom function, defaults to "sha1")
	 *
	 * @example
	 * ```typescript
	 * // Simple ring with default SHA-1 hashing
	 * const ring = new HashRing(['node1', 'node2']);
	 *
	 * // Ring with custom hash function
	 * const customRing = new HashRing(['node1', 'node2'], 'md5');
	 *
	 * // Ring with weighted nodes
	 * const weightedRing = new HashRing([
	 *   { node: 'heavy-server', weight: 3 },
	 *   { node: 'light-server', weight: 1 }
	 * ]);
	 * ```
	 */
	constructor(
		initialNodes: ReadonlyArray<TNode | { weight: number; node: TNode }> = [],
		hashFn: string | HashFunction = "sha1",
	) {
		this.hashFn =
			typeof hashFn === "string" ? hashFunctionForBuiltin(hashFn) : hashFn;
		for (const node of initialNodes) {
			if (typeof node === "object" && "weight" in node && "node" in node) {
				this.addNode(node.node, node.weight);
			} else {
				this.addNode(node);
			}
		}
	}

	/**
	 * Add a new node to the ring. If the node already exists in the ring, it
	 * will be updated. For example, you can use this to update the node's weight.
	 *
	 * @param node - The node to add to the ring
	 * @param weight - The relative weight of this node (default: 1). Higher weights mean more keys will be assigned to this node. A weight of 0 removes the node.
	 * @throws {RangeError} If weight is negative
	 *
	 * @example
	 * ```typescript
	 * const ring = new HashRing();
	 * ring.addNode('server1'); // Add with default weight of 1
	 * ring.addNode('server2', 2); // Add with weight of 2 (will handle ~2x more keys)
	 * ring.addNode('server1', 3); // Update server1's weight to 3
	 * ring.addNode('server2', 0); // Remove server2
	 * ```
	 */
	public addNode(node: TNode, weight = 1) {
		if (weight === 0) {
			this.removeNode(node);
		} else if (weight < 0) {
			throw new RangeError("Cannot add a node to the hashring with weight < 0");
		} else {
			this.removeNode(node);
			const key = keyFor(node);
			this.nodes.set(key, node);
			this.addNodeToClock(key, Math.round(weight * HashRing.baseWeight));
		}
	}

	/**
	 * Removes the node from the ring. No-op if the node does not exist.
	 *
	 * @param node - The node to remove from the ring
	 *
	 * @example
	 * ```typescript
	 * const ring = new HashRing(['server1', 'server2']);
	 * ring.removeNode('server1'); // Removes server1 from the ring
	 * ring.removeNode('nonexistent'); // Safe to call with non-existent node
	 * ```
	 */
	public removeNode(node: TNode) {
		const key = keyFor(node);
		if (this.nodes.delete(key)) {
			this.clock = this.clock.filter(([, n]) => n !== key);
		}
	}

	/**
	 * Gets the node which should handle the given input key. Returns undefined if
	 * the hashring has no nodes.
	 *
	 * Uses consistent hashing to ensure the same input always maps to the same node,
	 * and minimizes redistribution when nodes are added or removed.
	 *
	 * @param input - The key to find the responsible node for (string or Buffer)
	 * @returns The node responsible for this key, or undefined if ring is empty
	 *
	 * @example
	 * ```typescript
	 * const ring = new HashRing(['server1', 'server2', 'server3']);
	 * const node = ring.getNode('user:123'); // Returns e.g., 'server2'
	 * const sameNode = ring.getNode('user:123'); // Always returns 'server2'
	 *
	 * // Also accepts Buffer input
	 * const bufferNode = ring.getNode(Buffer.from('user:123'));
	 * ```
	 */
	public getNode(input: string | Buffer): TNode | undefined {
		if (this.clock.length === 0) {
			return undefined;
		}

		const index = this.getIndexForInput(input);
		const key =
			index === this.clock.length ? this.clock[0][1] : this.clock[index][1];

		return this.nodes.get(key);
	}

	/**
	 * Finds the index in the clock for the given input by hashing it and performing binary search.
	 *
	 * @param input - The input to find the clock position for
	 * @returns The index in the clock array
	 */
	private getIndexForInput(input: string | Buffer) {
		const hash = this.hashFn(
			typeof input === "string" ? Buffer.from(input) : input,
		);
		return binarySearchRing(this.clock, hash);
	}

	/**
	 * Gets multiple replica nodes that should handle the given input. Useful for
	 * implementing replication strategies where you want to store data on multiple nodes.
	 *
	 * The returned array will contain unique nodes in the order they appear on the ring
	 * starting from the primary node. If there are fewer nodes than replicas requested,
	 * all nodes are returned.
	 *
	 * @param input - The key to find replica nodes for
	 * @param replicas - The number of replica nodes to return
	 * @returns Array of nodes that should handle this key (length ≤ replicas)
	 *
	 * @example
	 * ```typescript
	 * const ring = new HashRing(['server1', 'server2', 'server3', 'server4']);
	 *
	 * // Get 3 replicas for a key
	 * const replicas = ring.getNodes('user:123', 3);
	 * // Returns e.g., ['server2', 'server4', 'server1']
	 *
	 * // If requesting more replicas than nodes, returns all nodes
	 * const allNodes = ring.getNodes('user:123', 10);
	 * // Returns ['server1', 'server2', 'server3', 'server4']
	 * ```
	 */
	public getNodes(input: string, replicas: number): TNode[] {
		if (replicas >= this.nodes.size) {
			return [...this.nodes.values()];
		}

		const chosen = new Set<string>();

		// We know this will terminate, since we know there are at least as many
		// unique nodes to be chosen as there are replicas
		for (let i = this.getIndexForInput(input); chosen.size < replicas; i++) {
			chosen.add(this.clock[i % this.clock.length][1]);
		}

		return [...chosen].map((c) => this.nodes.get(c) as TNode);
	}

	/**
	 * Adds virtual nodes to the clock for the given node key.
	 * Creates multiple positions on the ring for better distribution.
	 *
	 * @param key - The node key to add to the clock
	 * @param weight - The number of virtual nodes to create (weight * baseWeight)
	 */
	private addNodeToClock(key: string, weight: number) {
		for (let i = weight; i > 0; i--) {
			const hash = this.hashFn(Buffer.from(`${key}\0${i}`));
			this.clock.push([hash, key]);
		}

		this.clock.sort((a, b) => a[0] - b[0]);
	}
}

/**
 * Performs binary search on the hash ring to find the appropriate position for a given hash.
 * Returns the index of the first virtual node with a hash value >= the input hash.
 * If no such node exists, returns the length of the ring (wraps to beginning).
 *
 * @param ring - The sorted array of [hash, node] tuples
 * @param hash - The hash value to search for
 * @returns The index where the hash should be inserted or the next node position
 */
function binarySearchRing(ring: HashClock, hash: number) {
	let mid: number;
	let lo = 0;
	let hi = ring.length - 1;

	while (lo <= hi) {
		mid = Math.floor((lo + hi) / 2);

		if (ring[mid][0] >= hash) {
			hi = mid - 1;
		} else {
			lo = mid + 1;
		}
	}

	return lo;
}
