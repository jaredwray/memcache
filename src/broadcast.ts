import type { HashProvider } from "./index.js";
import type { MemcacheNode } from "./node.js";

/**
 * A distribution hash implementation that sends every key to all nodes.
 * Unlike KetamaHash or ModulaHash, this does not partition keys — every
 * operation targets every node in the cluster.
 *
 * This is useful for replication scenarios where all nodes should hold
 * the same data, or for broadcast operations like flush/delete.
 *
 * @example
 * ```typescript
 * const client = new Memcache({
 *   nodes: ['server1:11211', 'server2:11211'],
 *   hash: new BroadcastHash(),
 * });
 * // Every set/get/delete will hit all nodes
 * ```
 */
export class BroadcastHash implements HashProvider {
	/** The name of this distribution strategy */
	public readonly name = "broadcast";

	/** Map of node IDs to MemcacheNode instances */
	private nodeMap: Map<string, MemcacheNode>;

	/** Cached array of nodes, rebuilt only on add/remove */
	private nodeCache: Array<MemcacheNode>;

	constructor() {
		this.nodeMap = new Map();
		this.nodeCache = [];
	}

	/**
	 * Gets all nodes in the distribution.
	 * @returns Array of all MemcacheNode instances
	 */
	public get nodes(): Array<MemcacheNode> {
		return [...this.nodeCache];
	}

	/**
	 * Adds a node to the distribution.
	 * @param node - The MemcacheNode to add
	 */
	public addNode(node: MemcacheNode): void {
		this.nodeMap.set(node.id, node);
		this.rebuildCache();
	}

	/**
	 * Removes a node from the distribution by its ID.
	 * @param id - The node ID (e.g., "localhost:11211")
	 */
	public removeNode(id: string): void {
		this.nodeMap.delete(id);
		this.rebuildCache();
	}

	/**
	 * Gets a specific node by its ID.
	 * @param id - The node ID (e.g., "localhost:11211")
	 * @returns The MemcacheNode if found, undefined otherwise
	 */
	public getNode(id: string): MemcacheNode | undefined {
		return this.nodeMap.get(id);
	}

	/**
	 * Returns all nodes regardless of key. Every operation is broadcast
	 * to every node in the cluster.
	 * @param _key - The cache key (ignored — all nodes are always returned)
	 * @returns Array of all MemcacheNode instances
	 */
	public getNodesByKey(_key: string): Array<MemcacheNode> {
		return [...this.nodeCache];
	}

	/**
	 * Rebuilds the cached node array from the map.
	 */
	private rebuildCache(): void {
		this.nodeCache = [...this.nodeMap.values()];
	}
}
