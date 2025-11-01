import type { MemcacheNode } from "node";

export enum DistributionStrategy {
	Ketama = "ketama",
	Custom = "custom",
}

export type DistributionHash = (
	key: string | Buffer,
	nodes: ReadonlyArray<MemcacheNode>,
) => number;

export class Distribution {
	private _strategy = DistributionStrategy.Ketama;
	private _nonBlocking = false;
	private _cache: Map<string, number> = new Map();
	private _lruSize = 10_000;
}
