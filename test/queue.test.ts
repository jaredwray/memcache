import { describe, expect, it } from "vitest";
import { COMPACT_THRESHOLD, Queue } from "../src/queue.js";

/** The array behind the queue, to check what it still holds. */
const backing = (queue: Queue<unknown>) =>
	(queue as unknown as { _items: unknown[] })._items;

const range = (start: number, count: number) =>
	Array.from({ length: count }, (_, i) => start + i);

describe("Queue", () => {
	it("should return items in the order they were added", () => {
		const queue = new Queue<number>();
		queue.push(1);
		queue.push(2);
		queue.push(3);

		expect(queue.length).toBe(3);
		expect(queue.peek()).toBe(1);
		expect(queue.shift()).toBe(1);
		expect(queue.shift()).toBe(2);
		expect(queue.length).toBe(1);
		expect(queue.shift()).toBe(3);
		expect(queue.length).toBe(0);
	});

	it("should return undefined when empty", () => {
		const queue = new Queue<number>();
		expect(queue.peek()).toBeUndefined();
		expect(queue.shift()).toBeUndefined();

		queue.push(1);
		queue.shift();
		expect(queue.peek()).toBeUndefined();
		expect(queue.shift()).toBeUndefined();
		expect(queue.length).toBe(0);
	});

	it("should keep the order while it is compacted and grows", () => {
		const queue = new Queue<number>();
		let added = 0;
		let taken = 0;
		// Three in for every two out: the queue grows while its consumed
		// part is dropped many times
		for (let round = 0; round < 5 * COMPACT_THRESHOLD; round++) {
			queue.push(added++);
			queue.push(added++);
			queue.push(added++);
			expect(queue.shift()).toBe(taken++);
			expect(queue.shift()).toBe(taken++);
		}

		expect(queue.length).toBe(added - taken);
		expect(queue.toArray()).toEqual(range(taken, added - taken));
		while (queue.length > 0) {
			expect(queue.shift()).toBe(taken++);
		}
		expect(taken).toBe(added);
	});

	it("should drop taken items from a queue that never empties", () => {
		const queue = new Queue<number>();
		for (let i = 0; i < 100; i++) {
			queue.push(i);
		}
		for (let i = 100; i < 100_000; i++) {
			queue.push(i);
			queue.shift();
		}

		expect(queue.length).toBe(100);
		expect(queue.peek()).toBe(99_900);
		// Without compaction the array would hold all 100,000 items
		expect(backing(queue).length).toBeLessThanOrEqual(COMPACT_THRESHOLD + 100);
	});

	it("should release an item as soon as it is taken", () => {
		const queue = new Queue<object>();
		queue.push({});
		queue.push({});
		queue.shift();

		expect(backing(queue)[0]).toBeUndefined();
	});

	it("should drain every item in order and stay usable", () => {
		const queue = new Queue<number>();
		for (let i = 0; i < 3 * COMPACT_THRESHOLD; i++) {
			queue.push(i);
		}
		for (let i = 0; i < 2 * COMPACT_THRESHOLD; i++) {
			queue.shift();
		}

		expect(queue.drain()).toEqual(
			range(2 * COMPACT_THRESHOLD, COMPACT_THRESHOLD),
		);
		expect(queue.length).toBe(0);
		expect(queue.shift()).toBeUndefined();

		queue.push(7);
		expect(queue.toArray()).toEqual([7]);
	});

	it("should return a copy from toArray", () => {
		const queue = new Queue<number>();
		queue.push(1);
		queue.push(2);

		const items = queue.toArray();
		items.length = 0;
		items.push(9);

		expect(queue.toArray()).toEqual([1, 2]);
		expect(queue.length).toBe(2);
	});
});
