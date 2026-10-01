/**
 * The consumed part of the array is only copied away once it has at least
 * this many items, so a queue that stays short never copies.
 */
export const COMPACT_THRESHOLD = 1024;

/**
 * A first-in, first-out queue with amortized O(1) `push` and `shift`.
 *
 * `Array.prototype.shift()` copies the whole array once it holds more than a
 * few thousand items, so taking the responses of a burst of pipelined
 * commands off an array was quadratic: 60,000 concurrent gets spent seconds
 * in `shift()`. This queue reads from a head index instead, and drops the
 * consumed part once it is at least half of the array.
 */
export class Queue<T> {
	private _items: Array<T | undefined> = [];
	private _head: number = 0;

	/** The number of items in the queue. */
	public get length(): number {
		return this._items.length - this._head;
	}

	/** Adds an item to the end of the queue. */
	public push(item: T): void {
		this._items.push(item);
	}

	/** The first item, without removing it. */
	public peek(): T | undefined {
		return this._items[this._head];
	}

	/** Removes and returns the first item. */
	public shift(): T | undefined {
		if (this._head === this._items.length) {
			return undefined;
		}

		const item = this._items[this._head];
		// Let the item be garbage collected before the array is compacted
		this._items[this._head] = undefined;
		this._head++;
		if (this._head === this._items.length) {
			this._items.length = 0;
			this._head = 0;
		} else if (
			this._head >= COMPACT_THRESHOLD &&
			this._head * 2 >= this._items.length
		) {
			this._items = this._items.slice(this._head);
			this._head = 0;
		}

		return item;
	}

	/** Removes and returns every item, first to last. */
	public drain(): T[] {
		const items = this.toArray();
		this._items = [];
		this._head = 0;
		return items;
	}

	/** A copy of the items, first to last. */
	public toArray(): T[] {
		return this._items.slice(this._head) as T[];
	}
}
