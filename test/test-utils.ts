import { faker } from "@faker-js/faker";

/**
 * Generate a unique key for testing
 * @param prefix Optional prefix for the key
 * @returns A unique alphanumeric key safe for memcache
 */
export function generateKey(prefix?: string): string {
	const base = faker.string.alphanumeric(16);
	return prefix ? `${prefix}-${base}` : base;
}

/**
 * Generate a unique value for testing
 * @returns A random string value
 */
export function generateValue(): string {
	return faker.lorem.word();
}

/**
 * Generate a unique numeric string value for testing counters
 * @param min Minimum value (default: 1)
 * @param max Maximum value (default: 100)
 * @returns A numeric string
 */
export function generateNumericValue(min = 1, max = 100): string {
	return faker.number.int({ min, max }).toString();
}

/**
 * Generate a large value for testing payload sizes
 * @param size Size in characters
 * @returns A string of the specified size
 */
export function generateLargeValue(size: number): string {
	return faker.string.alphanumeric(size);
}

export { faker };
