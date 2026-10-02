/**
 * Binary protocol constants and utilities for SASL authentication.
 * Memcached binary protocol is used for SASL handshake, after which
 * the connection switches to the text protocol for commands.
 */

// Magic bytes
export const REQUEST_MAGIC = 0x80;
export const RESPONSE_MAGIC = 0x81;

// SASL opcodes
export const OPCODE_SASL_LIST_MECHS = 0x20;
export const OPCODE_SASL_AUTH = 0x21;
export const OPCODE_SASL_STEP = 0x22;

// Command opcodes
export const OPCODE_GET = 0x00;
export const OPCODE_SET = 0x01;
export const OPCODE_ADD = 0x02;
export const OPCODE_REPLACE = 0x03;
export const OPCODE_DELETE = 0x04;
export const OPCODE_INCREMENT = 0x05;
export const OPCODE_DECREMENT = 0x06;
export const OPCODE_QUIT = 0x07;
export const OPCODE_FLUSH = 0x08;
export const OPCODE_NOOP = 0x0a;
export const OPCODE_VERSION = 0x0b;
export const OPCODE_APPEND = 0x0e;
export const OPCODE_PREPEND = 0x0f;
export const OPCODE_STAT = 0x10;
export const OPCODE_TOUCH = 0x1c;

// Status codes
export const STATUS_SUCCESS = 0x0000;
export const STATUS_KEY_NOT_FOUND = 0x0001;
export const STATUS_KEY_EXISTS = 0x0002;
export const STATUS_VALUE_TOO_LARGE = 0x0003;
export const STATUS_INVALID_ARGUMENTS = 0x0004;
export const STATUS_ITEM_NOT_STORED = 0x0005;
export const STATUS_AUTH_ERROR = 0x0020;
export const STATUS_AUTH_CONTINUE = 0x0021;

// Header size in bytes
export const HEADER_SIZE = 24;

export interface BinaryHeader {
	magic: number;
	opcode: number;
	keyLength: number;
	extrasLength: number;
	dataType: number;
	status: number;
	totalBodyLength: number;
	opaque: number;
	cas: Buffer;
}

/**
 * Serialize a binary protocol header to a Buffer
 * @param header - Partial header object with values to set
 * @returns A 24-byte Buffer containing the binary header
 */
export function serializeHeader(header: Partial<BinaryHeader>): Buffer {
	const buf = Buffer.alloc(HEADER_SIZE);
	buf.writeUInt8(header.magic ?? REQUEST_MAGIC, 0);
	buf.writeUInt8(header.opcode ?? 0, 1);
	buf.writeUInt16BE(header.keyLength ?? 0, 2);
	buf.writeUInt8(header.extrasLength ?? 0, 4);
	buf.writeUInt8(header.dataType ?? 0, 5);
	buf.writeUInt16BE(header.status ?? 0, 6);
	buf.writeUInt32BE(header.totalBodyLength ?? 0, 8);
	buf.writeUInt32BE(header.opaque ?? 0, 12);
	if (header.cas) {
		header.cas.copy(buf, 16);
	}
	return buf;
}

/**
 * Deserialize a binary protocol header from a Buffer
 * @param buf - Buffer containing at least 24 bytes of header data
 * @returns Parsed BinaryHeader object
 */
export function deserializeHeader(buf: Buffer): BinaryHeader {
	return {
		magic: buf.readUInt8(0),
		opcode: buf.readUInt8(1),
		keyLength: buf.readUInt16BE(2),
		extrasLength: buf.readUInt8(4),
		dataType: buf.readUInt8(5),
		status: buf.readUInt16BE(6),
		totalBodyLength: buf.readUInt32BE(8),
		opaque: buf.readUInt32BE(12),
		cas: buf.subarray(16, 24),
	};
}

/**
 * A request packet with room for a body of `bodyLength` bytes after the
 * header, in one allocation. `Buffer.allocUnsafe()` can hand out old bytes,
 * so every header byte is written: the data type and vbucket are 0, so is
 * the opaque (the node sets it when it queues the request), and so is the
 * CAS, since no request here sends one. The caller writes the whole body.
 */
function requestPacket(
	opcode: number,
	keyLength: number,
	extrasLength: number,
	bodyLength: number,
): Buffer {
	const packet = Buffer.allocUnsafe(HEADER_SIZE + bodyLength);
	packet[0] = REQUEST_MAGIC;
	packet[1] = opcode;
	packet.writeUInt16BE(keyLength, 2);
	packet[4] = extrasLength;
	packet[5] = 0;
	packet.writeUInt16BE(0, 6);
	packet.writeUInt32BE(bodyLength, 8);
	packet.writeUInt32BE(0, 12);
	packet.writeUInt32BE(0, 16);
	packet.writeUInt32BE(0, 20);
	return packet;
}

function byteLength(value: string | Buffer): number {
	return typeof value === "string" ? Buffer.byteLength(value) : value.length;
}

function writeValue(packet: Buffer, value: string | Buffer, offset: number) {
	if (typeof value === "string") {
		packet.write(value, offset);
	} else {
		value.copy(packet, offset);
	}
}

/** A request whose body is only the key (GET, DELETE, STAT). */
function keyRequest(opcode: number, key: string): Buffer {
	const keyLength = Buffer.byteLength(key);
	const packet = requestPacket(opcode, keyLength, 0, keyLength);
	packet.write(key, HEADER_SIZE);
	return packet;
}

/** A request whose body is the key and then a value, without extras. */
function keyValueRequest(
	opcode: number,
	key: string,
	value: string | Buffer,
): Buffer {
	const keyLength = Buffer.byteLength(key);
	const packet = requestPacket(
		opcode,
		keyLength,
		0,
		keyLength + byteLength(value),
	);
	packet.write(key, HEADER_SIZE);
	writeValue(packet, value, HEADER_SIZE + keyLength);
	return packet;
}

/** SET, ADD or REPLACE: flags and expiration as extras, then key and value. */
function storageRequest(
	opcode: number,
	key: string,
	value: string | Buffer,
	flags: number,
	exptime: number,
): Buffer {
	const keyLength = Buffer.byteLength(key);
	const packet = requestPacket(
		opcode,
		keyLength,
		8,
		8 + keyLength + byteLength(value),
	);
	packet.writeUInt32BE(flags, HEADER_SIZE);
	packet.writeUInt32BE(exptime, HEADER_SIZE + 4);
	packet.write(key, HEADER_SIZE + 8);
	writeValue(packet, value, HEADER_SIZE + 8 + keyLength);
	return packet;
}

/**
 * INCREMENT or DECREMENT: the 64-bit delta and initial value and the
 * expiration as extras, then the key.
 */
function counterRequest(
	opcode: number,
	key: string,
	delta: number,
	initial: number,
	exptime: number,
): Buffer {
	const keyLength = Buffer.byteLength(key);
	const packet = requestPacket(opcode, keyLength, 20, 20 + keyLength);
	// Each 64-bit big-endian number as two 32-bit writes
	packet.writeUInt32BE(Math.floor(delta / 0x100000000), HEADER_SIZE);
	packet.writeUInt32BE(delta >>> 0, HEADER_SIZE + 4);
	packet.writeUInt32BE(Math.floor(initial / 0x100000000), HEADER_SIZE + 8);
	packet.writeUInt32BE(initial >>> 0, HEADER_SIZE + 12);
	packet.writeUInt32BE(exptime, HEADER_SIZE + 16);
	packet.write(key, HEADER_SIZE + 20);
	return packet;
}

/**
 * Build a SASL PLAIN authentication request packet.
 * SASL PLAIN format: \0username\0password
 * @param username - The username for authentication
 * @param password - The password for authentication
 * @returns Buffer containing the complete binary request packet
 */
export function buildSaslPlainRequest(
	username: string,
	password: string,
): Buffer {
	return keyValueRequest(
		OPCODE_SASL_AUTH,
		"PLAIN",
		`\x00${username}\x00${password}`,
	);
}

/**
 * Build a SASL list mechanisms request packet.
 * This can be used to query the server for supported SASL mechanisms.
 * @returns Buffer containing the complete binary request packet
 */
export function buildSaslListMechsRequest(): Buffer {
	return requestPacket(OPCODE_SASL_LIST_MECHS, 0, 0, 0);
}

/**
 * Build a GET request packet
 * @param key - The key to retrieve
 * @returns Buffer containing the complete binary request packet
 */
export function buildGetRequest(key: string): Buffer {
	return keyRequest(OPCODE_GET, key);
}

/**
 * Build a SET request packet
 * @param key - The key to set
 * @param value - The value to store
 * @param flags - Optional flags (default: 0)
 * @param exptime - Expiration time in seconds (default: 0)
 * @returns Buffer containing the complete binary request packet
 */
export function buildSetRequest(
	key: string,
	value: string | Buffer,
	flags = 0,
	exptime = 0,
): Buffer {
	return storageRequest(OPCODE_SET, key, value, flags, exptime);
}

/**
 * Build an ADD request packet (only stores if key doesn't exist)
 */
export function buildAddRequest(
	key: string,
	value: string | Buffer,
	flags = 0,
	exptime = 0,
): Buffer {
	return storageRequest(OPCODE_ADD, key, value, flags, exptime);
}

/**
 * Build a REPLACE request packet (only stores if key exists)
 */
export function buildReplaceRequest(
	key: string,
	value: string | Buffer,
	flags = 0,
	exptime = 0,
): Buffer {
	return storageRequest(OPCODE_REPLACE, key, value, flags, exptime);
}

/**
 * Build a DELETE request packet
 * @param key - The key to delete
 * @returns Buffer containing the complete binary request packet
 */
export function buildDeleteRequest(key: string): Buffer {
	return keyRequest(OPCODE_DELETE, key);
}

/**
 * Build an INCREMENT request packet
 * @param key - The key to increment
 * @param delta - Amount to increment by
 * @param initial - Initial value if key doesn't exist
 * @param exptime - Expiration time
 * @returns Buffer containing the complete binary request packet
 */
export function buildIncrementRequest(
	key: string,
	delta = 1,
	initial = 0,
	exptime = 0,
): Buffer {
	return counterRequest(OPCODE_INCREMENT, key, delta, initial, exptime);
}

/**
 * Build a DECREMENT request packet
 * @param key - The key to decrement
 * @param delta - Amount to decrement by
 * @param initial - Initial value if key doesn't exist
 * @param exptime - Expiration time
 * @returns Buffer containing the complete binary request packet
 */
export function buildDecrementRequest(
	key: string,
	delta = 1,
	initial = 0,
	exptime = 0,
): Buffer {
	return counterRequest(OPCODE_DECREMENT, key, delta, initial, exptime);
}

/**
 * Build an APPEND request packet
 */
export function buildAppendRequest(
	key: string,
	value: string | Buffer,
): Buffer {
	return keyValueRequest(OPCODE_APPEND, key, value);
}

/**
 * Build a PREPEND request packet
 */
export function buildPrependRequest(
	key: string,
	value: string | Buffer,
): Buffer {
	return keyValueRequest(OPCODE_PREPEND, key, value);
}

/**
 * Build a TOUCH request packet
 */
export function buildTouchRequest(key: string, exptime: number): Buffer {
	const keyLength = Buffer.byteLength(key);
	const packet = requestPacket(OPCODE_TOUCH, keyLength, 4, 4 + keyLength);
	packet.writeUInt32BE(exptime, HEADER_SIZE);
	packet.write(key, HEADER_SIZE + 4);
	return packet;
}

/**
 * Build a FLUSH request packet
 */
export function buildFlushRequest(exptime = 0): Buffer {
	const packet = requestPacket(OPCODE_FLUSH, 0, 4, 4);
	packet.writeUInt32BE(exptime, HEADER_SIZE);
	return packet;
}

/**
 * Build a VERSION request packet
 */
export function buildVersionRequest(): Buffer {
	return requestPacket(OPCODE_VERSION, 0, 0, 0);
}

/**
 * Build a STAT request packet
 */
export function buildStatRequest(key?: string): Buffer {
	if (key) {
		return keyRequest(OPCODE_STAT, key);
	}
	return requestPacket(OPCODE_STAT, 0, 0, 0);
}

/**
 * Build a QUIT request packet
 */
export function buildQuitRequest(): Buffer {
	return requestPacket(OPCODE_QUIT, 0, 0, 0);
}

/**
 * The status of a response packet, read without parsing the rest of the
 * header into an object.
 */
export function readStatus(packet: Buffer): number {
	return packet.readUInt16BE(6);
}

/**
 * Parse a GET response: its status, and the value after the extras and the
 * key. An empty value reads as no value.
 */
export function parseGetResponse(buf: Buffer): {
	status: number;
	value: string | undefined;
} {
	const status = readStatus(buf);
	if (status !== STATUS_SUCCESS) {
		return { status, value: undefined };
	}

	const valueStart = HEADER_SIZE + buf[4] + buf.readUInt16BE(2);
	const valueEnd = HEADER_SIZE + buf.readUInt32BE(8);
	const value =
		valueEnd > valueStart
			? buf.toString("utf8", valueStart, valueEnd)
			: undefined;
	return { status, value };
}

/**
 * Parse an increment/decrement response
 */
export function parseIncrDecrResponse(buf: Buffer): {
	status: number;
	value: number | undefined;
} {
	const status = readStatus(buf);
	if (status !== STATUS_SUCCESS || buf.readUInt32BE(8) < 8) {
		return { status, value: undefined };
	}

	// Value is 8-byte big-endian unsigned integer
	const high = buf.readUInt32BE(HEADER_SIZE);
	const low = buf.readUInt32BE(HEADER_SIZE + 4);
	return { status, value: high * 0x100000000 + low };
}
