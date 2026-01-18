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
	const mechanism = "PLAIN";
	const authData = `\x00${username}\x00${password}`;

	const keyBuf = Buffer.from(mechanism, "utf8");
	const valueBuf = Buffer.from(authData, "utf8");

	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_SASL_AUTH,
		keyLength: keyBuf.length,
		totalBodyLength: keyBuf.length + valueBuf.length,
	});

	return Buffer.concat([header, keyBuf, valueBuf]);
}

/**
 * Build a SASL list mechanisms request packet.
 * This can be used to query the server for supported SASL mechanisms.
 * @returns Buffer containing the complete binary request packet
 */
export function buildSaslListMechsRequest(): Buffer {
	return serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_SASL_LIST_MECHS,
	});
}

/**
 * Build a GET request packet
 * @param key - The key to retrieve
 * @returns Buffer containing the complete binary request packet
 */
export function buildGetRequest(key: string): Buffer {
	const keyBuf = Buffer.from(key, "utf8");
	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_GET,
		keyLength: keyBuf.length,
		totalBodyLength: keyBuf.length,
	});
	return Buffer.concat([header, keyBuf]);
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
	const keyBuf = Buffer.from(key, "utf8");
	const valueBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, "utf8");

	// Extras: 4 bytes flags + 4 bytes expiration
	const extras = Buffer.alloc(8);
	extras.writeUInt32BE(flags, 0);
	extras.writeUInt32BE(exptime, 4);

	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_SET,
		keyLength: keyBuf.length,
		extrasLength: 8,
		totalBodyLength: 8 + keyBuf.length + valueBuf.length,
	});

	return Buffer.concat([header, extras, keyBuf, valueBuf]);
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
	const keyBuf = Buffer.from(key, "utf8");
	const valueBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, "utf8");

	const extras = Buffer.alloc(8);
	extras.writeUInt32BE(flags, 0);
	extras.writeUInt32BE(exptime, 4);

	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_ADD,
		keyLength: keyBuf.length,
		extrasLength: 8,
		totalBodyLength: 8 + keyBuf.length + valueBuf.length,
	});

	return Buffer.concat([header, extras, keyBuf, valueBuf]);
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
	const keyBuf = Buffer.from(key, "utf8");
	const valueBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, "utf8");

	const extras = Buffer.alloc(8);
	extras.writeUInt32BE(flags, 0);
	extras.writeUInt32BE(exptime, 4);

	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_REPLACE,
		keyLength: keyBuf.length,
		extrasLength: 8,
		totalBodyLength: 8 + keyBuf.length + valueBuf.length,
	});

	return Buffer.concat([header, extras, keyBuf, valueBuf]);
}

/**
 * Build a DELETE request packet
 * @param key - The key to delete
 * @returns Buffer containing the complete binary request packet
 */
export function buildDeleteRequest(key: string): Buffer {
	const keyBuf = Buffer.from(key, "utf8");
	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_DELETE,
		keyLength: keyBuf.length,
		totalBodyLength: keyBuf.length,
	});
	return Buffer.concat([header, keyBuf]);
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
	const keyBuf = Buffer.from(key, "utf8");

	// Extras: 8 bytes delta + 8 bytes initial + 4 bytes expiration
	const extras = Buffer.alloc(20);
	// Write delta as 64-bit big-endian (split into two 32-bit writes)
	extras.writeUInt32BE(Math.floor(delta / 0x100000000), 0);
	extras.writeUInt32BE(delta >>> 0, 4);
	// Write initial as 64-bit big-endian
	extras.writeUInt32BE(Math.floor(initial / 0x100000000), 8);
	extras.writeUInt32BE(initial >>> 0, 12);
	// Write expiration
	extras.writeUInt32BE(exptime, 16);

	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_INCREMENT,
		keyLength: keyBuf.length,
		extrasLength: 20,
		totalBodyLength: 20 + keyBuf.length,
	});

	return Buffer.concat([header, extras, keyBuf]);
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
	const keyBuf = Buffer.from(key, "utf8");

	const extras = Buffer.alloc(20);
	extras.writeUInt32BE(Math.floor(delta / 0x100000000), 0);
	extras.writeUInt32BE(delta >>> 0, 4);
	extras.writeUInt32BE(Math.floor(initial / 0x100000000), 8);
	extras.writeUInt32BE(initial >>> 0, 12);
	extras.writeUInt32BE(exptime, 16);

	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_DECREMENT,
		keyLength: keyBuf.length,
		extrasLength: 20,
		totalBodyLength: 20 + keyBuf.length,
	});

	return Buffer.concat([header, extras, keyBuf]);
}

/**
 * Build an APPEND request packet
 */
export function buildAppendRequest(
	key: string,
	value: string | Buffer,
): Buffer {
	const keyBuf = Buffer.from(key, "utf8");
	const valueBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, "utf8");

	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_APPEND,
		keyLength: keyBuf.length,
		totalBodyLength: keyBuf.length + valueBuf.length,
	});

	return Buffer.concat([header, keyBuf, valueBuf]);
}

/**
 * Build a PREPEND request packet
 */
export function buildPrependRequest(
	key: string,
	value: string | Buffer,
): Buffer {
	const keyBuf = Buffer.from(key, "utf8");
	const valueBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, "utf8");

	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_PREPEND,
		keyLength: keyBuf.length,
		totalBodyLength: keyBuf.length + valueBuf.length,
	});

	return Buffer.concat([header, keyBuf, valueBuf]);
}

/**
 * Build a TOUCH request packet
 */
export function buildTouchRequest(key: string, exptime: number): Buffer {
	const keyBuf = Buffer.from(key, "utf8");

	const extras = Buffer.alloc(4);
	extras.writeUInt32BE(exptime, 0);

	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_TOUCH,
		keyLength: keyBuf.length,
		extrasLength: 4,
		totalBodyLength: 4 + keyBuf.length,
	});

	return Buffer.concat([header, extras, keyBuf]);
}

/**
 * Build a FLUSH request packet
 */
export function buildFlushRequest(exptime = 0): Buffer {
	const extras = Buffer.alloc(4);
	extras.writeUInt32BE(exptime, 0);

	const header = serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_FLUSH,
		extrasLength: 4,
		totalBodyLength: 4,
	});

	return Buffer.concat([header, extras]);
}

/**
 * Build a VERSION request packet
 */
export function buildVersionRequest(): Buffer {
	return serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_VERSION,
	});
}

/**
 * Build a STAT request packet
 */
export function buildStatRequest(key?: string): Buffer {
	if (key) {
		const keyBuf = Buffer.from(key, "utf8");
		const header = serializeHeader({
			magic: REQUEST_MAGIC,
			opcode: OPCODE_STAT,
			keyLength: keyBuf.length,
			totalBodyLength: keyBuf.length,
		});
		return Buffer.concat([header, keyBuf]);
	}
	return serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_STAT,
	});
}

/**
 * Build a QUIT request packet
 */
export function buildQuitRequest(): Buffer {
	return serializeHeader({
		magic: REQUEST_MAGIC,
		opcode: OPCODE_QUIT,
	});
}

/**
 * Parse a binary response and extract the value
 */
export function parseGetResponse(buf: Buffer): {
	header: BinaryHeader;
	value: Buffer | undefined;
	key: string | undefined;
} {
	const header = deserializeHeader(buf);
	if (header.status !== STATUS_SUCCESS) {
		return { header, value: undefined, key: undefined };
	}

	const extrasEnd = HEADER_SIZE + header.extrasLength;
	const keyEnd = extrasEnd + header.keyLength;
	const valueEnd = HEADER_SIZE + header.totalBodyLength;

	const key =
		header.keyLength > 0
			? buf.subarray(extrasEnd, keyEnd).toString("utf8")
			: undefined;
	const value = valueEnd > keyEnd ? buf.subarray(keyEnd, valueEnd) : undefined;

	return { header, value, key };
}

/**
 * Parse an increment/decrement response
 */
export function parseIncrDecrResponse(buf: Buffer): {
	header: BinaryHeader;
	value: number | undefined;
} {
	const header = deserializeHeader(buf);
	if (header.status !== STATUS_SUCCESS || header.totalBodyLength < 8) {
		return { header, value: undefined };
	}

	// Value is 8-byte big-endian unsigned integer
	const high = buf.readUInt32BE(HEADER_SIZE);
	const low = buf.readUInt32BE(HEADER_SIZE + 4);
	const value = high * 0x100000000 + low;

	return { header, value };
}
