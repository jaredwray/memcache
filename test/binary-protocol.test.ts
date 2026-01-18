import { describe, expect, it } from "vitest";
import {
	buildAppendRequest,
	buildFlushRequest,
	buildPrependRequest,
	buildQuitRequest,
	buildSaslListMechsRequest,
	buildSaslPlainRequest,
	buildStatRequest,
	buildTouchRequest,
	buildVersionRequest,
	deserializeHeader,
	HEADER_SIZE,
	OPCODE_APPEND,
	OPCODE_FLUSH,
	OPCODE_PREPEND,
	OPCODE_QUIT,
	OPCODE_SASL_AUTH,
	OPCODE_SASL_LIST_MECHS,
	OPCODE_STAT,
	OPCODE_TOUCH,
	OPCODE_VERSION,
	parseGetResponse,
	parseIncrDecrResponse,
	REQUEST_MAGIC,
	RESPONSE_MAGIC,
	STATUS_AUTH_ERROR,
	STATUS_KEY_NOT_FOUND,
	STATUS_SUCCESS,
	serializeHeader,
} from "../src/binary-protocol.js";

describe("Binary Protocol", () => {
	describe("Constants", () => {
		it("should have correct magic byte values", () => {
			expect(REQUEST_MAGIC).toBe(0x80);
			expect(RESPONSE_MAGIC).toBe(0x81);
		});

		it("should have correct SASL opcode values", () => {
			expect(OPCODE_SASL_LIST_MECHS).toBe(0x20);
			expect(OPCODE_SASL_AUTH).toBe(0x21);
		});

		it("should have correct status code values", () => {
			expect(STATUS_SUCCESS).toBe(0x0000);
			expect(STATUS_AUTH_ERROR).toBe(0x0020);
		});

		it("should have correct header size", () => {
			expect(HEADER_SIZE).toBe(24);
		});
	});

	describe("serializeHeader", () => {
		it("should create a 24-byte buffer", () => {
			const header = serializeHeader({});
			expect(header.length).toBe(HEADER_SIZE);
		});

		it("should set magic byte at offset 0", () => {
			const header = serializeHeader({ magic: REQUEST_MAGIC });
			expect(header.readUInt8(0)).toBe(REQUEST_MAGIC);
		});

		it("should set opcode at offset 1", () => {
			const header = serializeHeader({ opcode: OPCODE_SASL_AUTH });
			expect(header.readUInt8(1)).toBe(OPCODE_SASL_AUTH);
		});

		it("should set key length at offset 2 (big endian)", () => {
			const header = serializeHeader({ keyLength: 5 });
			expect(header.readUInt16BE(2)).toBe(5);
		});

		it("should set extras length at offset 4", () => {
			const header = serializeHeader({ extrasLength: 8 });
			expect(header.readUInt8(4)).toBe(8);
		});

		it("should set data type at offset 5", () => {
			const header = serializeHeader({ dataType: 1 });
			expect(header.readUInt8(5)).toBe(1);
		});

		it("should set status at offset 6 (big endian)", () => {
			const header = serializeHeader({ status: STATUS_SUCCESS });
			expect(header.readUInt16BE(6)).toBe(STATUS_SUCCESS);
		});

		it("should set total body length at offset 8 (big endian)", () => {
			const header = serializeHeader({ totalBodyLength: 100 });
			expect(header.readUInt32BE(8)).toBe(100);
		});

		it("should set opaque at offset 12 (big endian)", () => {
			const header = serializeHeader({ opaque: 12345 });
			expect(header.readUInt32BE(12)).toBe(12345);
		});

		it("should copy CAS value at offset 16", () => {
			const cas = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8]);
			const header = serializeHeader({ cas });
			expect(header.subarray(16, 24)).toEqual(cas);
		});

		it("should use default values when properties are not provided", () => {
			const header = serializeHeader({});
			expect(header.readUInt8(0)).toBe(REQUEST_MAGIC); // Default magic
			expect(header.readUInt8(1)).toBe(0); // Default opcode
			expect(header.readUInt16BE(2)).toBe(0); // Default keyLength
			expect(header.readUInt8(4)).toBe(0); // Default extrasLength
			expect(header.readUInt8(5)).toBe(0); // Default dataType
			expect(header.readUInt16BE(6)).toBe(0); // Default status
			expect(header.readUInt32BE(8)).toBe(0); // Default totalBodyLength
			expect(header.readUInt32BE(12)).toBe(0); // Default opaque
		});
	});

	describe("deserializeHeader", () => {
		it("should parse magic byte from offset 0", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt8(RESPONSE_MAGIC, 0);
			const header = deserializeHeader(buf);
			expect(header.magic).toBe(RESPONSE_MAGIC);
		});

		it("should parse opcode from offset 1", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt8(OPCODE_SASL_AUTH, 1);
			const header = deserializeHeader(buf);
			expect(header.opcode).toBe(OPCODE_SASL_AUTH);
		});

		it("should parse key length from offset 2", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt16BE(10, 2);
			const header = deserializeHeader(buf);
			expect(header.keyLength).toBe(10);
		});

		it("should parse extras length from offset 4", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt8(8, 4);
			const header = deserializeHeader(buf);
			expect(header.extrasLength).toBe(8);
		});

		it("should parse data type from offset 5", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt8(1, 5);
			const header = deserializeHeader(buf);
			expect(header.dataType).toBe(1);
		});

		it("should parse status from offset 6", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt16BE(STATUS_AUTH_ERROR, 6);
			const header = deserializeHeader(buf);
			expect(header.status).toBe(STATUS_AUTH_ERROR);
		});

		it("should parse total body length from offset 8", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt32BE(256, 8);
			const header = deserializeHeader(buf);
			expect(header.totalBodyLength).toBe(256);
		});

		it("should parse opaque from offset 12", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt32BE(99999, 12);
			const header = deserializeHeader(buf);
			expect(header.opaque).toBe(99999);
		});

		it("should extract CAS from offset 16-24", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			const cas = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8]);
			cas.copy(buf, 16);
			const header = deserializeHeader(buf);
			expect(header.cas).toEqual(cas);
		});
	});

	describe("serializeHeader and deserializeHeader round-trip", () => {
		it("should preserve all values through serialization and deserialization", () => {
			const cas = Buffer.from([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
			const original = {
				magic: REQUEST_MAGIC,
				opcode: OPCODE_SASL_AUTH,
				keyLength: 5,
				extrasLength: 8,
				dataType: 0,
				status: STATUS_SUCCESS,
				totalBodyLength: 100,
				opaque: 12345,
				cas,
			};

			const serialized = serializeHeader(original);
			const deserialized = deserializeHeader(serialized);

			expect(deserialized.magic).toBe(original.magic);
			expect(deserialized.opcode).toBe(original.opcode);
			expect(deserialized.keyLength).toBe(original.keyLength);
			expect(deserialized.extrasLength).toBe(original.extrasLength);
			expect(deserialized.dataType).toBe(original.dataType);
			expect(deserialized.status).toBe(original.status);
			expect(deserialized.totalBodyLength).toBe(original.totalBodyLength);
			expect(deserialized.opaque).toBe(original.opaque);
			expect(deserialized.cas).toEqual(original.cas);
		});
	});

	describe("buildSaslPlainRequest", () => {
		it("should create a buffer with header and body", () => {
			const packet = buildSaslPlainRequest("testuser", "testpass");
			expect(packet.length).toBeGreaterThan(HEADER_SIZE);
		});

		it("should set request magic byte", () => {
			const packet = buildSaslPlainRequest("testuser", "testpass");
			expect(packet.readUInt8(0)).toBe(REQUEST_MAGIC);
		});

		it("should set SASL AUTH opcode", () => {
			const packet = buildSaslPlainRequest("testuser", "testpass");
			expect(packet.readUInt8(1)).toBe(OPCODE_SASL_AUTH);
		});

		it("should set key length to 5 (PLAIN)", () => {
			const packet = buildSaslPlainRequest("testuser", "testpass");
			expect(packet.readUInt16BE(2)).toBe(5); // "PLAIN".length
		});

		it("should include PLAIN mechanism as key", () => {
			const packet = buildSaslPlainRequest("testuser", "testpass");
			const key = packet.subarray(HEADER_SIZE, HEADER_SIZE + 5).toString();
			expect(key).toBe("PLAIN");
		});

		it("should include credentials in SASL PLAIN format", () => {
			const packet = buildSaslPlainRequest("testuser", "testpass");
			const keyLength = packet.readUInt16BE(2);
			const body = packet.subarray(HEADER_SIZE + keyLength);
			// SASL PLAIN format: \0username\0password
			expect(body.toString()).toBe("\x00testuser\x00testpass");
		});

		it("should set correct total body length", () => {
			const username = "testuser";
			const password = "testpass";
			const packet = buildSaslPlainRequest(username, password);

			const keyLength = 5; // "PLAIN".length
			// Value: \0 + username + \0 + password
			const valueLength = 1 + username.length + 1 + password.length;
			const expectedBodyLength = keyLength + valueLength;

			expect(packet.readUInt32BE(8)).toBe(expectedBodyLength);
		});

		it("should handle special characters in credentials", () => {
			const packet = buildSaslPlainRequest("user@domain.com", "p@ss!w0rd#$");
			const keyLength = packet.readUInt16BE(2);
			const body = packet.subarray(HEADER_SIZE + keyLength);
			expect(body.toString()).toBe("\x00user@domain.com\x00p@ss!w0rd#$");
		});

		it("should handle empty username", () => {
			const packet = buildSaslPlainRequest("", "password");
			const keyLength = packet.readUInt16BE(2);
			const body = packet.subarray(HEADER_SIZE + keyLength);
			expect(body.toString()).toBe("\x00\x00password");
		});

		it("should handle empty password", () => {
			const packet = buildSaslPlainRequest("username", "");
			const keyLength = packet.readUInt16BE(2);
			const body = packet.subarray(HEADER_SIZE + keyLength);
			expect(body.toString()).toBe("\x00username\x00");
		});
	});

	describe("buildSaslListMechsRequest", () => {
		it("should create a 24-byte header-only packet", () => {
			const packet = buildSaslListMechsRequest();
			expect(packet.length).toBe(HEADER_SIZE);
		});

		it("should set request magic byte", () => {
			const packet = buildSaslListMechsRequest();
			expect(packet.readUInt8(0)).toBe(REQUEST_MAGIC);
		});

		it("should set SASL LIST MECHS opcode", () => {
			const packet = buildSaslListMechsRequest();
			expect(packet.readUInt8(1)).toBe(OPCODE_SASL_LIST_MECHS);
		});

		it("should have zero body length", () => {
			const packet = buildSaslListMechsRequest();
			expect(packet.readUInt32BE(8)).toBe(0);
		});

		it("should have zero key length", () => {
			const packet = buildSaslListMechsRequest();
			expect(packet.readUInt16BE(2)).toBe(0);
		});
	});

	describe("buildAppendRequest", () => {
		it("should create packet with correct opcode", () => {
			const packet = buildAppendRequest("key", "value");
			expect(packet.readUInt8(1)).toBe(OPCODE_APPEND);
		});

		it("should set request magic byte", () => {
			const packet = buildAppendRequest("key", "value");
			expect(packet.readUInt8(0)).toBe(REQUEST_MAGIC);
		});

		it("should set correct key length", () => {
			const packet = buildAppendRequest("testkey", "value");
			expect(packet.readUInt16BE(2)).toBe(7); // "testkey".length
		});

		it("should set correct total body length", () => {
			const packet = buildAppendRequest("key", "value");
			// Total body = key (3) + value (5) = 8
			expect(packet.readUInt32BE(8)).toBe(8);
		});

		it("should include key and value in body", () => {
			const packet = buildAppendRequest("key", "value");
			const body = packet.subarray(HEADER_SIZE);
			expect(body.toString()).toBe("keyvalue");
		});

		it("should handle Buffer value", () => {
			const valueBuf = Buffer.from("buffervalue");
			const packet = buildAppendRequest("key", valueBuf);
			const body = packet.subarray(HEADER_SIZE);
			expect(body.toString()).toBe("keybuffervalue");
		});
	});

	describe("buildPrependRequest", () => {
		it("should create packet with correct opcode", () => {
			const packet = buildPrependRequest("key", "value");
			expect(packet.readUInt8(1)).toBe(OPCODE_PREPEND);
		});

		it("should set request magic byte", () => {
			const packet = buildPrependRequest("key", "value");
			expect(packet.readUInt8(0)).toBe(REQUEST_MAGIC);
		});

		it("should set correct key length", () => {
			const packet = buildPrependRequest("mykey", "value");
			expect(packet.readUInt16BE(2)).toBe(5); // "mykey".length
		});

		it("should set correct total body length", () => {
			const packet = buildPrependRequest("key", "value");
			expect(packet.readUInt32BE(8)).toBe(8); // key (3) + value (5)
		});

		it("should include key and value in body", () => {
			const packet = buildPrependRequest("key", "value");
			const body = packet.subarray(HEADER_SIZE);
			expect(body.toString()).toBe("keyvalue");
		});

		it("should handle Buffer value", () => {
			const valueBuf = Buffer.from("bufval");
			const packet = buildPrependRequest("k", valueBuf);
			const body = packet.subarray(HEADER_SIZE);
			expect(body.toString()).toBe("kbufval");
		});
	});

	describe("buildTouchRequest", () => {
		it("should create packet with correct opcode", () => {
			const packet = buildTouchRequest("key", 3600);
			expect(packet.readUInt8(1)).toBe(OPCODE_TOUCH);
		});

		it("should set request magic byte", () => {
			const packet = buildTouchRequest("key", 3600);
			expect(packet.readUInt8(0)).toBe(REQUEST_MAGIC);
		});

		it("should set extras length to 4", () => {
			const packet = buildTouchRequest("key", 3600);
			expect(packet.readUInt8(4)).toBe(4);
		});

		it("should set correct key length", () => {
			const packet = buildTouchRequest("testkey", 3600);
			expect(packet.readUInt16BE(2)).toBe(7);
		});

		it("should set correct total body length", () => {
			const packet = buildTouchRequest("key", 3600);
			// extras (4) + key (3) = 7
			expect(packet.readUInt32BE(8)).toBe(7);
		});

		it("should include exptime in extras", () => {
			const packet = buildTouchRequest("key", 3600);
			const exptime = packet.readUInt32BE(HEADER_SIZE);
			expect(exptime).toBe(3600);
		});

		it("should include key after extras", () => {
			const packet = buildTouchRequest("mykey", 100);
			const key = packet.subarray(HEADER_SIZE + 4, HEADER_SIZE + 4 + 5);
			expect(key.toString()).toBe("mykey");
		});
	});

	describe("buildFlushRequest", () => {
		it("should create packet with correct opcode", () => {
			const packet = buildFlushRequest();
			expect(packet.readUInt8(1)).toBe(OPCODE_FLUSH);
		});

		it("should set request magic byte", () => {
			const packet = buildFlushRequest();
			expect(packet.readUInt8(0)).toBe(REQUEST_MAGIC);
		});

		it("should set extras length to 4", () => {
			const packet = buildFlushRequest();
			expect(packet.readUInt8(4)).toBe(4);
		});

		it("should set total body length to 4", () => {
			const packet = buildFlushRequest();
			expect(packet.readUInt32BE(8)).toBe(4);
		});

		it("should default exptime to 0", () => {
			const packet = buildFlushRequest();
			const exptime = packet.readUInt32BE(HEADER_SIZE);
			expect(exptime).toBe(0);
		});

		it("should set custom exptime", () => {
			const packet = buildFlushRequest(60);
			const exptime = packet.readUInt32BE(HEADER_SIZE);
			expect(exptime).toBe(60);
		});
	});

	describe("buildVersionRequest", () => {
		it("should create a header-only packet", () => {
			const packet = buildVersionRequest();
			expect(packet.length).toBe(HEADER_SIZE);
		});

		it("should set correct opcode", () => {
			const packet = buildVersionRequest();
			expect(packet.readUInt8(1)).toBe(OPCODE_VERSION);
		});

		it("should set request magic byte", () => {
			const packet = buildVersionRequest();
			expect(packet.readUInt8(0)).toBe(REQUEST_MAGIC);
		});

		it("should have zero body length", () => {
			const packet = buildVersionRequest();
			expect(packet.readUInt32BE(8)).toBe(0);
		});
	});

	describe("buildStatRequest", () => {
		it("should create header-only packet when no key provided", () => {
			const packet = buildStatRequest();
			expect(packet.length).toBe(HEADER_SIZE);
		});

		it("should set correct opcode", () => {
			const packet = buildStatRequest();
			expect(packet.readUInt8(1)).toBe(OPCODE_STAT);
		});

		it("should set request magic byte", () => {
			const packet = buildStatRequest();
			expect(packet.readUInt8(0)).toBe(REQUEST_MAGIC);
		});

		it("should have zero body length when no key", () => {
			const packet = buildStatRequest();
			expect(packet.readUInt32BE(8)).toBe(0);
		});

		it("should include key when provided", () => {
			const packet = buildStatRequest("items");
			expect(packet.length).toBe(HEADER_SIZE + 5);
		});

		it("should set key length when key provided", () => {
			const packet = buildStatRequest("slabs");
			expect(packet.readUInt16BE(2)).toBe(5);
		});

		it("should set body length to key length when key provided", () => {
			const packet = buildStatRequest("items");
			expect(packet.readUInt32BE(8)).toBe(5);
		});

		it("should include key in body", () => {
			const packet = buildStatRequest("items");
			const key = packet.subarray(HEADER_SIZE);
			expect(key.toString()).toBe("items");
		});
	});

	describe("buildQuitRequest", () => {
		it("should create a header-only packet", () => {
			const packet = buildQuitRequest();
			expect(packet.length).toBe(HEADER_SIZE);
		});

		it("should set correct opcode", () => {
			const packet = buildQuitRequest();
			expect(packet.readUInt8(1)).toBe(OPCODE_QUIT);
		});

		it("should set request magic byte", () => {
			const packet = buildQuitRequest();
			expect(packet.readUInt8(0)).toBe(REQUEST_MAGIC);
		});

		it("should have zero body length", () => {
			const packet = buildQuitRequest();
			expect(packet.readUInt32BE(8)).toBe(0);
		});
	});

	describe("parseGetResponse", () => {
		it("should return undefined value when status is not SUCCESS", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt8(RESPONSE_MAGIC, 0);
			buf.writeUInt16BE(STATUS_KEY_NOT_FOUND, 6);
			const result = parseGetResponse(buf);
			expect(result.header.status).toBe(STATUS_KEY_NOT_FOUND);
			expect(result.value).toBeUndefined();
			expect(result.key).toBeUndefined();
		});

		it("should return undefined value when status is AUTH_ERROR", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt8(RESPONSE_MAGIC, 0);
			buf.writeUInt16BE(STATUS_AUTH_ERROR, 6);
			const result = parseGetResponse(buf);
			expect(result.header.status).toBe(STATUS_AUTH_ERROR);
			expect(result.value).toBeUndefined();
		});

		it("should parse successful response with value", () => {
			// Create a successful GET response
			const value = "testvalue";
			const extras = Buffer.alloc(4); // flags
			const valueBuf = Buffer.from(value);
			const totalBody = extras.length + valueBuf.length;

			const header = Buffer.alloc(HEADER_SIZE);
			header.writeUInt8(RESPONSE_MAGIC, 0);
			header.writeUInt16BE(STATUS_SUCCESS, 6);
			header.writeUInt8(4, 4); // extrasLength
			header.writeUInt32BE(totalBody, 8);

			const buf = Buffer.concat([header, extras, valueBuf]);
			const result = parseGetResponse(buf);

			expect(result.header.status).toBe(STATUS_SUCCESS);
			expect(result.value?.toString()).toBe(value);
		});
	});

	describe("parseIncrDecrResponse", () => {
		it("should return undefined value when status is not SUCCESS", () => {
			const buf = Buffer.alloc(HEADER_SIZE);
			buf.writeUInt8(RESPONSE_MAGIC, 0);
			buf.writeUInt16BE(STATUS_KEY_NOT_FOUND, 6);
			const result = parseIncrDecrResponse(buf);
			expect(result.header.status).toBe(STATUS_KEY_NOT_FOUND);
			expect(result.value).toBeUndefined();
		});

		it("should return undefined value when body length is less than 8", () => {
			const buf = Buffer.alloc(HEADER_SIZE + 4);
			buf.writeUInt8(RESPONSE_MAGIC, 0);
			buf.writeUInt16BE(STATUS_SUCCESS, 6);
			buf.writeUInt32BE(4, 8); // totalBodyLength = 4 (less than 8)
			const result = parseIncrDecrResponse(buf);
			expect(result.value).toBeUndefined();
		});

		it("should parse successful response with value", () => {
			const buf = Buffer.alloc(HEADER_SIZE + 8);
			buf.writeUInt8(RESPONSE_MAGIC, 0);
			buf.writeUInt16BE(STATUS_SUCCESS, 6);
			buf.writeUInt32BE(8, 8); // totalBodyLength = 8
			// Write value as 64-bit big-endian: 42
			buf.writeUInt32BE(0, HEADER_SIZE); // high 32 bits
			buf.writeUInt32BE(42, HEADER_SIZE + 4); // low 32 bits

			const result = parseIncrDecrResponse(buf);
			expect(result.header.status).toBe(STATUS_SUCCESS);
			expect(result.value).toBe(42);
		});

		it("should parse large 64-bit values", () => {
			const buf = Buffer.alloc(HEADER_SIZE + 8);
			buf.writeUInt8(RESPONSE_MAGIC, 0);
			buf.writeUInt16BE(STATUS_SUCCESS, 6);
			buf.writeUInt32BE(8, 8);
			// Write a large value: 0x0000000100000000 (4294967296)
			buf.writeUInt32BE(1, HEADER_SIZE); // high 32 bits
			buf.writeUInt32BE(0, HEADER_SIZE + 4); // low 32 bits

			const result = parseIncrDecrResponse(buf);
			expect(result.value).toBe(0x100000000);
		});
	});
});
