import { describe, expect, it } from "vitest";
import {
	buildSaslListMechsRequest,
	buildSaslPlainRequest,
	deserializeHeader,
	HEADER_SIZE,
	OPCODE_SASL_AUTH,
	OPCODE_SASL_LIST_MECHS,
	REQUEST_MAGIC,
	RESPONSE_MAGIC,
	STATUS_AUTH_ERROR,
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
});
