import { ReadStream, unlinkSync } from "fs";
import { open } from "fs/promises";
import path from "path";
import { SegmentWriter } from "../lib/segment-writer";
import { SegmentReader } from "../lib/segment-reader";
import { EntryRegistry } from "../lib/entry-registry";
import { IEntry, Reader } from "../lib/entry";
import * as crc32 from "crc-32";
import { TextEntry } from "./utils";

describe("Test segment reader ops", () => {
  const files: string[] = [];

  beforeAll(() => {
    EntryRegistry.register(() => new TextEntry());
  });

  afterAll(() => {
    for (const file of files) {
      unlinkSync(file);
    }
  });

  it("Should read single line from segment", async () => {
    const randomName = Math.random().toString(36).substring(7);
    const filePath = path.join(__dirname, `${randomName}.wal`);
    files.push(filePath);
    const wfile = await open(filePath, "a+");
    const rfile = await open(filePath, "r");

    const writer = new SegmentWriter(wfile.createWriteStream());
    const typ = 0;

    const entry = new TextEntry();
    entry.length = 4;
    entry.content = "test";

    const encoded = entry.encode();
    const checksum = crc32.buf(encoded);

    await writer.write(0, typ, checksum, encoded);

    await writer.sync();

    const reader = new SegmentReader(rfile);

    const isRead = await reader.readNext();

    expect(isRead).toBe(true);

    const offset = reader.offset;

    expect(offset).toBe(0);

    reader.decode();

    expect((reader.entry as TextEntry).content).toBe("test");
  });

  it("Should throw if invalid checksum", async () => {
    const randomName = Math.random().toString(36).substring(7);
    const filePath = path.join(__dirname, `${randomName}.wal`);
    files.push(filePath);
    const wfile = await open(filePath, "a+");
    const rfile = await open(filePath, "r");

    const writer = new SegmentWriter(wfile.createWriteStream());
    const typ = 0;

    const entry = new TextEntry();
    entry.length = 4;
    entry.content = "test";

    const encoded = entry.encode();
    const checksum = crc32.str("test") >>> 0;

    console.log(checksum);

    await writer.write(0, typ, checksum, encoded);

    await writer.sync();

    const reader = new SegmentReader(rfile);

    const isRead = await reader.readNext();

    expect(isRead).toBe(true);

    const offset = reader.offset;

    expect(offset).toBe(0);

    try {
      reader.decode();
    } catch (e) {
      expect(e.message).toBe("Detected WAL Entry corruption at WAL offset 0");
    }
  });

  it("Should throw if invalid entry type", async () => {
    const randomName = Math.random().toString(36).substring(7);
    const filePath = path.join(__dirname, `${randomName}.wal`);
    files.push(filePath);
    const wfile = await open(filePath, "a+");
    const rfile = await open(filePath, "r");

    const writer = new SegmentWriter(wfile.createWriteStream());
    const typ = 1;

    const entry = new TextEntry();
    entry.length = 4;
    entry.content = "test";

    const encoded = entry.encode();
    const checksum = crc32.buf(encoded);

    await writer.write(0, typ, checksum, encoded);

    await writer.sync();

    const reader = new SegmentReader(rfile);

    try {
      await reader.readNext();
    } catch (e) {
      expect(e.message).toBe("Invalid entry type");
    }
  });

  it("Should throw when calling decode before readNext", async () => {
    const randomName = Math.random().toString(36).substring(7);
    const filePath = path.join(__dirname, `${randomName}.wal`);
    files.push(filePath);
    const wfile = await open(filePath, "a+");
    const rfile = await open(filePath, "r");

    const writer = new SegmentWriter(wfile.createWriteStream());
    const typ = 0;

    const entry = new TextEntry();
    entry.length = 4;
    entry.content = "test";

    const encoded = entry.encode();
    const checksum = crc32.buf(encoded);

    await writer.write(0, typ, checksum, encoded);

    await writer.sync();

    const reader = new SegmentReader(rfile);

    try {
      reader.decode();
    } catch (e) {
      expect(e.message).toBe("Must call SegmentReader.readNext() first");
    }
  });

  it("Seek end of file", async () => {
    const randomName = Math.random().toString(36).substring(7);
    const filePath = path.join(__dirname, `${randomName}.wal`);
    files.push(filePath);
    const wfile = await open(filePath, "a+");
    const rfile = await open(filePath, "r");

    const writer = new SegmentWriter(wfile.createWriteStream());
    const typ = 0;

    for (let i = 0; i < 10; i++) {
      const entry = new TextEntry();
      entry.content = "test" + i;
      entry.length = entry.content.length;

      const encoded = entry.encode();
      const checksum = crc32.buf(encoded) >>> 0;

      await writer.write(i, typ, checksum, encoded);
    }

    await writer.sync();

    const reader = new SegmentReader(rfile);

    const lastOffset = await reader.seekEnd();

    // const isRead = await reader.readNext();

    expect(lastOffset).toBe(9);
  });
});
