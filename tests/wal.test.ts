import * as crc32 from "crc-32";
import { createHash } from "crypto";
import fs, { mkdirSync, readdirSync } from "fs";
import { open } from "fs/promises";
import path from "path";
import { EntryRegistry } from "../lib/entry-registry";
import { SegmentReader } from "../lib/segment-reader";
import { SegmentWriter } from "../lib/segment-writer";
import { WAL } from "../lib/wal";
import { createRandomString, TextEntry } from "./utils";

describe("Test WAL ops", () => {
  const dirs: string[] = [];

  beforeAll(() => {
    EntryRegistry.register(() => new TextEntry());
  });

  afterAll(() => {
    for (const dir of dirs) {
      fs.rmSync(dir, { recursive: true });
    }
  });

  it("Should create new segments on first write if no segments exists", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath);

    expect(wal.isInitialized).toBe(false);

    await wal.init();
    const segmentsExists = readdirSync(walDirPath);
    expect(segmentsExists.length).toBe(0);

    //Segment will be created after first write.
    const entry = TextEntry.from("test");

    await wal.write(entry);

    const newSegments = readdirSync(walDirPath);
    expect(newSegments.length).toBe(1);

    await wal.close();
  });

  it("Should load from latest segment when prev segments exist", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    // Generate fake segments.
    for (let i = 0; i < 5; i++) {
      const file = await open(path.join(walDirPath, `${i}.wal`), "a+");
      const writer = new SegmentWriter(file);

      const baseOffset = i * 100;
      // write 100 entries
      for (let j = baseOffset; j < baseOffset + 100; j++) {
        const entry = TextEntry.from(`test-${j}`);
        const encoded = entry.encode();
        const checksum = crc32.buf(encoded) >>> 0;
        await writer.write(j, entry.type(), checksum, encoded);
      }

      await writer.close();
    }

    const wal = new WAL(walDirPath);

    await wal.init();
    const segmentsExists = readdirSync(walDirPath);

    expect(segmentsExists.length).toBe(5);

    expect(wal.getCurrentSegmentID()).toBe(4);

    //Segment will be created after first write.
    const entry = TextEntry.from("test-500");

    const lastOffset = await wal.write(entry);

    expect(lastOffset).toBe(500);

    await wal.close();
  });

  it("Should rollout new segment when max segment size is reached.", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 1024,
    });

    await wal.init();

    // Generate entries.
    for (let i = 0; i < 100; i++) {
      await wal.write(TextEntry.from(`test`));
    }

    const segmentsExists = readdirSync(walDirPath);

    expect(segmentsExists.length).toBeGreaterThan(1);

    expect(wal.getCurrentSegmentID()).toBeGreaterThan(0);

    await wal.close();
  });

  it("Should write entries concurrently", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 10 * 1024,
    });

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 10000 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    expect(wal.getCurrentSegmentID()).toBeGreaterThan(0);
    expect(wal.getLastOffset()).toBe(10000);

    await wal.close();

    const expectedContent = Buffer.concat(
      Array.from({ length: 10000 }, (_, i) => TextEntry.from(`test-${i}`).encode()),
    );

    const segmentsExists = readdirSync(walDirPath).sort((a, b) => parseInt(a) - parseInt(b));
    const content = await Promise.all(
      segmentsExists.map(async (segment) => {
        const file = await open(path.join(walDirPath, segment), "r");
        const reader = new SegmentReader(file);

        const entries: Buffer[] = [];
        while (true) {
          const isRead = await reader.readNext();
          if (!isRead) {
            break;
          }

          reader.decode();
          entries.push(reader.entry.encode());
        }

        return Buffer.concat(entries);
      }),
    );
    const allContent = Buffer.concat(content);

    const expectedHash = createHash("sha1").update(expectedContent).digest("hex");
    const actualHash = createHash("sha1").update(allContent).digest("hex");

    expect(actualHash).toEqual(expectedHash);
  });
});
