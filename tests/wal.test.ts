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
import { glob } from "glob";
import { MetaFileManager } from "../lib/meta-manager";

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
    const segmentsExists = await glob.glob(`${walDirPath}/*.wal`);
    expect(segmentsExists.length).toBe(0);

    //Segment will be created after first write.
    const entry = TextEntry.from("test");

    await wal.write(entry);

    const newSegments = await glob.glob(`${walDirPath}/*.wal`);
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
      await file.close();
    }

    const wal = new WAL(walDirPath);

    await wal.init();
    const segmentsExists = await glob.glob(`${walDirPath}/*.wal`);

    expect(segmentsExists.length).toBe(5);

    expect(wal.getCurrentSegmentID()).toBe(4);

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
    expect(wal.getLastIndex()).toBe(10000 - 1);

    await wal.close();

    const expectedContent = Buffer.concat(
      Array.from({ length: 10000 }, (_, i) => TextEntry.from(`test-${i}`).encode()),
    );

    const allContent = await readWALContent(walDirPath);

    const expectedHash = createHash("sha1").update(expectedContent).digest("hex");
    const actualHash = createHash("sha1").update(allContent).digest("hex");

    expect(actualHash).toEqual(expectedHash);
  });

  it("Should truncate all uncommitted entries when recovering - single segment", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath);

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 1000 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    for (let i = 0; i < 500; i++) {
      await wal.commit(i);
    }

    await wal.recover();

    expect(wal.getLastIndex()).toBe(499);

    const expectedContent = Buffer.concat(Array.from({ length: 500 }, (_, i) => TextEntry.from(`test-${i}`).encode()));

    const allContent = await readWALContent(walDirPath);

    const expectedHash = createHash("sha1").update(expectedContent).digest("hex");
    const actualHash = createHash("sha1").update(allContent).digest("hex");

    expect(actualHash).toEqual(expectedHash);

    await wal.close();
  });

  it("Should truncate all uncommitted entries when recovering - multiple segment", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 1024,
    });

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 1000 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    for (let i = 0; i < 500; i++) {
      await wal.commit(i);
    }

    // await new Promise((resolve) => setTimeout(resolve, 10000));

    await wal.recover();

    expect(wal.getLastIndex()).toBe(499);

    const expectedContent = Buffer.concat(Array.from({ length: 500 }, (_, i) => TextEntry.from(`test-${i}`).encode()));

    const allContent = await readWALContent(walDirPath);

    const expectedHash = createHash("sha1").update(expectedContent).digest("hex");
    const actualHash = createHash("sha1").update(allContent).digest("hex");

    expect(actualHash).toEqual(expectedHash);

    await wal.close();
  });

  it("Should truncate all unrecoverable entries", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 1024,
    });

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 1000 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    for (let i = 0; i < 500; i++) {
      await wal.commit(i);
    }

    // await new Promise((resolve) => setTimeout(resolve, 10000));

    await wal.recover(async (index) => {
      return index < 800;
    });

    expect(wal.getLastIndex()).toBe(799);

    const expectedContent = Buffer.concat(Array.from({ length: 800 }, (_, i) => TextEntry.from(`test-${i}`).encode()));

    const allContent = await readWALContent(walDirPath);

    const expectedHash = createHash("sha1").update(expectedContent).digest("hex");
    const actualHash = createHash("sha1").update(allContent).digest("hex");

    expect(actualHash).toEqual(expectedHash);

    await wal.close();
  });

  it("Should compact & remove older segments", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 1024,
      minEntriesForCompaction: 100,
    });

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 1000 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    for (let i = 0; i < 500; i++) {
      await wal.commit(i);
    }

    // await new Promise((resolve) => setTimeout(resolve, 10000));

    const oldCommit = wal.commitIndex;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const metaManager = (wal as any).metaManager as MetaFileManager;
    const { segmentID: segmentOfLastCommitted } = await metaManager.position(oldCommit);

    const deletedSegments = readdirSync(walDirPath)
      .filter((file) => file.endsWith(".wal") && parseInt(file) < segmentOfLastCommitted)
      .sort((a, b) => parseInt(a) - parseInt(b));

    await wal.compact();

    const currSegments = readdirSync(walDirPath)
      .filter((file) => file.endsWith(".wal"))
      .sort((a, b) => parseInt(a) - parseInt(b));

    // current segments shouldn't include the deleted segments.
    expect(parseInt(deletedSegments[deletedSegments.length - 1])).toBeLessThan(parseInt(currSegments[0]));

    await wal.close();
  });

  it("Should not compact - not enough entries", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 1024,
      minEntriesForCompaction: 1000,
    });

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 1000 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    for (let i = 0; i < 500; i++) {
      await wal.commit(i);
    }

    const currentSegmentsBefore = readdirSync(walDirPath).filter((file) => file.endsWith(".wal") && parseInt(file));

    const didCompact = await wal.compact();

    expect(didCompact).toBe(false);

    const currentSegmentsAfter = readdirSync(walDirPath).filter((file) => file.endsWith(".wal") && parseInt(file));

    expect(currentSegmentsAfter.length).toBe(currentSegmentsBefore.length);
    await wal.close();
  });

  it("Should not compact - first segment is active", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 10 * 1024 * 1024,
      minEntriesForCompaction: 10,
    });

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 100 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    for (let i = 0; i < 50; i++) {
      await wal.commit(i);
    }

    const currentSegmentsBefore = readdirSync(walDirPath).filter((file) => file.endsWith(".wal") && parseInt(file));

    const didCompact = await wal.compact();

    expect(didCompact).toBe(false);

    const currentSegmentsAfter = readdirSync(walDirPath).filter((file) => file.endsWith(".wal") && parseInt(file));

    expect(currentSegmentsAfter.length).toBe(currentSegmentsBefore.length);
    await wal.close();
  });

  it("Should archive & move older segments", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 1024,
      minEntriesForCompaction: 100,
    });

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 1000 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    for (let i = 0; i < 500; i++) {
      await wal.commit(i);
    }

    // await new Promise((resolve) => setTimeout(resolve, 10000));

    const oldCommit = wal.commitIndex;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const metaManager = (wal as any).metaManager as MetaFileManager;
    const { segmentID: segmentOfLastCommitted } = await metaManager.position(oldCommit);

    const deletedSegments = readdirSync(walDirPath)
      .filter((file) => file.endsWith(".wal") && parseInt(file) < segmentOfLastCommitted)
      .sort((a, b) => parseInt(a) - parseInt(b));

    const archiveDirPath = path.join(walDirPath, "archive");

    mkdirSync(archiveDirPath);

    await wal.archive(archiveDirPath);

    const currSegments = readdirSync(walDirPath)
      .filter((file) => file.endsWith(".wal"))
      .sort((a, b) => parseInt(a) - parseInt(b));

    // current segments shouldn't include the deleted segments.
    expect(parseInt(deletedSegments[deletedSegments.length - 1])).toBeLessThan(parseInt(currSegments[0]));

    const archivedSegments = readdirSync(archiveDirPath)
      .filter((file) => file.endsWith(".wal"))
      .sort((a, b) => parseInt(a) - parseInt(b));

    expect(archivedSegments).toEqual(deletedSegments);

    await wal.close();
  });

  it("Should not archive - not enough entries", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 1024,
      minEntriesForCompaction: 1000,
    });

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 1000 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    for (let i = 0; i < 500; i++) {
      await wal.commit(i);
    }

    const currentSegmentsBefore = readdirSync(walDirPath).filter((file) => file.endsWith(".wal") && parseInt(file));

    const archiveDirPath = path.join(walDirPath, "archive");

    mkdirSync(archiveDirPath);

    const didArchive = await wal.archive(archiveDirPath);

    expect(didArchive).toBe(false);

    const currentSegmentsAfter = readdirSync(walDirPath).filter((file) => file.endsWith(".wal") && parseInt(file));

    expect(currentSegmentsAfter.length).toBe(currentSegmentsBefore.length);

    const archivedSegments = readdirSync(archiveDirPath);

    expect(archivedSegments.length).toBe(0);

    await wal.close();
  });

  it("Should not archive - first segment is active", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 10 * 1024 * 1024,
      minEntriesForCompaction: 10,
    });

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 100 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    for (let i = 0; i < 50; i++) {
      await wal.commit(i);
    }

    const currentSegmentsBefore = readdirSync(walDirPath).filter((file) => file.endsWith(".wal") && parseInt(file));

    const archiveDirPath = path.join(walDirPath, "archive");

    mkdirSync(archiveDirPath);

    const didArchive = await wal.archive(archiveDirPath);

    expect(didArchive).toBe(false);

    const currentSegmentsAfter = readdirSync(walDirPath).filter((file) => file.endsWith(".wal") && parseInt(file));

    expect(currentSegmentsAfter.length).toBe(currentSegmentsBefore.length);

    const archivedSegments = readdirSync(archiveDirPath);

    expect(archivedSegments.length).toBe(0);

    await wal.close();
  });

  it("Should throw when trying to get compacted entry", async () => {
    const randomDirName = createRandomString(10);
    const walDirPath = path.join(__dirname, randomDirName);
    dirs.push(walDirPath);
    mkdirSync(walDirPath);

    const wal = new WAL(walDirPath, {
      maxSegmentSize: 1024,
      minEntriesForCompaction: 100,
    });

    await wal.init();

    // Generate entries.
    await Promise.all(
      Array.from({ length: 1000 }, (_, i) => {
        return wal.write(TextEntry.from(`test-${i}`));
      }),
    );

    for (let i = 0; i < 500; i++) {
      await wal.commit(i);
    }

    await wal.compact();

    expect(wal.getEntry(0)).rejects.toThrow("Invalid log offset 0. Out of bounds");

    const entry = await wal.getEntry(800);

    expect(entry).not.toBeNull();

    await wal.close();
  });
});

async function readWALContent(walDirPath: string): Promise<Buffer> {
  const segmentsExists = readdirSync(walDirPath)
    .filter((file) => file.endsWith(".wal") && parseInt(file) >= 0)
    .sort((a, b) => parseInt(a) - parseInt(b));

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

      await file.close();
      return Buffer.concat(entries);
    }),
  );

  return Buffer.concat(content);
}
