import { Mutex } from "async-mutex";
import * as crc32 from "crc-32";
import EventEmitter from "events";
import * as fs from "fs/promises";
import * as glob from "glob";
import path from "path";
import { EntryType, IEntry } from "./entry";
import { SegmentReader } from "./segment-reader";
import { SegmentWriter } from "./segment-writer";

type WALOptions = {
  logger?: (level: string, msg: string, attrs?: Record<string, unknown>) => void;
  maxSegmentSize?: number;
  onSync?: (() => void)[];
};

export class WAL {
  private currSegmentFile: fs.FileHandle;
  private currSegmentWriter: SegmentWriter | null = null;
  private currSegmentID = 0;
  private lastOffset = 0;

  private maxSegmentSize: number = 10 * 1024 * 1024; // 10MB
  private onSync: (() => void)[] = [];

  private writeLock = new Mutex();

  // eslint-disable-next-line @typescript-eslint/no-empty-function
  private logger: (level: string, msg: string, attrs?: Record<string, unknown>) => void = () => {}; // noop

  public isInitialized = false;
  private isClosed = false;

  private syncWaiters: (() => void)[] = []; // List of resolve functions.

  private emitter: EventEmitter = new EventEmitter();

  constructor(private walFilePath: string, opts?: WALOptions) {
    this.logger = opts?.logger || this.logger;
    this.maxSegmentSize = opts?.maxSegmentSize || this.maxSegmentSize;
    this.onSync = opts?.onSync || this.onSync;
  }

  async init(): Promise<void> {
    if (this.isInitialized) {
      return;
    }

    this.logger("debug", `Initializing WAL at ${this.walFilePath}`);

    const segments = await this.loadSegmentFilesNames();
    if (segments.length === 0) {
      this.logger("debug", `No segments found in WAL at ${this.walFilePath}`);

      return;
    }

    const lastSegment = segments[segments.length - 1];

    this.logger("debug", `Loading last segment in WAL at ${this.walFilePath}: ${lastSegment}`, {
      last_segment: lastSegment,
      segments: segments,
    });

    this.currSegmentWriter = await this.openSegmentFile(lastSegment);
    this.currSegmentID = parseInt(lastSegment.split(".")[0]);

    this.logger(
      "debug",
      `Loaded last segment in WAL at ${this.walFilePath}: ${lastSegment}, offset: ${this.lastOffset}`,
      {
        last_segment: lastSegment,
        last_offset: this.lastOffset,
        segments: segments,
      },
    );

    this.isInitialized = true;
  }

  public async write(entry: IEntry): Promise<number> {
    // Serialize the new WAL entry first into a buffer and then flush it with a
    // single write operation to disk.
    const encodedEntry = entry.encode();
    const checksum = crc32.buf(encodedEntry) >>> 0;

    const offset = await this.writeLock.runExclusive(() => this.doWrite(entry.type(), checksum, encodedEntry));

    // Update the last offset.

    return offset;
  }

  // Close gracefully shuts down the writeAheadLog by making sure that all pending
  // writes are completed and synced to disk before then closing the WAL segment file.
  // Any future writes after the WAL has been closed will lead to an error.
  public async close(): Promise<void> {
    this.isClosed = true;

    if (!this.isInitialized) {
      return;
    }

    if (this.currSegmentWriter === null) {
      // We never got a single write, so we can return immediately.
      return;
    }

    await this.sync();

    this.logger("debug", `Closing WAL at ${this.walFilePath}`);

    await this.currSegmentWriter.close();

    this.currSegmentWriter = null;
    this.isInitialized = false;
  }

  private async loadSegmentFilesNames(): Promise<string[]> {
    const dirs = await glob.glob(`${this.walFilePath}/*.wal`);

    return dirs
      .map((dir) => path.basename(dir))
      .sort((a, b) => {
        const aInt = parseInt(a.split(".")[0]);
        const bInt = parseInt(b.split(".")[0]);

        return aInt - bInt;
      });
  }

  private async openSegmentFile(segment: string): Promise<SegmentWriter> {
    const file = await fs.open(`${this.walFilePath}/${segment}`, "a+");

    // seek to the end of the file
    const segmentReader = new SegmentReader(file);
    this.lastOffset = await segmentReader.seekEnd();

    return new SegmentWriter(file.createWriteStream());
  }

  private async doWrite(type: EntryType, checksum: number, payload: Buffer): Promise<number> {
    // While holding the lock, make sure the log has not been closed.
    if (this.isClosed) {
      throw new Error("WAL is closed");
    }

    await this.rollSegmentIfNeeded();

    // First check if we need to roll over to a new segment because the current
    // one is full. It might also be that we do not yet have a segment file at
    // all, because this is the very first write to the WAL. In this case this
    // function is going to set up the segment writer for us now.
    // await this.rollSegmentIfNeeded();

    const offset = this.lastOffset + 1;

    this.logger("debug", "Writing WAL entry", {
      entry: payload.toString(),
      offset,
      type,
      checksum,
      segmentID: this.currSegmentID,
    });

    // Write the entry to the segment file.
    await this.currSegmentWriter.write(offset, type, checksum, payload);
    this.lastOffset = offset;

    // this.scheduleSync();
    await this.sync();

    return offset;
  }

  private async rollSegmentIfNeeded(): Promise<void> {
    if (this.currSegmentWriter !== null && this.currSegmentWriter.size < this.maxSegmentSize) {
      return;
    }

    // create new segment
    this.currSegmentID += 1;

    if (this.currSegmentWriter !== null) {
      // Sync all pending writes before rolling over to a new segment.
      await this.currSegmentWriter.sync();
      await this.currSegmentWriter.close();
    }

    const segmentFilename = path.join(this.walFilePath, `${this.currSegmentID}.wal`);
    this.currSegmentFile = await fs.open(segmentFilename, "a+");

    this.logger("debug", "Rolling over to new WAL segment", {
      segmentID: this.currSegmentID,
      maxSegmentSize: this.maxSegmentSize,
    });

    this.currSegmentWriter = new SegmentWriter(this.currSegmentFile.createWriteStream());

    return;
  }

  // sync the segment writer and then notify all goroutines that currently wait
  // for a WAL sync.
  // The caller must ensure the WAL is write-locked before calling this function.
  private async sync(): Promise<void> {
    const t1 = Date.now();
    await this.currSegmentWriter.sync();
    const duration = Date.now() - t1;

    if (this.syncWaiters.length === 0) {
      return;
    }

    this.logger("debug", "WAL sync done", {
      duration,
      segmentID: this.currSegmentID,
    });

    for (const cb of this.syncWaiters) {
      cb();
    }

    this.syncWaiters = [];

    this.emitter.emit("sync");
  }

  getCurrentSegmentID(): number {
    return this.currSegmentID;
  }

  getLastOffset(): number {
    return this.lastOffset;
  }
}
