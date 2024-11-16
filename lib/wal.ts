import { Mutex } from "async-mutex";
import * as crc32 from "crc-32";
import * as fs from "fs/promises";
import * as glob from "glob";
import path from "path";
import { EntryType, IEntry } from "./entry";
import { MetaFileManager, MetaManagerOptions } from "./meta-manager";
import { SegmentWriter } from "./segment-writer";
import { SegmentReader } from "./segment-reader";

type WALOptions = {
  logger?: (level: string, msg: string, attrs?: Record<string, unknown>) => void;
  maxSegmentSize?: number;
  // onSync?: (() => void)[];
  meta?: MetaManagerOptions;
  // syncDelay?: number;
};

export class WAL {
  private currSegmentFile: fs.FileHandle;
  private currSegmentWriter: SegmentWriter | null = null;
  private currSegmentID = -1;
  // private lastOffset = 0;
  private metaManager: MetaFileManager;
  private metaManagerOpts: MetaManagerOptions;

  private maxSegmentSize: number = 10 * 1024 * 1024; // 10MB
  // private onSync: (() => void)[] = [];
  private syncDelay = 0;

  private writeLock = new Mutex();

  // eslint-disable-next-line @typescript-eslint/no-empty-function
  private logger: (level: string, msg: string, attrs?: Record<string, unknown>) => void = () => {}; // noop

  private _isInitialized = false;
  public get isInitialized(): boolean {
    return this._isInitialized;
  }

  private isClosed = false;

  private syncWaiters: (() => void)[] = []; // List of resolve functions.
  private isSyncOngoing = false;

  // private emitter: EventEmitter = new EventEmitter();

  constructor(private walFilePath: string, opts?: WALOptions) {
    this.logger = opts?.logger || this.logger;
    this.maxSegmentSize = opts?.maxSegmentSize || this.maxSegmentSize;
    // this.onSync = opts?.onSync || this.onSync;
    this.metaManagerOpts = opts?.meta;
    // this.syncDelay = opts?.syncDelay || this.syncDelay;
  }

  async init(): Promise<void> {
    if (this._isInitialized) {
      return;
    }

    this.logger("debug", `Initializing WAL at ${this.walFilePath}`);

    await this.loadOrCreateMetaFileManager();

    const segments = await this.loadSegmentFilesNames();
    if (segments.length === 0) {
      this.logger("debug", `No segments found in WAL at ${this.walFilePath}`);
      this._isInitialized = true;

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
      `Loaded last segment in WAL at ${this.walFilePath}: ${lastSegment}, index: ${this.metaManager.lastIndex}`,
      {
        last_segment: lastSegment,
        last_index: this.metaManager.lastIndex,
        segments: segments,
      },
    );

    this._isInitialized = true;
  }

  /**
   * Write a new entry to the WAL.
   * @param {IEntry} entry
   * @returns {Promise<number>} The index of the new entry.
   */
  public async write(entry: IEntry): Promise<number> {
    // Serialize the new WAL entry first into a buffer and then flush it with a
    // single write operation to disk.
    const encodedEntry = entry.encode();
    const checksum = crc32.buf(encodedEntry) >>> 0;

    const p = new Promise<void>((resolve) => {
      this.syncWaiters.push(resolve);
    });

    const index = await this.writeLock.runExclusive(() => this.doWrite(entry.type(), checksum, encodedEntry));

    await p;

    return index;
  }

  /**
   * Commit marks the entry at the given index as committed.
   * @param {number} index
   */
  public async commit(index: number): Promise<void> {
    await this.metaManager.commit(index);
  }

  /**
    Close gracefully shuts down the writeAheadLog by making sure that all pending
    writes are completed and synced to disk before then closing the WAL segment file.
    Any future writes after the WAL has been closed will lead to an error.
   */
  public async close(): Promise<void> {
    await this.writeLock.runExclusive(async () => {
      this.isClosed = true;

      if (!this._isInitialized) {
        return;
      }

      if (this.currSegmentWriter !== null) {
        await this.sync();

        this.logger("debug", `Closing WAL at ${this.walFilePath}`);

        await this.currSegmentWriter.close();

        this.currSegmentWriter = null;
      }

      await this.metaManager.close();

      this._isInitialized = false;
    });
  }

  public async recover(handler?: (index: number, entry: IEntry) => Promise<boolean>): Promise<void> {
    if (this.isClosed) {
      throw new Error("WAL is closed");
    }

    if (!handler) {
      handler = async () => false;
    }

    if (this.metaManager.commitIndex === -1 || this.metaManager.commitIndex === this.metaManager.lastIndex) {
      return;
    }

    // start recovery from index commitIndex + 1
    const startIndex = this.metaManager.commitIndex + 1;

    for (let i = startIndex; i <= this.metaManager.lastIndex; i++) {
      const entry = await this.getEntry(i);
      const shallCommit = await handler(i, entry);

      if (shallCommit) {
        await this.metaManager.commit(i);
      } else {
        await this.truncate(i);
        return;
      }
    }
  }

  public async getEntry(index: number): Promise<IEntry> {
    const pos = await this.metaManager.position(index);
    const segment = await fs.open(`${this.walFilePath}/${pos.segmentID}.wal`, "r");
    const reader = new SegmentReader(segment);
    const entry = await reader.readOffset(pos.offset);

    await segment.close();

    return entry;
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

  private async loadOrCreateMetaFileManager(): Promise<void> {
    const metaFilePath = path.join(this.walFilePath, "index.META");

    const doesExists = await checkFileExists(metaFilePath);
    if (!doesExists) {
      this.logger("debug", "Creating new meta file.");
      this.metaManager = await MetaFileManager.create(metaFilePath);

      return;
    }

    this.logger("debug", "Loading existing meta file.");

    this.metaManager = new MetaFileManager(await fs.open(metaFilePath, "r+"), this.metaManagerOpts);

    await this.metaManager.init();
  }

  private async openSegmentFile(segment: string): Promise<SegmentWriter> {
    const file = await fs.open(`${this.walFilePath}/${segment}`, "r+");

    // seek to the end of the file
    // const segmentReader = new SegmentReader(file);
    // this.lastOffset = await segmentReader.seekEnd();

    return new SegmentWriter(file);
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

    const newIndex = this.metaManager.head;

    this.logger("debug", "Writing WAL entry", {
      entry: payload.toString(),
      newIndex,
      type,
      checksum,
      segmentID: this.currSegmentID,
    });

    // Write the entry to the segment file.
    const offset = await this.currSegmentWriter.write(newIndex, type, checksum, payload);
    await this.metaManager.write(this.currSegmentID, offset);
    // this.lastOffset = offset;

    this.scheduleSync();

    // await this.sync();

    return newIndex;
  }

  private async rollSegmentIfNeeded(): Promise<void> {
    if (this.currSegmentWriter !== null && this.currSegmentWriter.size < this.maxSegmentSize) {
      return;
    }

    // create new segment
    this.currSegmentID += 1;

    if (this.currSegmentWriter !== null) {
      // Sync all pending writes before rolling over to a new segment.
      await this.currSegmentWriter.close();
    }

    const segmentFilename = path.join(this.walFilePath, `${this.currSegmentID}.wal`);
    this.currSegmentFile = await fs.open(segmentFilename, "a+");

    this.logger("debug", "Rolling over to new WAL segment", {
      segmentID: this.currSegmentID,
      maxSegmentSize: this.maxSegmentSize,
    });

    this.currSegmentWriter = new SegmentWriter(this.currSegmentFile);

    return;
  }

  private async truncate(fromLogIndex: number): Promise<void> {
    const pos = await this.metaManager.position(fromLogIndex);
    await this.metaManager.truncate(fromLogIndex);
    // truncate the segment file and remove the rest of the segments if any.

    if (pos.segmentID === this.currSegmentID) {
      await this.currSegmentFile.truncate(pos.offset);

      return;
    }

    // remove the current segment file.
    await this.currSegmentWriter.close();

    await Promise.all(
      Array.from({ length: this.currSegmentID - pos.segmentID }, (_, i) => {
        return fs.unlink(`${this.walFilePath}/${pos.segmentID + i + 1}.wal`);
      }),
    );

    this.currSegmentID = pos.segmentID;

    // reset the current segment writer
    this.currSegmentFile = await fs.open(`${this.walFilePath}/${this.currSegmentID}.wal`, "r+");
    await this.currSegmentFile.truncate(pos.offset);

    this.currSegmentWriter = new SegmentWriter(this.currSegmentFile);
    // truncate the segment file to the given offset

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

    // this.emitter.emit("sync");
  }

  private scheduleSync(): void {
    if (!!this.isSyncOngoing) {
      return;
    }
    this.isSyncOngoing = true;

    if (this.syncDelay > 0) {
      setTimeout(() => {
        this.writeLock.runExclusive(async () => {
          await this.sync();
          this.isSyncOngoing = false;
        });
      }, this.syncDelay);

      return;
    }

    this.writeLock.runExclusive(async () => {
      await this.sync();
      this.isSyncOngoing = false;
    });
  }

  getCurrentSegmentID(): number {
    return this.currSegmentID;
  }

  getLastIndex(): number {
    return this.metaManager.head - 1;
  }

  getNextIndex(): number {
    return this.metaManager.head;
  }

  get commitIndex(): number {
    return this.metaManager.commitIndex;
  }

  isCommitted(index: number): boolean {
    return this.metaManager.isCommitted(index);
  }
}

async function checkFileExists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}
