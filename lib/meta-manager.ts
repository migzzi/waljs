import { FileHandle, open } from "fs/promises";

const META_FILE_MARKER = "META";
const INT_SIZE = 4;

const META_FILE_MARKER_OFFSET = 0;
const META_FILE_BASE_OFFSET = 4;
const META_FILE_HEAD_OFFSET = 8;
const META_FILE_COMMIT_OFFSET = 12;

const HEADER_SIZE = 16;
const INDEX_SIZE = 8;
/**
   MetaFileManager is responsible for managing the meta file of the WAL.
  
    Layout:      
        Every Entry is written, using the following binary layout (big endian format):
    
          ┌──────────────────┬───────────┬───────────┬─────────────┬───────────────────┐
          │ File Marker (4B) │ Base (4B) │ Head (4B) │ Commit (4B) │ ...Index (8B)     │
          └──────────────────┴───────────┴───────────┴─────────────┴───────────────────┘
    
        - File Marker = 32bit - WAL entry number for each record in order to implement a low-water mark
        - Base        = 32Bit - The base offset of the indexes. 0 most of the time. Base is only meaningful when indexes are split across many meta files so are a placeholder for future work.
        - Head        = 32bit - The next offset to be written to the WAL
        - Commit      = 32bit - The offset that has been committed to disk
        - Index       = 68bit[] - The index entries for the WAL. Each index entry is 8 bytes long. The first 4 bytes are the segment ID and the second 4 bytes are the offset in the segment.

    Example (octets)...

                Base      Head        Commit      Index 0                ...  Index 200
                v           v           v           V                         v
    49 44 58 24 00 00 00 00 00 00 00 02 00 00 00 01 00 00 00 01 00 00 00 ...  00 00 00 03 00 00 00 02 1f

    Entry at index 0: Segment ID = 1, Offset = 0
    Entry at index 200: Segment ID = 3, Offset = 543

    
 */
export class MetaFileManager {
  private metaFile: FileHandle;
  private header: Buffer = null;

  private updatesQueue: Buffer[] = null;
  private updatesQueueFileOffset = 0;
  private isBufferingEnabled = true;
  private maxBufferSize = 1024;
  private autoSyncInterval = 1000;
  private syncTimer: NodeJS.Timeout = null;

  constructor(metaFile: FileHandle, opts?: MetaManagerOptions) {
    this.metaFile = metaFile;
    this.isBufferingEnabled = opts?.bufferingEnabled ?? true;

    if (this.isBufferingEnabled) {
      this.updatesQueue = [];
      this.maxBufferSize = opts?.maxBufferSize ?? 1024;
      this.autoSyncInterval = opts?.autoSyncInterval ?? 1000;

      this.startAutoSync();
    }
  }

  async init(): Promise<void> {
    if (this.header !== null) {
      // File already opened.
      return;
    }

    const header = Buffer.alloc(HEADER_SIZE);
    await this.metaFile.read(header, 0, HEADER_SIZE, META_FILE_MARKER_OFFSET);

    const marker = header.toString("ascii", META_FILE_MARKER_OFFSET, META_FILE_BASE_OFFSET);
    if (marker !== META_FILE_MARKER) {
      throw new Error(`Invalid meta file marker. Expected ${META_FILE_MARKER}, got ${this.marker}`);
    }

    this.header = header;
  }

  async close(): Promise<void> {
    if (this.header === null) {
      // File already closed.
      return;
    }

    if (this.syncTimer) {
      clearInterval(this.syncTimer);
    }

    await this.sync();
    await this.metaFile.close();

    this.header = null;
  }

  get marker(): string {
    return this.header.toString("ascii", META_FILE_MARKER_OFFSET, META_FILE_BASE_OFFSET);
  }

  get base(): number {
    return this.header.readUInt32BE(META_FILE_BASE_OFFSET);
  }

  get head(): number {
    return this.header.readUInt32BE(META_FILE_HEAD_OFFSET);
  }

  get lastIndex(): number {
    return this.head - 1;
  }

  get commitIndex(): number {
    return this.header.readInt32BE(META_FILE_COMMIT_OFFSET);
  }

  isCommitted(index: number): boolean {
    return index <= this.commitIndex;
  }

  async commit(index: number): Promise<number> {
    if (index < this.commitIndex + 1) {
      // Already committed
      return this.commitIndex;
    }

    if (index !== this.commitIndex + 1) {
      // Out of order commit
      throw new Error(`Out of order commit. Expected ${this.commitIndex + 1}, got ${index}`);
    }

    this.header.writeInt32BE(index, META_FILE_COMMIT_OFFSET);
    if (!this.isBufferingEnabled) {
      await this.metaFile.write(this.header, META_FILE_COMMIT_OFFSET, INT_SIZE, META_FILE_COMMIT_OFFSET);
    }

    return index;
  }

  async position(logIndex: number): Promise<{
    segmentID: number;
    offset: number;
    // length: number;
  }> {
    if (logIndex >= this.head) {
      throw new Error(`Invalid log offset ${logIndex}. Out of bounds`);
    }

    await this.sync();

    const index = this.localIndex(logIndex);
    const indexFileOffset = HEADER_SIZE + index * INDEX_SIZE;
    const indexBuffer = Buffer.alloc(INDEX_SIZE);

    // Read 2 indexes starting from the indexFileOffset.
    await this.metaFile.read(indexBuffer, 0, indexBuffer.length, indexFileOffset);

    // console.log(indexBuffer);

    const segmentID = indexBuffer.readUInt32BE(0);
    const offset = indexBuffer.readUInt32BE(INT_SIZE);
    // const length = indexBuffer.readUInt32BE(INDEX_SIZE + INT_SIZE) - offset;

    return {
      segmentID,
      offset,
      //   length,
    };
  }

  async truncate(fromLogIndex: number): Promise<void> {
    if (fromLogIndex <= this.commitIndex) {
      throw new Error(`Invalid log offset ${fromLogIndex}. Can't truncate committed entries`);
    }

    if (fromLogIndex >= this.head) {
      throw new Error(`Invalid log offset ${fromLogIndex}. Out of bounds`);
    }

    this.header.writeUInt32BE(fromLogIndex, META_FILE_HEAD_OFFSET);

    await this.metaFile.write(this.header, META_FILE_HEAD_OFFSET, INT_SIZE, META_FILE_HEAD_OFFSET);
  }

  async write(segmentID: number, offset: number): Promise<number> {
    const index = this.localIndex(this.head);
    const indexFileOffset = HEADER_SIZE + index * INDEX_SIZE;
    const currentHead = this.head;
    const indexBuffer = Buffer.alloc(INDEX_SIZE);

    indexBuffer.writeUInt32BE(segmentID, 0);
    indexBuffer.writeUInt32BE(offset, INT_SIZE);

    if (this.isBufferingEnabled) {
      if (this.updatesQueue.length === 0) {
        this.updatesQueueFileOffset = indexFileOffset;
      }

      this.updatesQueue.push(indexBuffer);
    } else {
      await this.metaFile.write(indexBuffer, 0, INDEX_SIZE, indexFileOffset);
    }

    this.header.writeUInt32BE(this.head + 1, META_FILE_HEAD_OFFSET);

    if (!this.isBufferingEnabled) {
      await this.metaFile.write(this.header, META_FILE_HEAD_OFFSET, INT_SIZE, META_FILE_HEAD_OFFSET);
    }

    if (this.isBufferingEnabled && this.updatesQueue.length >= this.maxBufferSize) {
      await this.sync();
    }

    return currentHead;
  }

  async sync(): Promise<void> {
    await this.metaFile.write(this.header, META_FILE_HEAD_OFFSET, 2 * INT_SIZE, META_FILE_HEAD_OFFSET);

    if (!this.updatesQueue || this.updatesQueue.length === 0) {
      return;
    }

    const buffer = Buffer.concat(this.updatesQueue);

    await this.metaFile.write(buffer, 0, buffer.length, this.updatesQueueFileOffset);
    // await this.metaFile.write(this.header, META_FILE_COMMIT_OFFSET, INT_SIZE, META_FILE_COMMIT_OFFSET);

    this.updatesQueue = [];
    this.updatesQueueFileOffset = 0;
  }

  private localIndex(logOffset: number): number {
    return logOffset - this.base;
  }

  private startAutoSync(): void {
    this.syncTimer = setInterval(() => {
      this.sync();
    }, this.autoSyncInterval);
  }

  static async create(
    filePath: string,
    opts?: {
      bufferingEnabled?: boolean;
      maxBufferSize?: number;
    },
  ): Promise<MetaFileManager> {
    const metaFile = await open(filePath, "w+");
    const metaFileManager = new MetaFileManager(metaFile, opts);

    const header = Buffer.alloc(HEADER_SIZE);
    await metaFile.read(header, 0, HEADER_SIZE, 0);

    if (header.toString("ascii", META_FILE_MARKER_OFFSET, META_FILE_BASE_OFFSET) !== META_FILE_MARKER) {
      // New file. Write the header.
      header.write(META_FILE_MARKER, META_FILE_MARKER_OFFSET);
      header.writeUInt32BE(0, META_FILE_BASE_OFFSET);
      header.writeUInt32BE(0, META_FILE_HEAD_OFFSET);
      header.writeInt32BE(-1, META_FILE_COMMIT_OFFSET);

      await metaFile.write(header, 0, HEADER_SIZE, 0);
    }

    metaFileManager.header = header;

    return metaFileManager;
  }
}

export type MetaManagerOptions = {
  bufferingEnabled?: boolean;
  maxBufferSize?: number;
  autoSyncInterval?: number;
};
