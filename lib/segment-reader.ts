import * as crc32 from "crc-32";
import { IEntry, EntryType, Reader } from "./entry";
import { EntryRegistry } from "./entry-registry";
import { ReadStream } from "fs";
import { FileHandle } from "fs/promises";

// The SegmentReader is responsible for reading WAL entries from their binary
// representation, typically from disk. It is used by the WAL to automatically
// resume the last open segment upon startup, but it can also be used to manually
// iterate through WAL segments.

// The complete usage pattern looks like this:

// const r = new SegmentReader(…);
// …

// while (r.readNext()) {
//   const offset = r.offset();
//   …
//   const entry = r.decode();
//   …
// }

// if (r.err()) {
//   …
// }
export class SegmentReader {
  private reader: FileReader;
  private _offset: number;
  private type: EntryType;
  private checksum: number;
  private _entry: IEntry;
  private payload: Buffer;

  constructor(reader: FileHandle) {
    this.reader = new FileReader(reader);
    this._offset = 0;
    this.type = 0;
    this.checksum = 0;
    this._entry = null;
    this.payload = Buffer.alloc(0);
  }

  // SeekEnd reads through the entire segment until the end and returns the last offset.
  async seekEnd(): Promise<number> {
    while (await this.readNext()) {}

    return this._offset;
  }

  // ReadNext loads the data for the next Entry from the underlying reader.
  // For efficiency reasons, this function neither checks the entry checksum,
  // nor does it decode the entry bytes. This is done, so the caller can quickly
  // seek through a WAL up to a specific offset without having to decode each WAL
  // entry.
  //
  // You can get the offset of the current entry using SegmentReader.offset().
  // In order to actually decode the read WAL entry, you need to use SegmentReader.decode(…).
  async readNext(): Promise<boolean> {
    const header = Buffer.alloc(9); // 4B offset + 1B type + 4B checksum
    const res = await this.reader.file.read(header, 0, 9);

    if (res === null || res.bytesRead === 0) {
      return false;
    }

    if (res.bytesRead < 9) {
      throw new Error("Unexpected EOF");
    }

    this._offset = header.readUInt32BE(0);
    this.type = header.readUInt8(4);
    this.checksum = header.readUInt32BE(5);

    this._entry = EntryRegistry.get(this.type);
    if (!this._entry) {
      throw new Error("Invalid entry type");
    }

    this.payload = await this._entry.read(this.reader);
    return true;
  }

  // Decode decodes the last entry that was read using SegmentReader.readNext().
  decode(): IEntry {
    if (!this._entry) {
      throw new Error("Must call SegmentReader.readNext() first");
    }

    if (this.checksum !== crc32.buf(this.payload) >>> 0) {
      throw new Error(
        `Detected WAL Entry corruption at WAL offset ${this.offset}`
      );
    }

    this._entry.decode(this.payload);
    return this._entry;
  }

  // offset returns the offset of the last read entry.
  get offset(): number {
    return this._offset;
  }

  get entry(): IEntry {
    return this._entry;
  }
}

class FileReader implements Reader {
  public file: FileHandle;

  constructor(file: FileHandle) {
    this.file = file;
  }

  async read(size: number, offset?: number): Promise<Buffer> {
    const buf = Buffer.alloc(size);
    await this.file.read(buf, 0, size, offset);

    return buf;
  }
}
