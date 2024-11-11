import { ReadStream } from "fs";

export type EntryType = number;

// class WALEntry {
//   constructor(
//     private offset: number,
//     private type: EntryType,
//     private checksum: string,
//     private data: Buffer
//   ) {}

//   toString(): string {
//     return this.data.toString();
//   }

//   fromBuffer(buf: Buffer): WALEntry {
//     const offset = buf.readUInt32BE(0);
//     const type = buf.readUInt8(4);
//     const checksum = buf.subarray(5, 9).toString("hex");
//     const data = buf.subarray(9);

//     return new WALEntry(offset, this.typeFromNumber(type), checksum, data);
//   }
// }

export interface IEntry {
  type(): EntryType;

  // encode encodes the payload into the provided buffer. In case the
  // buffer is too small to fit the entire payload, this function can grow the
  // old and return a new slice. Otherwise, the old slice must be returned.
  encode(): Buffer;

  // read reads the payload from the reader but does not yet decode it.
  // Reading and decoding are separate steps for performance reasons. Sometimes
  // we might want to quickly seek through the WAL without having to decode
  // every entry.
  read(r: Reader): Promise<Buffer>;

  // DecodePayload decodes an entry from a payload that has previously been read
  // by read(…).
  decode(buf: Buffer): void;
}

export interface Reader {
  read(size: number, offset?: number): Promise<Buffer>;
}
