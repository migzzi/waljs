export type EntryType = number;

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
  read(r: Reader, offset?: number): Promise<Buffer>;

  // DecodePayload decodes an entry from a payload that has previously been read
  // by read(…).
  decode(buf: Buffer): void;
}

export interface Reader {
  read(size: number, offset?: number): Promise<Buffer>;
}
