import { FileHandle } from "fs/promises";

// The SegmentWriter is responsible for writing WAL entry records to disk.
// This type handles the necessary buffered I/O as well as file system syncing.
//
// Every Entry is written, using the following binary layout (big endian format):
//
//	  ┌─────────────┬───────────┬──────────┬─────────┐
//	  │ Index (4B)  │ Type (1B) │ CRC (4B) │ Payload │
//	  └─────────────┴───────────┴──────────┴─────────┘
//
//		- Index = 32bit WAL entry number for each record in order to implement a low-water mark
//		- Type = Type of WAL entry
//		- CRC = 32bit hash computed over the payload using CRC
//		- Payload = The actual WAL entry payload data
export class SegmentWriter {
  private _size = 0;
  get size(): number {
    return this._size;
  }

  private writer: BufferedWriter;

  constructor(
    writer: FileHandle,
    private opts?: {
      // The maximum size of a single segment file. Once this size is reached, a new
      // segment file is created.
      maxSegmentSize: number;
      // Buffer size for writing WAL entries. This is used to batch writes to the
      // underlying writer.
      bufferSize: number;
    },
  ) {
    this.opts = opts || { maxSegmentSize: 4 * 1024, bufferSize: 4 * 1024 };
    this.writer = new BufferedWriter(writer, this.opts.bufferSize);
  }

  // Write a new WAL entry.
  //
  // Note, that we do not use the Entry interface here because encoding the
  // payload is done at an earlier stage than actually writing data to the WAL
  // segment.
  async write(index: number, typ: number, checksum: number, payload: Buffer): Promise<number> {
    const header = Buffer.alloc(9);

    header.writeUInt32BE(index, 0);
    header.writeUInt8(typ, 4);
    header.writeUInt32BE(checksum, 5);

    const oldSize = this._size;
    // If the current batch size exceeds the maximum allowed size, we need to
    await this.writer.write(Buffer.concat([header, payload]));

    this._size += 9 + payload.length;

    return oldSize;
  }

  // Sync writes any buffered data to the underlying io.Writer and syncs the file
  // systems in-memory copy of recently written data to disk if we are writing to
  // an os.File.
  async sync(): Promise<void> {
    await this.writer.flush();
  }

  // Close ensures that all buffered data is flushed to disk before and then closes
  // the associated writer or file.
  async close(): Promise<void> {
    await this.writer.close();
  }
}

class BufferedWriter {
  private buffer: Buffer;
  private size = 0;

  constructor(private writer: FileHandle, private bufferSize: number) {
    this.buffer = Buffer.alloc(bufferSize);
  }

  async write(data: Buffer): Promise<void> {
    let remaining = data.length;
    while (remaining > this.available) {
      if (this.size === 0) {
        // Large write. Write directly to the file.
        await this.writer.write(data);
      } else {
        const slice = data.subarray(0, this.available);

        const n = slice.copy(this.buffer, this.size, 0, slice.length);
        this.size += n;
        await this.flush();
        data = data.subarray(n);
        remaining -= n;
      }
    }

    if (remaining > 0) {
      data.copy(this.buffer, this.size, 0, remaining);
      this.size += remaining;
    }

    return;
  }

  async flush(): Promise<void> {
    if (this.size === 0) {
      return;
    }

    const res = await this.writer.write(this.buffer.subarray(0, this.size));
    if (res.bytesWritten !== this.size) {
      throw new Error("Failed to write all data to the file");
    }

    this.size = 0;
  }

  async close(): Promise<void> {
    await this.flush();
    await this.writer.close();
  }

  get available(): number {
    return this.bufferSize - this.size;
  }
}

// export class SegmentWriter {
//   private _size = 0;
//   get size(): number {
//     return this._size;
//   }

//   private currBatchSize = 0;
//   private currBatchIndex = 0;

//   private stream: WriteStream;

//   private corked = 0;

//   constructor(
//     writer: WriteStream,
//     private opts?: {
//       // The maximum size of a single segment file. Once this size is reached, a new
//       // segment file is created.
//       maxSegmentSize: number;
//       // Buffer size for writing WAL entries. This is used to batch writes to the
//       // underlying writer.
//       bufferSize: number;
//     },
//   ) {
//     this.opts = opts || { maxSegmentSize: 4 * 1024, bufferSize: 4 * 1024 };
//     this.stream = writer;
//   }

//   // Write a new WAL entry.
//   //
//   // Note, that we do not use the Entry interface here because encoding the
//   // payload is done at an earlier stage than actually writing data to the WAL
//   // segment.
//   async write(offset: number, typ: number, checksum: number, payload: Buffer): Promise<void> {
//     const header = Buffer.alloc(9);

//     header.writeUInt32BE(offset, 0);
//     header.writeUInt8(typ, 4);
//     header.writeUInt32BE(checksum, 5);

//     this.stream.cork();
//     this.corked++;

//     // console.log("corked", this.corked);

//     // If the current batch size exceeds the maximum allowed size, we need to
//     if (this.currBatchSize + 9 + payload.length > this.opts.bufferSize) {
//       await this.sync();

//       this.currBatchSize = 0;
//       this.currBatchIndex++;
//     }

//     this.stream.write(Buffer.concat([header, payload]));

//     this.currBatchSize += 9 + payload.length;
//     this._size += 9 + payload.length;
//   }

//   // Sync writes any buffered data to the underlying io.Writer and syncs the file
//   // systems in-memory copy of recently written data to disk if we are writing to
//   // an os.File.
//   sync(): Promise<void> {
//     if (this.corked === 0) {
//       return;
//     }

//     return new Promise((resolve) => {
//       const corked = this.corked;
//       this.corked = 0;

//       // console.log("uncorking", corked);

//       for (let i = 0; i < corked; i++) {
//         this.stream.uncork();
//       }

//       setImmediate(resolve);

//       // process.nextTick(() => {
//       //   for (let i = 0; i < corked; i++) {
//       //     this.stream.uncork();
//       //   }
//       //   setImmediate(resolve);
//       // });
//     });
//   }

//   // Close ensures that all buffered data is flushed to disk before and then closes
//   // the associated writer or file.
//   async close(): Promise<void> {
//     await this.sync();
//     await new Promise((resolve) => this.stream.end(resolve));
//     await new Promise((resolve) => this.stream.close(resolve));
//   }
// }
