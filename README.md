# WAL.js

![Status](https://img.shields.io/badge/status-stable-brightgreen.svg?style=flat)
![Type](https://img.shields.io/badge/type-library-orange.svg?style=flat)
![Auther](https://img.shields.io/badge/author-migzzi-informational.svg?style=flat&logo=DarkReader)
![License](https://img.shields.io/badge/license-MIT-green?style=flat)

waljs is an efficient write-ahead log implementation for Node.js.

The main goal of a Write-ahead Log (WAL) is to make the application more durable, so it does not lose data in case of a crash. WALs are used in applications such as database systems to flush all written data to disk before the changes are written to the database. In case of a crash, the WAL enables the application to recover lost in-memory changes by reconstructing all required operations from the log.

## Table of Contents

- [WAL.js](#waljs)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Initialization](#initialization)
  - [Usage](#usage)
    - [Write](#write)
    - [commit](#commit)
    - [Recovery](#recovery)
    - [Compaction](#compaction)
      - [Compact](#compact)
      - [Archive](#archive)
    - [Closing](#closing)
  - [Configuration](#configuration)
  - [How it works](#how-it-works)
  - [Benchmarks](#benchmarks)
    - [WAL write](#wal-write)
  - [Contributing](#contributing)
  - [Versioning](#versioning)
  - [Acknowledgments](#acknowledgments)
  - [What's next?](#whats-next)

## Installation

Just simply run the following command

```sh
npm install --save @zamurai/wal
```

, Or if you're using yarn

```sh
yarn add @zamurai/wal
```

## Initialization

Use the following steps to initialize an instance of a wal.

```ts
// Fist register the entries factory functions
EntryRegistry.register(() => EntryExample1());
EntryRegistry.register(() => EntryExample2());
EntryRegistry.register(() => EntryExample3());

// Initialize the wall
const wal = new WAL(walDirPath);
await wal.init();
```

## Usage

### Write

Use `write` to write an entry to the wal.

```ts
await wal.write(new EntryExample1(data1));
await wal.write(new EntryExample1(data2));
await wal.write(new EntryExample2(data3));
```

### commit

Use `commit`/`commitUpTo` to commit the entries to the wal.

```ts
await wal.commit(index); // Will commit entry at the given index.

// Or, you can use the following to commit all entries up to the given index.
await wal.commitUpTo(index); 
```

### Recovery

You can also recover the WAL using the following call.

```ts
await wal.recover(); // Will remove all uncommitted entries.

// Or you can use the following to recover the WAL and do something with the uncommitted entries.
await wal.recover(async (index, entry): boolean => {
  // Do something with the recovered entry.
  return true; // Return false to stop the recovery process.
}); // Will recover all entries.
```

### Compaction

#### Compact

You can compact the WAL using the following call.

```ts
await wal.compact(); // Will remove all committed entries and segments.
// Method returns a boolean indicating if the compact was done.
```

This will remove all committed entries from the WAL and all dead segments and keep the uncommitted entries.

#### Archive

Or if you don't want to delete older entries and keep them on the side for later you can use the following to archive the WAL into a separate archive directory while keeping the uncommitted entries and active segments in the WAL directory.

```ts
const archived = await wal.archive(archivePath); // Will move all uncommitted entries and segments to the archive directory.
// Method returns a boolean indicating if the archive was done.
```

> [!NOTE]
> This library takes no automatic action to compact/archive the WAL. You need to call these methods manually based on your application's requirements.
> 
> We encourage you to either use the `compact` or `archive` method regularly to keep the WAL size in check.
>


> [!IMPORTANT]
> `archive` and `compact` methods will return boolean indicating if the operation was successful or not.
>
> If the operation was not successful, it means that the WAL is in a state where it cannot be compacted or archived.
> i.e. there are not enough committed entries to compact or there are no **dead segments** *(Segments that has all its entries committed)* to archive.
>
> You can control the minimum number of entries required for compaction using the `minEntriesForCompaction` configuration described [below](#configuration).
> 

### Closing

When you're done using the WAL, you can stop it using the following call.

```ts
await wal.close();
```

## Configuration

```ts
{
  // log function used to log internal messages.
  // Default is NOOP.
  logger?: (level: string, msg: string, attrs?: Record<string, unknown>) => void;

  // The maximum size of a single WAL segment file in bytes. 
  // Default is 10MB.
  maxSegmentSize?: number;

  // The minimum number of committed entries required to be ready for compaction.
  // Default is 1000 entries.
  minEntriesForCompaction?: number;

  // Configuration for metadata file.
  meta?: {
    // If buffering is enabled, the WAL will buffer **METADATA** writes (i.e. head, commitIndex) in memory before writing them to disk. 
    // Note that this does not affect the WAL entries themselves, which are always written to disk immediately.
    // Also note that buffering may cause metadata data loss in case of a crash.
    // Default is true.
    bufferingEnabled?: boolean;

    // The maximum number of the metadata updates buffer.
    // When this size is reached, the WAL will flush the buffer to disk. Even if the autoSyncInterval is not reached.
    // Default 1024 updates.
    maxBufferSize?: number;

    // The interval in milliseconds at which the WAL will sync the metadata to disk. 
    // Default is 1000ms.
    autoSyncInterval?: number;
  };
};
```

## How it works

Each `WAL.write(…)` call creates a binary encoding of the passed `IEntry` which 
we call the entry's _payload_. This payload is written to disk together with some
metadata such as the entry type, a CRC checksum and an offset number.

The full binary layout looks like the following:

[embedmd]:# (lib/segment-writer.ts /.*the following binary layout.*/ /.*- Payload =.*/)
```ts
// Every Entry is written, using the following binary layout (big endian format):
//
//	  ┌─────────────┬───────────┬──────────┬─────────┐
//	  │ Offset (4B) │ Type (1B) │ CRC (4B) │ Payload │
//	  └─────────────┴───────────┴──────────┴─────────┘
//
//		- Offset = 32bit WAL entry number for each record in order to implement a low-water mark
//		- Type = Type of WAL entry
//		- CRC = 32bit hash computed over the payload using CRC
//		- Payload = The actual WAL entry payload data
```

This data is appended to a file and the WAL makes sure that it is actually
written to non-volatile storage rather than just being stored in a memory-based
write cache that would be lost if power failed (see [fsynced][fsync]).

When the WAL file reaches a configurable maximum size, it is closed and the WAL
starts to append its records to a new and empty file. These files are called WAL
_segments_. Typically, the WAL is split into multiple segments to enable other
processes to take care of cleaning old segments, implement WAL segment backups
and more. When the WAL is started, it will resume operation at the end of the
last open segment file.

## Benchmarks

These benchmarks are run on a machine with the following specifications:

- OS: MacOS
- CPU: Apple M1 Pro Chip
- RAM: 16GB
- Node: v18.15.0

### WAL write

<!-- ![Alt text](resources/cache-hit-benchmark-chart.png) -->

| name                       | ops     | margin |
| -------------------------- | ------- | ------ |
| WAL write with sync        | 56317   | ±4.95% |

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and on the process for submitting pull requests to this repository.

## Versioning

THIS SOFTWARE IS STILL IN ALPHA AND THERE ARE NO GUARANTEES REGARDING API STABILITY YET.

All significant (e.g. breaking) changes are documented in the [CHANGELOG.md](CHANGELOG.md).

After the v1.0 release we plan to use SemVer for versioning. For the versions available, see the releases page.

## Acknowledgments

This work was inspired by the [WAL implementation in Go by fgrosse](https://github.com/fgrosse/wal).

## What's next?

- [x] Add support for WAL Recovery
- [x] Add support for WAL compaction
- [ ] Add support for WAL compression
- [ ] Add support for WAL encryption
