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
  - [How it works](#how-it-works)
  - [Benchmarks](#benchmarks)
    - [WAL write](#wal-write)
  - [Contributing](#contributing)
  - [Versioning](#versioning)

## Installation

Just simply run the following command

```sh
npm install --save waljs
```

, Or if you're using yarn

```sh
yarn add waljs
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

Use `write` to write an entry to the wal.

```ts
await wal.write(new EntryExample1(data1));
await wal.write(new EntryExample1(data2));
await wal.write(new EntryExample2(data3));
```

When you're done using the WAL, you can stop it using the following call.

```ts
await wal.close();
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

| name                       | ops     | margin | percentSlower |
| -------------------------- | ------- | ------ | ------------- |
| WAL write with sync        | 55607   | 4.7    | 0             |

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and on the process for submitting pull requests to this repository.

## Versioning

THIS SOFTWARE IS STILL IN ALPHA AND THERE ARE NO GUARANTEES REGARDING API STABILITY YET.

All significant (e.g. breaking) changes are documented in the [CHANGELOG.md](CHANGELOG.md).

After the v1.0 release we plan to use SemVer for versioning. For the versions available, see the releases page.
