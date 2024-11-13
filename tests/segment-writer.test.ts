import { readFileSync, unlinkSync } from "fs";
import path from "path";
import { SegmentWriter } from "../lib/segment-writer";

import { createHash } from "crypto";
import { open } from "fs/promises";
import { createRandomString } from "./utils";

describe("Test segment writer ops", () => {
  const files: string[] = [];

  afterAll(() => {
    for (const file of files) {
      unlinkSync(file);
    }
  });

  it("Should write single line to segment", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.wal`);
    files.push(filePath);
    const file = await open(filePath, "a+");
    const writer = new SegmentWriter(file);
    const offset = 0;
    const typ = 0;
    const checksum = 0;
    const payload = Buffer.from("test");

    await writer.write(offset, typ, checksum, payload);

    await writer.sync();

    const content = readFileSync(filePath);
    const expected = Buffer.concat([Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 0]), payload]);

    expect(content).toEqual(expected);

    await writer.close();
  });

  it("Should write multiple lines to segment", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.wal`);
    files.push(filePath);
    const file = await open(filePath, "a+");
    const writer = new SegmentWriter(file);
    const offset = 0;
    const typ = 0;
    const checksum = 0;
    const payload1 = Buffer.from("test1");
    const payload2 = Buffer.from("test2");
    const payload3 = Buffer.from("test4");

    await writer.write(offset, typ, checksum, payload1);
    await writer.write(offset + 1, typ, checksum, payload2);
    await writer.write(offset + 2, typ, checksum, payload3);

    await writer.sync();

    const content = readFileSync(filePath);
    const expected = Buffer.concat([
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 0]),
      payload1,
      Buffer.from([0, 0, 0, 1, 0, 0, 0, 0, 0]),
      payload2,
      Buffer.from([0, 0, 0, 2, 0, 0, 0, 0, 0]),
      payload3,
    ]);

    expect(content).toEqual(expected);

    await writer.close();
  });

  it("Test correct file size", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.wal`);
    files.push(filePath);
    const file = await open(filePath, "a+");
    const writer = new SegmentWriter(file);
    const offset = 0;
    const typ = 0;
    const checksum = 0;
    const payload1 = Buffer.from("test1");
    const payload2 = Buffer.from("test2");
    const payload3 = Buffer.from("test4");

    await writer.write(offset, typ, checksum, payload1);
    await writer.write(offset + 1, typ, checksum, payload2);
    await writer.write(offset + 2, typ, checksum, payload3);

    await writer.sync();

    expect(writer.size).toBe(9 + payload1.length + 9 + payload2.length + 9 + payload3.length);

    const stat = readFileSync(filePath);
    expect(stat.length).toBe(writer.size);

    await writer.close();
  });

  it("Test close - should flush", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.wal`);
    files.push(filePath);
    const file = await open(filePath, "a+");
    const writer = new SegmentWriter(file);
    const offset = 0;
    const typ = 0;
    const checksum = 0;
    const payload1 = Buffer.from("test1");
    const payload2 = Buffer.from("test2");
    const payload3 = Buffer.from("test4");

    await writer.write(offset, typ, checksum, payload1);
    await writer.write(offset + 1, typ, checksum, payload2);
    await writer.write(offset + 2, typ, checksum, payload3);

    await writer.close();

    const expected = Buffer.concat([
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 0]),
      payload1,
      Buffer.from([0, 0, 0, 1, 0, 0, 0, 0, 0]),
      payload2,
      Buffer.from([0, 0, 0, 2, 0, 0, 0, 0, 0]),
      payload3,
    ]);

    const content = readFileSync(filePath);
    expect(content).toEqual(expected);
  });

  it("Write large number of entries.", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.wal`);
    files.push(filePath);
    const file = await open(filePath, "a+");
    const writer = new SegmentWriter(file);
    const typ = 0;
    const checksum = 0;

    for (let i = 0; i < 1000; i++) {
      await writer.write(i, typ, checksum, Buffer.from("test" + i));
    }

    await writer.sync();

    const content = readFileSync(filePath, {
      flag: "r",
    });
    const expected = Buffer.concat(
      Array.from({ length: 1000 }, (_, i) => {
        const header = Buffer.alloc(9);

        header.writeUInt32BE(i, 0);
        header.writeUInt8(typ, 4);
        header.writeUInt32BE(checksum, 5);
        return Buffer.concat([header, Buffer.from("test" + i)]);
      }),
    );

    // writeFileSync(path.join(__dirname, "expected.wal"), expected);
    // const expectedFile = readFileSync(path.join(__dirname, "expected.wal"));

    // for (let i = 0; i < expectedFile.length; i++) {
    //   if (expectedFile[i] !== content[i]) {
    //     console.log(expectedFile.length);
    //     console.log(content.length);
    //     console.log(content.subarray(0, 100));
    //     console.log(expectedFile.subarray(0, 100));
    //     console.log(writer.size);

    //     console.log("Mismatch at byte", i);
    //     console.log("Expected:", expectedFile[i]);
    //     console.log("Actual:", content[i]);
    //     break;
    //   }
    // }

    // Compare hashes instead of buffers
    const fileHash = createHash("sha1").update(content).digest("hex");
    const expectedHash = createHash("sha1").update(expected).digest("hex");
    expect(fileHash).toBe(expectedHash);

    await writer.close();
  });
});
