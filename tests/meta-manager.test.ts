import { readFileSync, unlinkSync } from "fs";
import path from "path";

import { MetaFileManager } from "../lib/meta-manager";
import { createRandomString } from "./utils";
import { open } from "fs/promises";

describe("Meta file manager ops", () => {
  const files: string[] = [];

  afterAll(() => {
    for (const file of files) {
      unlinkSync(file);
    }
  });

  it("Should create meta file", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.meta`);
    files.push(filePath);
    const meta = await MetaFileManager.create(filePath);

    expect(meta.head).toEqual(0);
    expect(meta.commitIndex).toEqual(-1);
    expect(meta.marker).toEqual("META");

    const commitBuffer = Buffer.alloc(4);
    commitBuffer.writeInt32BE(-1);

    const expectedContent = Buffer.concat([
      Buffer.from("META"),
      Buffer.from([0, 0, 0, 0]),
      Buffer.from([0, 0, 0, 0]),
      commitBuffer,
    ]);

    const content = readFileSync(filePath);

    expect(content).toEqual(expectedContent);

    await meta.close();
  });

  it("Should write multiple with correct header", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.meta`);
    files.push(filePath);
    const meta = await MetaFileManager.create(filePath);

    await meta.write(0, 0);
    await meta.write(0, 1);
    await meta.write(0, 2);

    expect(meta.head).toEqual(3);
    expect(meta.commitIndex).toEqual(-1);

    const commitBuffer = Buffer.alloc(4);
    commitBuffer.writeInt32BE(-1);

    const expectedContent = Buffer.concat([
      Buffer.from("META"),
      Buffer.from([0, 0, 0, 0]),
      Buffer.from([0, 0, 0, 3]),
      commitBuffer,
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 1]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 2]),
    ]);

    const content = readFileSync(filePath);

    expect(content).toEqual(expectedContent);

    await meta.close();
  });

  it("Should write and commit multiple with correct header", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.meta`);
    files.push(filePath);
    const meta = await MetaFileManager.create(filePath);

    await meta.write(0, 0);
    await meta.commit(0);

    await meta.write(0, 10);
    await meta.commit(1);

    await meta.write(0, 20);

    expect(meta.head).toEqual(3);
    expect(meta.commitIndex).toEqual(1);

    const commitBuffer = Buffer.alloc(4);
    commitBuffer.writeInt32BE(1);

    const expectedContent = Buffer.concat([
      Buffer.from("META"),
      Buffer.from([0, 0, 0, 0]),
      Buffer.from([0, 0, 0, 3]),
      commitBuffer,
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 10]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 20]),
    ]);

    const content = readFileSync(filePath);

    expect(content).toEqual(expectedContent);

    await meta.close();
  });

  it("Should throw when committing out of order", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.meta`);
    files.push(filePath);
    const meta = await MetaFileManager.create(filePath);

    await meta.write(0, 0);
    await meta.write(0, 1);
    await meta.write(0, 2);

    await meta.commit(0);

    expect(meta.commit(2)).rejects.toThrowError("Out of order commit. Expected 1, got 2");

    await meta.close();
  });

  it("Should return correct position", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.meta`);
    files.push(filePath);
    const meta = await MetaFileManager.create(filePath);

    await meta.write(0, 0);
    await meta.commit(0);

    await meta.write(0, 10);
    await meta.commit(1);

    await meta.write(0, 20);
    await meta.commit(2);

    const pos = await meta.position(1);

    expect(pos.segmentID).toEqual(0);
    expect(pos.offset).toEqual(10);

    expect(meta.head).toEqual(3);
    expect(meta.commitIndex).toEqual(2);

    const commitBuffer = Buffer.alloc(4);
    commitBuffer.writeInt32BE(2);

    const expectedContent = Buffer.concat([
      Buffer.from("META"),
      Buffer.from([0, 0, 0, 0]),
      Buffer.from([0, 0, 0, 3]),
      commitBuffer,
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 10]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 20]),
    ]);

    const content = readFileSync(filePath);

    expect(content).toEqual(expectedContent);

    await meta.close();
  });

  it("Should throw when getting position of out of bounds index", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.meta`);
    files.push(filePath);
    const meta = await MetaFileManager.create(filePath);

    await meta.write(0, 0);
    await meta.commit(0);

    await meta.write(0, 10);
    await meta.commit(1);

    await meta.write(0, 20);
    await meta.commit(2);

    expect(meta.position(3)).rejects.toThrowError("Invalid log offset 3. Out of bounds");
  });

  it("Should contain correct data after re-open", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.meta`);
    files.push(filePath);
    let meta = await MetaFileManager.create(filePath);

    await meta.write(0, 0);
    await meta.commit(0);

    await meta.write(0, 10);
    await meta.commit(1);

    await meta.write(0, 20);
    await meta.commit(2);

    await meta.write(0, 30);
    await meta.commit(3);

    await meta.write(0, 40);

    await meta.write(0, 50);

    await meta.close();

    meta = new MetaFileManager(await open(filePath, "r+"));
    await meta.init();

    expect(meta.head).toEqual(6);
    expect(meta.commitIndex).toEqual(3);

    const pos = await meta.position(1);

    expect(pos.segmentID).toEqual(0);
    expect(pos.offset).toEqual(10);

    const commitBuffer = Buffer.alloc(4);
    commitBuffer.writeInt32BE(3);

    const expectedContent = Buffer.concat([
      Buffer.from("META"),
      Buffer.from([0, 0, 0, 0]),
      Buffer.from([0, 0, 0, 6]),
      commitBuffer,
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 10]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 20]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 30]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 40]),
      Buffer.from([0, 0, 0, 0, 0, 0, 0, 50]),
    ]);

    const content = readFileSync(filePath);

    expect(content).toEqual(expectedContent);

    await meta.close();
  });

  it("Should truncate", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.meta`);
    files.push(filePath);
    const meta = await MetaFileManager.create(filePath);

    await meta.write(0, 0);
    await meta.commit(0);

    await meta.write(0, 10);
    await meta.commit(1);

    await meta.write(0, 20);
    await meta.commit(2);

    await meta.write(0, 30);

    await meta.write(0, 40);

    await meta.write(0, 50);

    expect(meta.head).toEqual(6);
    expect(meta.commitIndex).toEqual(2);

    await meta.truncate(3);

    expect(meta.head).toEqual(3);
    expect(meta.commitIndex).toEqual(2);

    await meta.close();
  });

  it("Should throw when truncate committed index", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.meta`);
    files.push(filePath);
    const meta = await MetaFileManager.create(filePath);

    await meta.write(0, 0);
    await meta.commit(0);

    await meta.write(0, 10);
    await meta.commit(1);

    await meta.write(0, 20);
    await meta.commit(2);

    await meta.write(0, 30);

    await meta.write(0, 40);

    await meta.write(0, 50);

    expect(meta.truncate(1)).rejects.toThrowError("Invalid log offset 1. Can't truncate committed entries");

    await meta.close();
  });

  it("Should throw when truncate beyond offset", async () => {
    const randomName = createRandomString(10);
    const filePath = path.join(__dirname, `${randomName}.meta`);
    files.push(filePath);
    const meta = await MetaFileManager.create(filePath);

    await meta.write(0, 0);
    await meta.commit(0);

    await meta.write(0, 10);
    await meta.commit(1);

    await meta.write(0, 20);
    await meta.commit(2);

    await meta.write(0, 30);

    await meta.write(0, 40);

    await meta.write(0, 50);

    expect(meta.truncate(6)).rejects.toThrowError("Invalid log offset 6. Out of bounds");

    await meta.close();
  });
});
