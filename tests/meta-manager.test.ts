import { readFileSync, unlinkSync } from "fs";
import path from "path";

import { MetaFileManager } from "../lib/meta-manager";
import { createRandomString } from "./utils";

describe("Meta file manager ops", () => {
  const files: string[] = [];

  afterAll(() => {
    for (const file of files) {
      unlinkSync(file);
    }
  });

  describe("With buffering enabled", () => {
    it("Should write multiple with correct header", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

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
        Buffer.from([0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 1]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 2]),
      ]);

      await meta.sync();
      const content = readFileSync(filePath);

      expect(content).toEqual(expectedContent);

      await meta.close();
    });

    it("Should write and commit multiple with correct header", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

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
        Buffer.from([0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 20]),
      ]);

      await meta.sync();

      const content = readFileSync(filePath);

      expect(content).toEqual(expectedContent);

      await meta.close();
    });

    it("Should throw when committing out of order", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

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
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

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
        Buffer.from([0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 20]),
      ]);

      await meta.sync();

      const content = readFileSync(filePath);

      expect(content).toEqual(expectedContent);

      await meta.close();
    });

    it("Should throw when getting position of out of bounds index", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

      await meta.write(0, 0);
      await meta.commit(0);

      await meta.write(0, 10);
      await meta.commit(1);

      await meta.write(0, 20);
      await meta.commit(2);

      expect(meta.position(3)).rejects.toThrowError("Invalid log offset 3. Out of bounds");

      await meta.close();
    });

    it("Should contain correct data after re-open", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      let meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

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

      meta = new MetaFileManager(filePath);
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
        Buffer.from([0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 20]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 30]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 40]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 50]),
      ]);

      await meta.sync();

      const content = readFileSync(filePath);

      expect(content).toEqual(expectedContent);

      await meta.close();
    });

    it("Should truncate", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

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
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

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
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

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

    it("Should write indexes on multiple segments", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

      await meta.write(0, 0);
      await meta.write(0, 10);
      await meta.write(0, 20);

      await meta.commit(0);
      await meta.commit(1);

      expect(meta.segmentID).toEqual(0);

      // Write to new segment
      await meta.write(1, 0);
      await meta.write(1, 10);
      await meta.write(1, 20);

      expect(meta.segmentID).toEqual(1);

      let commitBuffer = Buffer.alloc(4);
      commitBuffer.writeInt32BE(1);

      let expectedContent = Buffer.concat([
        Buffer.from("META"),
        Buffer.from([0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 6]),
        commitBuffer,
        Buffer.from([0, 0, 0, 1]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 20]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 20]),
      ]);

      await meta.sync();

      let content = readFileSync(filePath);

      expect(content).toEqual(expectedContent);

      // Commit more indexes
      await meta.commit(2);
      await meta.commit(3);
      await meta.commit(4);

      commitBuffer = Buffer.alloc(4);
      commitBuffer.writeInt32BE(4);

      expectedContent = Buffer.concat([
        Buffer.from("META"),
        Buffer.from([0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 6]),
        commitBuffer,
        Buffer.from([0, 0, 0, 1]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 20]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 20]),
      ]);

      expect(meta.commitIndex).toEqual(4);

      await meta.sync();

      content = readFileSync(filePath);

      expect(content).toEqual(expectedContent);

      await meta.close();
    });

    it("Should throw when writing to an older segment", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

      await meta.write(0, 0);
      await meta.write(0, 10);
      await meta.write(0, 20);

      await meta.commit(0);
      await meta.commit(1);

      expect(meta.segmentID).toEqual(0);

      // Write to new segment
      await meta.write(1, 0);
      await meta.write(1, 10);
      await meta.write(1, 20);

      expect(meta.write(0, 30)).rejects.toThrowError("Invalid segment ID 0. Out of order");

      await meta.close();
    });

    it("Should compact meta file", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: true,
      });

      let currIndex = 0;

      // Write to segment 0
      for (let i = 0; i < 100; i++, currIndex++) {
        await meta.write(0, i * 10);
        await meta.commit(currIndex);
      }

      // Write to segment 1
      for (let i = 0; i < 100; i++, currIndex++) {
        await meta.write(1, i * 10);
        await meta.commit(currIndex);
      }

      // Write to segment 2
      for (let i = 0; i < 100; i++, currIndex++) {
        await meta.write(2, i * 10);
        // Commit first 50 indexes
        if (i < 50) {
          await meta.commit(currIndex);
        }
      }

      expect(meta.base).toEqual(0);
      expect(meta.segmentID).toEqual(2);

      const oldHead = meta.head;
      const oldCommitIndex = meta.commitIndex;

      await meta.compact();

      expect(meta.base).toBe(oldCommitIndex + 1);
      expect(meta.segmentID).toEqual(2);
      expect(meta.head).toBe(oldHead);

      const commitBuffer = Buffer.alloc(4);
      commitBuffer.writeInt32BE(oldCommitIndex);

      const headBuffer = Buffer.alloc(4);
      headBuffer.writeInt32BE(oldHead);

      const baseBuffer = Buffer.alloc(4);
      baseBuffer.writeInt32BE(oldCommitIndex + 1);

      const buffers = [Buffer.from("META"), baseBuffer, headBuffer, commitBuffer, Buffer.from([0, 0, 0, 2])];

      for (let i = 50; i < 100; i++) {
        const buffer = Buffer.alloc(8);
        buffer.writeUInt32BE(2);
        buffer.writeUInt32BE(i * 10, 4);
        buffers.push(buffer);
      }

      await meta.sync();
      const content = readFileSync(filePath);

      expect(content).toEqual(Buffer.concat(buffers));

      await meta.close();
    });
  });

  describe("With buffering disabled", () => {
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
        Buffer.from([0, 0, 0, 0]),
      ]);

      const content = readFileSync(filePath);

      expect(content).toEqual(expectedContent);

      await meta.close();
    });

    it("Should write multiple with correct header", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

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
        Buffer.from([0, 0, 0, 0]),
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
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

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
        Buffer.from([0, 0, 0, 0]),
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
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

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
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

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
        Buffer.from([0, 0, 0, 0]),
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
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

      await meta.write(0, 0);
      await meta.commit(0);

      await meta.write(0, 10);
      await meta.commit(1);

      await meta.write(0, 20);
      await meta.commit(2);

      expect(meta.position(3)).rejects.toThrowError("Invalid log offset 3. Out of bounds");

      await meta.close();
    });

    it("Should contain correct data after re-open", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      let meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

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

      meta = new MetaFileManager(filePath);
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
        Buffer.from([0, 0, 0, 0]),
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
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

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
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

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
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

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

    it("Should write indexes on multiple segments", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

      await meta.write(0, 0);
      await meta.write(0, 10);
      await meta.write(0, 20);

      await meta.commit(0);
      await meta.commit(1);

      expect(meta.segmentID).toEqual(0);

      // Write to new segment
      await meta.write(1, 0);
      await meta.write(1, 10);
      await meta.write(1, 20);

      expect(meta.segmentID).toEqual(1);

      let commitBuffer = Buffer.alloc(4);
      commitBuffer.writeInt32BE(1);

      let expectedContent = Buffer.concat([
        Buffer.from("META"),
        Buffer.from([0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 6]),
        commitBuffer,
        Buffer.from([0, 0, 0, 1]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 20]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 20]),
      ]);

      await meta.sync();

      let content = readFileSync(filePath);

      expect(content).toEqual(expectedContent);

      // Commit more indexes
      await meta.commit(2);
      await meta.commit(3);
      await meta.commit(4);

      commitBuffer = Buffer.alloc(4);
      commitBuffer.writeInt32BE(4);

      expectedContent = Buffer.concat([
        Buffer.from("META"),
        Buffer.from([0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 6]),
        commitBuffer,
        Buffer.from([0, 0, 0, 1]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 0, 0, 0, 0, 20]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 0]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 10]),
        Buffer.from([0, 0, 0, 1, 0, 0, 0, 20]),
      ]);

      expect(meta.commitIndex).toEqual(4);

      await meta.sync();

      content = readFileSync(filePath);

      expect(content).toEqual(expectedContent);

      await meta.close();
    });

    it("Should throw when writing to an older segment", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

      await meta.write(0, 0);
      await meta.write(0, 10);
      await meta.write(0, 20);

      await meta.commit(0);
      await meta.commit(1);

      expect(meta.segmentID).toEqual(0);

      // Write to new segment
      await meta.write(1, 0);
      await meta.write(1, 10);
      await meta.write(1, 20);

      expect(meta.write(0, 30)).rejects.toThrowError("Invalid segment ID 0. Out of order");

      await meta.close();
    });

    it("Should compact meta file", async () => {
      const randomName = createRandomString(10);
      const filePath = path.join(__dirname, `${randomName}.meta`);
      files.push(filePath);
      const meta = await MetaFileManager.create(filePath, {
        bufferingEnabled: false,
      });

      let currIndex = 0;

      // Write to segment 0
      for (let i = 0; i < 100; i++, currIndex++) {
        await meta.write(0, i * 10);
        await meta.commit(currIndex);
      }

      // Write to segment 1
      for (let i = 0; i < 100; i++, currIndex++) {
        await meta.write(1, i * 10);
        await meta.commit(currIndex);
      }

      // Write to segment 2
      for (let i = 0; i < 100; i++, currIndex++) {
        await meta.write(2, i * 10);
        // Commit first 50 indexes
        if (i < 50) {
          await meta.commit(currIndex);
        }
      }

      expect(meta.base).toEqual(0);
      expect(meta.segmentID).toEqual(2);

      const oldHead = meta.head;
      const oldCommitIndex = meta.commitIndex;

      await meta.compact();

      expect(meta.base).toBe(oldCommitIndex + 1);
      expect(meta.segmentID).toEqual(2);
      expect(meta.head).toBe(oldHead);

      const commitBuffer = Buffer.alloc(4);
      commitBuffer.writeInt32BE(oldCommitIndex);

      const headBuffer = Buffer.alloc(4);
      headBuffer.writeInt32BE(oldHead);

      const baseBuffer = Buffer.alloc(4);
      baseBuffer.writeInt32BE(oldCommitIndex + 1);

      const buffers = [Buffer.from("META"), baseBuffer, headBuffer, commitBuffer, Buffer.from([0, 0, 0, 2])];

      for (let i = 50; i < 100; i++) {
        const buffer = Buffer.alloc(8);
        buffer.writeUInt32BE(2);
        buffer.writeUInt32BE(i * 10, 4);
        buffers.push(buffer);
      }

      await meta.sync();
      const content = readFileSync(filePath);

      expect(content).toEqual(Buffer.concat(buffers));

      await meta.close();
    });
  });
});
