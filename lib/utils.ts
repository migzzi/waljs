import { createReadStream, createWriteStream } from "fs";
import fs from "fs/promises";

export async function moveFile(src: string, dest: string): Promise<void> {
  try {
    await fs.rename(src, dest);
  } catch (err) {
    if (err.code === "EXDEV") {
      // fallback to copy and delete
      await copyFile(src, dest);
      await fs.unlink(src);
    } else {
      throw err;
    }
  }
}

async function copyFile(src: string, dest: string): Promise<void> {
  const readStream = createReadStream(src);
  const writeStream = createWriteStream(dest);

  return new Promise((resolve, reject) => {
    readStream.on("error", reject);
    writeStream.on("error", reject);

    readStream.on("end", resolve);

    readStream.pipe(writeStream);
  });
}

export async function checkFileExists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}
