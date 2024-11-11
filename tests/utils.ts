import { IEntry, Reader } from "../lib/entry";

export class TextEntry implements IEntry {
  public length: number;
  public content: string;

  static from(content: string): TextEntry {
    const entry = new TextEntry();
    entry.length = content.length;
    entry.content = content;

    return entry;
  }

  type(): number {
    return 0;
  }

  encode(): Buffer {
    const length = Buffer.alloc(4);
    length.writeUInt32BE(this.length);

    return Buffer.concat([length, Buffer.from(this.content)]);
  }

  decode(buf: Buffer): void {
    this.length = buf.readUInt32BE(0);
    this.content = buf.subarray(4, 4 + this.length).toString();
  }

  async read(r: Reader): Promise<Buffer> {
    const length = await r.read(4);
    const content = await r.read(length.readUInt32BE(0));

    return Buffer.concat([length, content]);
  }
}

export function createRandomString(length: number): string {
  const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let res = "";
  for (let i = 0; i < length; i++) {
    res += charset.charAt(Math.floor(Math.random() * charset.length));
  }

  return res;
}
