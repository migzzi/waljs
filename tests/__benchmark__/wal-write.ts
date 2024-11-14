import benny from "benny";
import { WAL } from "../../lib/wal";
import path from "path";
import { createRandomString, TextEntry } from "../utils";
import { rm } from "fs/promises";
import { existsSync, mkdirSync, rmSync } from "fs";

if (existsSync(path.join(__dirname, "wal"))) {
  rmSync(path.join(__dirname, "wal"), { recursive: true });
}

mkdirSync(path.join(__dirname, "wal"));
const wal = new WAL(path.join(__dirname, "wal"));

module.exports = () =>
  benny.suite(
    "WAL write suite",
    benny.add(
      "WAL write",
      async () => {
        await wal.init();
        return async () => {
          await wal.write(TextEntry.from(createRandomString(10)));
        };
      },
      {
        minSamples: 100,
      },
    ),
    benny.cycle(),
    benny.complete(),
    // benny.save({ file: "cache-hit-benchmark-chart", version: "1.0.0", format: "chart.html" }),
    benny.save({ file: "wal-write-benchmark-table", version: "1.0.0", format: "table.html" }),
  );

export default module.exports;
