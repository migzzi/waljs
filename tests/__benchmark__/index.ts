import walWriteBench from "./wal-write";

async function main(): Promise<void> {
  await walWriteBench();
  process.exit(0);
}

main();
