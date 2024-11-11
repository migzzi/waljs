import { EntryType, IEntry } from "./entry";

export class EntryRegistry {
  private static entries: Map<EntryType, () => IEntry> = new Map();

  static register(entryFactory: () => IEntry): void {
    const entry = entryFactory();
    this.entries.set(entry.type(), entryFactory);
  }

  static get(type: number): IEntry {
    const entryFactory = this.entries.get(type);

    if (!entryFactory) {
      return null;
    }

    return entryFactory();
  }
}
