import { EntryType, IEntry } from "./entry";

export class EntryRegistry {
  private static ENTIRES: Map<EntryType, () => IEntry> = new Map();

  static register(entryFactory: () => IEntry): void {
    const entry = entryFactory();
    this.ENTIRES.set(entry.type(), entryFactory);
  }

  static get(type: number): IEntry {
    const entryFactory = this.ENTIRES.get(type);

    if (!entryFactory) {
      return null;
    }

    return entryFactory();
  }
}
