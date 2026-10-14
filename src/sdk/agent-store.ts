// Per-credential SDK local agent store cache.
//
// SDK_STORE_BACKEND=sqlite (default) uses the SDK's SqliteLocalAgentStore
// (`{root}/index.db` + `{root}/agents/<hash>/store.db`, WAL, node:sqlite);
// SDK_STORE_BACKEND=jsonl keeps the legacy JsonlLocalAgentStore (rollback path).
// SQLite stores live in NEW directories (`sdk-store-sqlite/<fp>`, `sand/store-sqlite/<fp>`)
// so the old JSONL data is never touched and switching back is just an env change.
import { existsSync } from "node:fs";
import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { JsonlLocalAgentStore, type LocalAgentStore } from "@cursor/sdk";
import type { SqliteLocalAgentStore } from "@cursor/sdk/sqlite";

export type SdkStoreBackend = "sqlite" | "jsonl";

export function sdkStoreBackend(env: NodeJS.ProcessEnv = process.env): SdkStoreBackend {
  const raw = (env.SDK_STORE_BACKEND ?? "").trim().toLowerCase();
  if (raw === "" || raw === "sqlite") return "sqlite";
  if (raw === "jsonl") return "jsonl";
  throw new Error(`SDK_STORE_BACKEND must be "sqlite" or "jsonl" (got "${env.SDK_STORE_BACKEND}")`);
}

/** Directory name of the store root for a backend (relative to stateDir or the sand root). */
export function storeRootName(backend: SdkStoreBackend, profile: "sdk" | "sand"): string {
  if (profile === "sand") return backend === "sqlite" ? "store-sqlite" : "store";
  return backend === "sqlite" ? "sdk-store-sqlite" : "sdk-store";
}

export class AgentStoreRegistry {
  private readonly stores = new Map<string, Promise<LocalAgentStore>>();
  private disposed = false;

  constructor(readonly backend: SdkStoreBackend) {}

  /** Returns the cached store for `storeDir`, opening it once. */
  get(storeDir: string, workspaceRef: string): Promise<LocalAgentStore> {
    if (this.disposed) return Promise.reject(new Error("agent store registry is disposed"));
    let entry = this.stores.get(storeDir);
    if (!entry) {
      entry =
        this.backend === "jsonl"
          ? Promise.resolve(new JsonlLocalAgentStore(storeDir) as LocalAgentStore)
          : openSqlite(storeDir, workspaceRef);
      entry.catch(() => {
        if (this.stores.get(storeDir) === entry) this.stores.delete(storeDir);
      });
      this.stores.set(storeDir, entry);
    }
    return entry;
  }

  /** Stores opened so far (dir -> store). */
  async opened(): Promise<Array<{ dir: string; store: LocalAgentStore }>> {
    const out: Array<{ dir: string; store: LocalAgentStore }> = [];
    for (const [dir, p] of this.stores) {
      try {
        out.push({ dir, store: await p });
      } catch {
        /* failed open: skipped */
      }
    }
    return out;
  }

  /** Open every sqlite store that already exists on disk under the given roots. */
  async discover(roots: string[]): Promise<void> {
    if (this.backend !== "sqlite") return;
    for (const root of roots) {
      let entries;
      try {
        entries = await readdir(root, { withFileTypes: true });
      } catch {
        continue;
      }
      for (const e of entries) {
        const dir = join(root, e.name);
        if (e.isDirectory() && existsSync(join(dir, "index.db"))) {
          await this.get(dir, dir).catch(() => undefined);
        }
      }
    }
  }

  async disposeAll(): Promise<void> {
    this.disposed = true;
    const all = [...this.stores.values()];
    this.stores.clear();
    await Promise.all(
      all.map(async (p) => {
        try {
          const store = await p;
          const d = (store as Partial<SqliteLocalAgentStore>).dispose;
          if (typeof d === "function") await d.call(store);
        } catch {
          /* ignore */
        }
      }),
    );
  }
}

async function openSqlite(storeDir: string, workspaceRef: string): Promise<LocalAgentStore> {
  // Lazy import: the jsonl backend never loads the sqlite driver.
  const { SqliteLocalAgentStore } = await import("@cursor/sdk/sqlite");
  return SqliteLocalAgentStore.open({ stateRoot: storeDir, workspaceRef });
}
