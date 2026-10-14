// Idle-agent pruning for the SDK SqliteLocalAgentStore (SDK_STORE_BACKEND=sqlite).
//
// Verified against @cursor/sdk 1.0.30 (dist/esm/sqlite.js + local-runtime chunks):
//  - store.agents.delete({filter:{agentIds:[id]}}) deletes, per agent: every run's
//    run_events rows (index.db), the checkpoint blob store (rm -rf agents/<hash>/,
//    after disposing its cached sqlite handle), then runs + agent rows (index.db).
//    It throws if no agent matched, or if the agent's active run is not terminal.
//  - index.db is WAL with auto_vacuum off, so freed pages are reused but the file
//    does not shrink; we optionally VACUUM it when the free list is large.
//  - agents.list / runs.list load all rows then paginate in memory, so one call
//    with a large limit is a single cheap scan.
import { stat } from "node:fs/promises";
import { join } from "node:path";
import type { LocalAgentStore } from "@cursor/sdk";
import type { PruneLogger } from "./store-prune.js";

export interface SqlitePruneOptions {
  retentionMs: number;
  protectedAgentIds?: Set<string>;
  /** Checked before each delete; return false to stop (e.g. shutting down). */
  canContinue?: () => boolean;
  /** Startup mode: nothing is serving, so VACUUM cannot contend with SDK writes. */
  exclusive: boolean;
  now?: number;
}

export interface SqlitePruneResult {
  dir: string;
  agentsTotal: number;
  agentsRemoved: number;
  agentsKeptActive: number;
  agentsKeptProtected: number;
  errors: number;
  indexDbBytesBefore: number;
  indexDbBytesAfter: number;
  agentDirsBytesFreedApprox?: number;
  vacuumed?: boolean;
}

const ALL = Number.MAX_SAFE_INTEGER;
const TERMINAL = new Set(["finished", "error", "cancelled", "expired"]);

async function fileSize(p: string): Promise<number> {
  try {
    return (await stat(p)).size;
  } catch {
    return 0;
  }
}

async function dbBytes(dir: string): Promise<number> {
  const base = join(dir, "index.db");
  return (await fileSize(base)) + (await fileSize(`${base}-wal`));
}

export async function pruneSqliteStore(
  dir: string,
  store: LocalAgentStore,
  opts: SqlitePruneOptions,
  log: PruneLogger,
): Promise<SqlitePruneResult> {
  const now = opts.now ?? Date.now();
  const cutoff = now - opts.retentionMs;
  const before = await dbBytes(dir);
  const agents = (await store.agents.list({ filter: { limit: ALL } })).items;
  const runs = (await store.runs.list({ filter: { limit: ALL } })).items;

  const lastActive = new Map<string, number>();
  const busy = new Set<string>();
  for (const a of agents) {
    lastActive.set(a.agentId, Math.max(a.createdAt || 0, a.updatedAt || 0));
    if (a.status === "running") busy.add(a.agentId);
  }
  for (const r of runs) {
    const t = Math.max(r.createdAt || 0, r.updatedAt || 0, r.endedAt || 0);
    lastActive.set(r.agentId, Math.max(lastActive.get(r.agentId) ?? 0, t));
    if (!TERMINAL.has(r.status)) busy.add(r.agentId);
  }

  const res: SqlitePruneResult = {
    dir,
    agentsTotal: agents.length,
    agentsRemoved: 0,
    agentsKeptActive: 0,
    agentsKeptProtected: 0,
    errors: 0,
    indexDbBytesBefore: before,
    indexDbBytesAfter: before,
  };
  for (const a of agents) {
    const t = lastActive.get(a.agentId) ?? 0;
    if (t >= cutoff) continue;
    if (opts.protectedAgentIds?.has(a.agentId)) {
      res.agentsKeptProtected += 1;
      continue;
    }
    if (busy.has(a.agentId)) {
      res.agentsKeptActive += 1;
      continue;
    }
    if (opts.canContinue && !opts.canContinue()) break;
    try {
      await store.agents.delete({ filter: { agentIds: [a.agentId] } });
      res.agentsRemoved += 1;
    } catch (error) {
      res.errors += 1;
      log.warn({ dir, agent_id: a.agentId, err: String(error) }, "sqlite store prune: delete failed");
    }
  }
  // Only at startup (nothing serving): a long VACUUM while serving could hold the write
  // lock past the SDK's 5 s busy_timeout. Freed pages are reused anyway, so the file
  // stays bounded by the retained working set between restarts.
  if (opts.exclusive) {
    res.vacuumed = await maybeVacuum(join(dir, "index.db"), opts, log);
  }
  res.indexDbBytesAfter = await dbBytes(dir);
  return res;
}

/**
 * VACUUM index.db only when >= 8 MiB and >= 30% of it is free pages. Uses a separate
 * node:sqlite connection with a busy timeout; never fatal.
 */
async function maybeVacuum(dbPath: string, opts: SqlitePruneOptions, log: PruneLogger): Promise<boolean> {
  if (opts.canContinue && !opts.canContinue()) return false;
  let db: import("node:sqlite").DatabaseSync | undefined;
  try {
    const { DatabaseSync } = await import("node:sqlite");
    db = new DatabaseSync(dbPath);
    db.exec("PRAGMA busy_timeout = 3000;");
    const pageSize = Number((db.prepare("PRAGMA page_size").get() as { page_size: number }).page_size);
    const pages = Number((db.prepare("PRAGMA page_count").get() as { page_count: number }).page_count);
    const free = Number((db.prepare("PRAGMA freelist_count").get() as { freelist_count: number }).freelist_count);
    const freeBytes = free * pageSize;
    if (freeBytes < 8 << 20 || free < pages * 0.3) return false;
    const t0 = Date.now();
    db.exec("VACUUM;");
    db.exec("PRAGMA wal_checkpoint(TRUNCATE);");
    log.info({ db: dbPath, free_bytes: freeBytes, elapsed_ms: Date.now() - t0 }, "sqlite store vacuumed");
    return true;
  } catch (error) {
    log.warn({ db: dbPath, err: String(error) }, "sqlite store vacuum skipped");
    return false;
  } finally {
    try {
      db?.close();
    } catch {
      /* ignore */
    }
  }
}

export async function pruneSqliteStores(
  stores: Array<{ dir: string; store: LocalAgentStore }>,
  opts: SqlitePruneOptions,
  log: PruneLogger,
): Promise<SqlitePruneResult[]> {
  const started = Date.now();
  const out: SqlitePruneResult[] = [];
  for (const { dir, store } of stores) {
    try {
      const r = await pruneSqliteStore(dir, store, opts, log);
      out.push(r);
      log.info(
        {
          dir,
          backend: "sqlite",
          mode: opts.exclusive ? "startup" : "periodic",
          retention_hours: opts.retentionMs / 3_600_000,
          agents_total: r.agentsTotal,
          agents_removed: r.agentsRemoved,
          agents_kept_active: r.agentsKeptActive,
          agents_kept_protected: r.agentsKeptProtected,
          errors: r.errors,
          index_db_bytes_before: r.indexDbBytesBefore,
          index_db_bytes_after: r.indexDbBytesAfter,
          vacuumed: r.vacuumed ?? false,
          elapsed_ms: Date.now() - started,
        },
        "sdk store prune",
      );
    } catch (error) {
      log.warn({ dir, err: String(error) }, "sdk store prune failed");
    }
  }
  return out;
}
