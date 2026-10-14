// Prunes idle agents from @cursor/sdk JsonlLocalAgentStore directories.
//
// Why: the SDK store (JsonlLocalAgentStore) never deletes anything on its own and
// every read/write does readFile(whole file) + JSON.parse(every line) + rewrite.
// checkpoints.ndjson grows unbounded and eventually blows the V8 heap.
//
// Findings from @cursor/sdk 1.0.30 (dist/cjs/index.js, JsonlLocalAgentStore):
//  - No in-memory cache: each operation re-reads the file from disk.
//  - Every mutation rewrites the whole file via `.<name>.<hex>.tmp` + rename (atomic).
//  - Writes are serialized only inside the SDK (module-private promise chain), so we
//    cannot join that queue. In-process pruning therefore streams to a tmp file and
//    only renames if the source file did not change (ino/size/mtime) while we copied.
//  - Checkpoint blobs are addressed by (agentId, blobId); there is no cross-agent
//    blob sharing, so dropping all rows of a pruned agent cannot break a kept agent.
//  - run_events rows carry runId (agentId only inside payload); runs map runId->agentId.
import { createReadStream, createWriteStream } from "node:fs";
import { open, readdir, rename, stat, unlink } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { basename, dirname, join } from "node:path";
import { createInterface } from "node:readline";

export interface PruneLogger {
  info(fields: Record<string, unknown>, msg: string): void;
  warn(fields: Record<string, unknown>, msg: string): void;
}

export interface PruneOptions {
  retentionMs: number;
  /** agentIds that must never be removed (e.g. live gateway sessions). */
  protectedAgentIds?: Set<string>;
  /** Startup mode: process is not serving, so no concurrent SDK writes are possible. */
  exclusive: boolean;
  /** Called right before each rename; return false to abort (e.g. a run became active). */
  canCommit?: () => boolean;
  now?: number;
}

export interface FileResult {
  file: string;
  bytesBefore: number;
  bytesAfter: number;
  linesRemoved: number;
  skipped?: string;
}

export interface DirResult {
  dir: string;
  agentsTotal: number;
  agentsKept: number;
  agentsRemoved: number;
  files: FileResult[];
}

const FILES = ["checkpoints.ndjson", "run_events.ndjson", "runs.ndjson", "agents.ndjson"] as const;

async function* lines(path: string): AsyncGenerator<string> {
  const rl = createInterface({ input: createReadStream(path, { encoding: "utf8" }), crlfDelay: Infinity });
  for await (const line of rl) yield line;
}

async function exists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

// Cheap id extraction for huge rows (checkpoint blobs, events) without JSON.parse of the payload.
const FIELD_RE: Record<string, RegExp> = {
  agentId: /"agentId":"([^"]+)"/,
  runId: /"runId":"([^"]+)"/,
};
function fieldPrefix(line: string, key: "agentId" | "runId"): string | undefined {
  return FIELD_RE[key]!.exec(line.slice(0, 512))?.[1];
}

function num(v: unknown): number {
  return typeof v === "number" && Number.isFinite(v) ? v : 0;
}

/** Find directories containing an SDK JSONL store under stateDir (sdk-store/<hash>, sand stores, ...). */
export async function findStoreDirs(root: string, depth = 5): Promise<string[]> {
  const out: string[] = [];
  const walk = async (dir: string, d: number): Promise<void> => {
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    if (entries.some((e) => e.isFile() && e.name === "agents.ndjson")) out.push(dir);
    if (d <= 0) return;
    for (const e of entries) {
      if (e.isDirectory() && e.name !== "node_modules" && !e.name.startsWith(".")) await walk(join(dir, e.name), d - 1);
    }
  };
  await walk(root, depth);
  return out;
}

async function filterFile(
  path: string,
  keepLine: (line: string) => boolean,
  opts: PruneOptions,
): Promise<FileResult> {
  const before = await stat(path);
  const res: FileResult = { file: basename(path), bytesBefore: before.size, bytesAfter: before.size, linesRemoved: 0 };
  // Same naming scheme as the SDK so the stale-tmp sweeper also collects crash leftovers.
  const tmp = join(dirname(path), `.${basename(path)}.${randomBytes(8).toString("hex")}.tmp`);
  const ws = createWriteStream(tmp, { flags: "wx", mode: before.mode & 0o777 });
  let written = 0;
  try {
    for await (const line of lines(path)) {
      if (line.trim().length === 0) continue;
      if (!keepLine(line)) {
        res.linesRemoved += 1;
        continue;
      }
      const chunk = line + "\n";
      written += Buffer.byteLength(chunk);
      if (!ws.write(chunk)) await once(ws, "drain");
    }
    ws.end();
    await once(ws, "finish");
  } catch (error) {
    ws.destroy();
    await unlink(tmp).catch(() => {});
    throw error;
  }
  if (res.linesRemoved === 0) {
    await unlink(tmp).catch(() => {});
    return res;
  }
  const fh = await open(tmp, "r+");
  try {
    await fh.sync();
  } finally {
    await fh.close();
  }
  if (!opts.exclusive) {
    const now = await stat(path);
    const changed = now.ino !== before.ino || now.size !== before.size || now.mtimeMs !== before.mtimeMs;
    if (changed || (opts.canCommit && !opts.canCommit())) {
      await unlink(tmp).catch(() => {});
      return { ...res, linesRemoved: 0, skipped: changed ? "changed_during_prune" : "busy" };
    }
  }
  await rename(tmp, path);
  res.bytesAfter = written;
  return res;
}

export async function pruneStoreDir(dir: string, opts: PruneOptions): Promise<DirResult> {
  const now = opts.now ?? Date.now();
  const cutoff = now - opts.retentionMs;
  const agentsPath = join(dir, "agents.ndjson");
  const runsPath = join(dir, "runs.ndjson");

  // 1. Last activity per agent = max(agent.createdAt/updatedAt, run.updatedAt/endedAt).
  const lastActive = new Map<string, number>();
  const bump = (id: string, t: number) => lastActive.set(id, Math.max(lastActive.get(id) ?? 0, t));
  for await (const line of lines(agentsPath)) {
    if (!line.trim()) continue;
    try {
      const a = JSON.parse(line) as Record<string, unknown>;
      if (typeof a.agentId === "string") bump(a.agentId, Math.max(num(a.createdAt), num(a.updatedAt)));
    } catch {
      /* torn last line: ignore */
    }
  }
  const runAgent = new Map<string, string>();
  if (await exists(runsPath)) {
    for await (const line of lines(runsPath)) {
      if (!line.trim()) continue;
      try {
        const r = JSON.parse(line) as Record<string, unknown>;
        if (typeof r.agentId !== "string" || typeof r.runId !== "string") continue;
        runAgent.set(r.runId, r.agentId);
        bump(r.agentId, Math.max(num(r.createdAt), num(r.updatedAt), num(r.endedAt)));
      } catch {
        /* ignore */
      }
    }
  }

  const remove = new Set<string>();
  for (const [id, t] of lastActive) {
    if (t < cutoff && !opts.protectedAgentIds?.has(id)) remove.add(id);
  }
  const result: DirResult = {
    dir,
    agentsTotal: lastActive.size,
    agentsKept: lastActive.size - remove.size,
    agentsRemoved: remove.size,
    files: [],
  };
  if (remove.size === 0) return result;

  // Rows whose agent cannot be determined, or agents not in agents.ndjson, are kept (conservative).
  const keepByAgent = (id: string | undefined) => id === undefined || !remove.has(id);
  const keepers: Record<(typeof FILES)[number], (line: string) => boolean> = {
    "checkpoints.ndjson": (l) => keepByAgent(fieldPrefix(l, "agentId")),
    "run_events.ndjson": (l) => {
      const runId = fieldPrefix(l, "runId");
      const agentId = (runId && runAgent.get(runId)) ?? fieldPrefix(l, "agentId");
      return keepByAgent(agentId);
    },
    "runs.ndjson": (l) => keepByAgent(fieldPrefix(l, "agentId")),
    "agents.ndjson": (l) => keepByAgent(fieldPrefix(l, "agentId")),
  };
  // Children first, agents.ndjson last, so a half-done prune never leaves rows for a
  // kept agent record missing while its parent survives in an inconsistent way.
  for (const name of FILES) {
    const p = join(dir, name);
    if (!(await exists(p))) continue;
    result.files.push(await filterFile(p, keepers[name], opts));
  }
  return result;
}

export async function pruneIdleAgents(
  stateDir: string,
  opts: PruneOptions,
  log: PruneLogger,
): Promise<DirResult[]> {
  const started = Date.now();
  const results: DirResult[] = [];
  for (const dir of await findStoreDirs(stateDir)) {
    try {
      const r = await pruneStoreDir(dir, opts);
      results.push(r);
      log.info(
        {
          dir,
          mode: opts.exclusive ? "startup" : "periodic",
          retention_hours: opts.retentionMs / 3_600_000,
          agents_total: r.agentsTotal,
          agents_kept: r.agentsKept,
          agents_removed: r.agentsRemoved,
          files: r.files,
          elapsed_ms: Date.now() - started,
        },
        "sdk store prune",
      );
    } catch (error) {
      log.warn({ dir, err: String(error) }, "sdk store prune failed");
    }
  }
  return results;
}
