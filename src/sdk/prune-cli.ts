#!/usr/bin/env node
// One-shot offline prune: node dist/sdk/prune-cli.js [stateDir]  (gateway must be stopped)
import { pruneIdleAgents } from "./store-prune.js";

const stateDir = process.argv[2] ?? process.env.STATE_DIR ?? "/data";
const hours = Number(process.env.CHECKPOINT_RETENTION_HOURS ?? "24");
const log = {
  info: (f: Record<string, unknown>, m: string) => console.log(JSON.stringify({ level: "info", msg: m, ...f })),
  warn: (f: Record<string, unknown>, m: string) => console.log(JSON.stringify({ level: "warn", msg: m, ...f })),
};
await pruneIdleAgents(stateDir, { retentionMs: hours * 3_600_000, exclusive: true }, log);
