#!/usr/bin/env node
import { loadConfig } from "./config.js";
import { SystemClock } from "./clock.js";
import { createLogger } from "./log.js";
import { createApp } from "./server/app.js";
import { agentStoreRoots, createCursorRuntime } from "./sdk/cursor-runtime.js";
import { ensureEmptyWorkspace } from "./sdk/empty-workspace.js";
import { configureSdkOutboundProxy } from "./sdk/proxy.js";
import { readdir, stat, unlink } from "node:fs/promises";
import { join } from "node:path";
import { pruneIdleAgents } from "./sdk/store-prune.js";
import { pruneSqliteStores } from "./sdk/sqlite-prune.js";

async function cleanupStaleTmp(rootDir: string, maxAgeMs: number, log: ReturnType<typeof createLogger>): Promise<void> {
  const now = Date.now();
  let removed = 0;
  const walk = async (dir: string): Promise<void> => {
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(full);
      } else if (/^\..*\.ndjson\..*\.tmp$/.test(entry.name)) {
        try {
          const st = await stat(full);
          if (now - st.mtimeMs > maxAgeMs) {
            await unlink(full);
            removed += 1;
          }
        } catch {
          /* ignore */
        }
      }
    }
  };
  await walk(rootDir);
  if (removed > 0) log.info({ removed, dir: rootDir }, "cleaned stale sdk tmp files");
}

function startStaleTmpCleanup(rootDir: string, log: ReturnType<typeof createLogger>): void {
  const maxAgeMs = 60 * 60_000;
  const intervalMs = 60 * 60_000;
  void cleanupStaleTmp(rootDir, maxAgeMs, log);
  setInterval(() => void cleanupStaleTmp(rootDir, maxAgeMs, log), intervalMs).unref();
}


const config = loadConfig();
const logger = createLogger(config.logLevel);
const proxy = configureSdkOutboundProxy();
const sdk = createCursorRuntime({ stateDir: config.stateDir });
const workspaceDir = ensureEmptyWorkspace(config.instanceId, config.emptyWorkspaceDir);
const app = createApp({
  config,
  sdk,
  clock: new SystemClock(),
  logger,
  workspaceDir,
});

// Prune idle agents from the SDK store BEFORE serving, while nothing can touch it.
const storeBackend = sdk.stores.backend;
const retentionHours = Number(process.env.CHECKPOINT_RETENTION_HOURS ?? "24");
const retentionMs = (Number.isFinite(retentionHours) && retentionHours > 0 ? retentionHours : 24) * 3_600_000;
if (storeBackend === "jsonl") {
  await pruneIdleAgents(config.stateDir, { retentionMs, exclusive: true }, logger);
} else {
  await pruneSqlite(true);
}

/** agentIds bound to sessions held by this process (never pruned). */
function liveAgentIds(): Set<string> {
  const live = new Set<string>();
  for (const s of app.registry.sessions.values()) {
    if (s.sdkAgentId) live.add(s.sdkAgentId);
    if (s.agent?.agentId) live.add(s.agent.agentId);
  }
  return live;
}

async function pruneSqlite(exclusive: boolean): Promise<void> {
  try {
    await sdk.stores.discover(agentStoreRoots(config.stateDir, "sqlite"));
    await pruneSqliteStores(
      await sdk.stores.opened(),
      {
        retentionMs,
        exclusive,
        protectedAgentIds: exclusive ? undefined : liveAgentIds(),
        canContinue: () => !app.registry.shuttingDown,
      },
      logger,
    );
  } catch (error) {
    logger.warn({ err: String(error) }, "sdk store prune failed");
  }
}

const server = app.listen();
logger.info(
  {
    host: config.host,
    port: config.port,
    auth_mode: config.authMode,
    instance_id: config.instanceId,
    sdk_version: sdk.sdkVersion,
    sdk_store_backend: storeBackend,
    proxy_configured: proxy.configured,
    agent_transport: proxy.agentTransport,
    fetch_transport: proxy.fetchTransport,
  },
  "cursor-sdk2api listening",
);

startStaleTmpCleanup(config.stateDir, logger);
startPeriodicPrune();

// Hourly in-process prune. Agents bound to live gateway sessions are never pruned.
//  - sqlite: delete idle agents through the SDK store API (see sdk/sqlite-prune.ts).
//  - jsonl: the SDK has no cache and replaces files via tmp+rename, so we stream-copy kept
//    rows to a tmp file and rename only if the source was untouched meanwhile (otherwise
//    retry in 5 min).
function startPeriodicPrune(): void {
  let running = false;
  let retries = 0;
  const run = async (): Promise<void> => {
    if (running || app.registry.shuttingDown) return;
    running = true;
    try {
      if (storeBackend === "sqlite") {
        await pruneSqlite(false);
        return;
      }
      const results = await pruneIdleAgents(
        config.stateDir,
        { retentionMs, exclusive: false, protectedAgentIds: liveAgentIds(), canCommit: () => !app.registry.shuttingDown },
        logger,
      );
      const skipped = results.some((r) => r.files.some((f) => f.skipped));
      if (skipped && retries < 6) {
        retries += 1;
        setTimeout(() => void run(), 5 * 60_000).unref();
      } else {
        retries = 0;
      }
    } finally {
      running = false;
    }
  };
  setInterval(() => void run(), 60 * 60_000).unref();
}

const shutdown = async (signal: string) => {
  logger.info({ signal }, "draining");
  app.beginShutdown();
  await app.coordinator.drain(config.runDeadlineMs);
  app.close();
  try {
    await sdk.stores.disposeAll();
  } catch (error) {
    logger.warn({ err: String(error) }, "sdk store dispose failed");
  }
  server.close(() => {
    logger.info({}, "shutdown complete");
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 10_000).unref();
};

process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});
process.on("SIGINT", () => {
  void shutdown("SIGINT");
});
