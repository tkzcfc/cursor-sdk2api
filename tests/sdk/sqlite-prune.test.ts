import { existsSync, mkdtempSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { AgentStoreRegistry } from "../../src/sdk/agent-store.js";
import { pruneSqliteStore } from "../../src/sdk/sqlite-prune.js";

const log = { info: () => undefined, warn: () => undefined };

test("sqlite prune deletes idle agents (rows + per-agent store.db dir) and keeps recent/protected/running", async () => {
  const dir = mkdtempSync(join(tmpdir(), "sqlite-prune-"));
  const reg = new AgentStoreRegistry("sqlite");
  const store = await reg.get(dir, "/ws");
  const now = Date.now();
  const old = now - 10 * 3_600_000;
  const mk = async (agentId: string, at: number, runStatus: "finished" | "running" = "finished") => {
    await store.agents.create({ agent: { agentId, cwd: "/ws", status: "idle", createdAt: at, updatedAt: at } });
    await store.runs.create({
      run: { runId: `run-${agentId}`, agentId, turnNumber: 1, status: runStatus, createdAt: at, updatedAt: at },
    });
    await store.runEvents.append({ runId: `run-${agentId}`, eventType: "x", payload: { a: 1 } });
    await store.checkpoints.create({ agentId, blobId: "b1", data: new Uint8Array(4096) });
  };
  await mk("idle-old", old);
  await mk("recent", now);
  await mk("protected-old", old);
  await mk("running-old", old, "running");
  expect(readdirSync(join(dir, "agents"))).toHaveLength(4);

  const r = await pruneSqliteStore(
    dir,
    store,
    { retentionMs: 6 * 3_600_000, exclusive: false, protectedAgentIds: new Set(["protected-old"]) },
    log,
  );
  expect(r.agentsRemoved).toBe(1);
  expect(r.agentsKeptProtected).toBe(1);
  expect(r.agentsKeptActive).toBe(1);
  expect(await store.agents.get({ agentId: "idle-old" })).toBeNull();
  expect(await store.runs.get({ agentId: "idle-old", runId: "run-idle-old" })).toBeNull();
  expect((await store.runEvents.list({ runId: "run-idle-old" })).items).toHaveLength(0);
  expect(await store.checkpoints.get({ agentId: "recent", blobId: "b1" })).not.toBeNull();
  expect(readdirSync(join(dir, "agents"))).toHaveLength(3);
  expect(existsSync(join(dir, "index.db"))).toBe(true);
  await reg.disposeAll();
});
