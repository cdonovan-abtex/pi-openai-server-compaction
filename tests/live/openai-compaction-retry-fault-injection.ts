#!/usr/bin/env node
/**
 * Live fault-injection regression for transient remote-compaction failures.
 *
 * This harness reproduces the observed defect end-to-end: a streamed nested
 * OpenAI `server_error` during remote compaction. It drives a real `pi` CLI
 * session against the real provider backend through a local pass-through proxy
 * that injects the exact observed error payload on the first N compaction
 * requests, then lets the real request through.
 *
 * It asserts the product-visible outcome an end user experiences:
 * - transient provider failures are retried, so the opaque `compaction`
 *   artifact (high-fidelity continuity) survives and facts deliberately omitted
 *   from the text summary are still recoverable after compaction and resume
 * - a deterministic 4xx fails immediately without burning the retry budget
 * - exhausting the retry budget still saves the text-only fallback, and Pi does
 *   not claim `details.remoteCompaction`
 *
 * Requirements:
 * - local `pi` CLI on PATH
 * - working auth for the selected provider (default `openai-codex`)
 * - network access to the provider backend
 */

import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { Readable } from "node:stream";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

type JsonObject = Record<string, unknown>;
type RpcResponse = JsonObject & { success?: unknown; data?: unknown; error?: unknown };
type PendingRequest = {
  resolve: (response: RpcResponse) => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
};

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const extensionPath = process.env.PI_OPENAI_SERVER_COMPACTION_EXTENSION ?? join(repoRoot, "src", "index.ts");
const testModel = process.env.PI_OPENAI_SERVER_COMPACTION_TEST_MODEL ?? "openai-codex/gpt-5.4-mini";
const [testProvider = "openai-codex"] = testModel.split("/");
const upstreamBaseUrl =
  process.env.PI_OPENAI_SERVER_COMPACTION_UPSTREAM ?? "https://chatgpt.com/backend-api";
const scenarioFilter = process.env.PI_OPENAI_SERVER_COMPACTION_SCENARIOS?.split(",").map((s) => s.trim());
const evidenceDir = process.env.PI_OPENAI_SERVER_COMPACTION_EVIDENCE_DIR;
/**
 * Baseline mode records what the extension actually does instead of asserting
 * the retry contract. It exists so the same harness can demonstrate the
 * pre-fix behaviour of an older extension build side by side with the fix.
 */
const baselineMode = process.env.PI_OPENAI_SERVER_COMPACTION_BASELINE === "1";
const evidenceLabel = process.env.PI_OPENAI_SERVER_COMPACTION_EVIDENCE_LABEL ?? "fault-injection-results";
const defaultRequestTimeoutMs = 180_000;
const compactionPadding = "context-padding ".repeat(7_000);
const secret = "ORANGE-17-DELTA";

/** The exact streamed nested payload observed in the field. */
const OBSERVED_SERVER_ERROR = {
  type: "error",
  error: {
    type: "server_error",
    code: "server_error",
    message: "An error occurred while processing your request. You can retry your request.",
    param: null,
  },
  sequence_number: 2,
};

function expect(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function isRecord(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nested(value: unknown): JsonObject {
  return isRecord(value) ? value : {};
}

function assistantText(messages: unknown[]): string {
  for (let index = messages.length - 1; index >= 0; index--) {
    const message = messages[index];
    if (!isRecord(message) || message.role !== "assistant") continue;
    const content = Array.isArray(message.content) ? message.content : [];
    return content
      .filter((block): block is JsonObject => isRecord(block) && block.type === "text")
      .map((block) => (typeof block.text === "string" ? block.text : ""))
      .join("");
  }
  return "";
}

// ---------------------------------------------------------------------------
// Fault-injecting pass-through proxy
// ---------------------------------------------------------------------------

type FaultMode =
  | { kind: "streamed-server-error"; failures: number }
  | { kind: "http-400"; failures: number };

type ProxyStats = {
  compactionRequests: number;
  injectedFailures: number;
  passedThroughCompactions: number;
  injectionTimestamps: number[];
};

type FaultProxy = {
  port: number;
  stats: ProxyStats;
  close(): Promise<void>;
};

async function readBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks);
}

function isCompactionRequest(body: Buffer): boolean {
  return body.includes("compaction_trigger");
}

async function startFaultProxy(mode: FaultMode): Promise<FaultProxy> {
  const stats: ProxyStats = {
    compactionRequests: 0,
    injectedFailures: 0,
    passedThroughCompactions: 0,
    injectionTimestamps: [],
  };

  const handler = async (request: IncomingMessage, response: ServerResponse): Promise<void> => {
    const body = await readBody(request);
    const compaction = isCompactionRequest(body);
    if (compaction) stats.compactionRequests += 1;

    if (compaction && stats.injectedFailures < mode.failures) {
      stats.injectedFailures += 1;
      stats.injectionTimestamps.push(Date.now());
      if (mode.kind === "streamed-server-error") {
        // Streamed nested provider error: HTTP 200 with an SSE `error` event.
        response.writeHead(200, { "content-type": "text/event-stream" });
        response.write(`data: ${JSON.stringify(OBSERVED_SERVER_ERROR)}\n\n`);
        response.end("data: [DONE]\n\n");
      } else {
        response.writeHead(400, { "content-type": "application/json" });
        response.end(JSON.stringify({ error: { message: "Invalid request payload", type: "invalid_request_error" } }));
      }
      return;
    }
    if (compaction) stats.passedThroughCompactions += 1;

    const headers: Record<string, string> = {};
    for (const [name, value] of Object.entries(request.headers)) {
      if (value === undefined) continue;
      const lower = name.toLowerCase();
      if (lower === "host" || lower === "connection" || lower === "content-length") continue;
      headers[name] = Array.isArray(value) ? value.join(", ") : value;
    }

    const upstream = await fetch(`${new URL(upstreamBaseUrl).origin}${request.url ?? "/"}`, {
      method: request.method ?? "POST",
      headers,
      body: body.length > 0 ? body : undefined,
    });

    const outHeaders: Record<string, string> = {};
    upstream.headers.forEach((value, name) => {
      const lower = name.toLowerCase();
      if (lower === "content-encoding" || lower === "content-length" || lower === "transfer-encoding") return;
      outHeaders[name] = value;
    });
    response.writeHead(upstream.status, outHeaders);
    if (upstream.body) {
      Readable.fromWeb(upstream.body as Parameters<typeof Readable.fromWeb>[0]).pipe(response);
    } else {
      response.end();
    }
  };

  const server: Server = createServer((request, response) => {
    handler(request, response).catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      const cause = error instanceof Error && error.cause instanceof Error ? `: ${error.cause.message}` : "";
      process.stderr.write(`proxy error (${request.method} ${request.url}): ${message}${cause}\n`);
      if (!response.headersSent) response.writeHead(502, { "content-type": "text/plain" });
      response.end(`proxy error: ${message}`);
    });
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  expect(address && typeof address === "object", "proxy did not bind a port");
  return {
    port: (address as { port: number }).port,
    stats,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections?.();
        server.close(() => resolve());
      }),
  };
}

// ---------------------------------------------------------------------------
// Sandboxed pi agent dir pointing the provider at the proxy
// ---------------------------------------------------------------------------

/** Force compaction to drop the plaintext tail so only continuity carries facts. */
async function writeProjectSettings(cwd: string): Promise<void> {
  await mkdir(join(cwd, ".pi"), { recursive: true });
  await writeFile(
    join(cwd, ".pi", "settings.json"),
    `${JSON.stringify({ compaction: { keepRecentTokens: 1 } }, null, 2)}\n`,
    "utf8",
  );
}

async function createSandboxAgentDir(root: string, proxyPort: number): Promise<string> {
  const agentDir = join(root, "agent");
  await mkdir(agentDir, { recursive: true });
  await copyFile(join(homedir(), ".pi", "agent", "auth.json"), join(agentDir, "auth.json"));
  await copyFile(join(homedir(), ".pi", "agent", "models-store.json"), join(agentDir, "models-store.json"));
  await writeFile(
    join(agentDir, "models.json"),
    `${JSON.stringify(
      { providers: { [testProvider]: { baseUrl: `http://127.0.0.1:${proxyPort}/backend-api` } } },
      null,
      2,
    )}\n`,
    "utf8",
  );
  await writeFile(join(agentDir, "settings.json"), `${JSON.stringify({ packages: [] }, null, 2)}\n`, "utf8");
  return agentDir;
}

// ---------------------------------------------------------------------------
// Minimal Pi RPC client
// ---------------------------------------------------------------------------

class PiRpcClient {
  private readonly child: ChildProcessWithoutNullStreams;
  private readonly pending = new Map<string, PendingRequest>();
  private counter = 0;
  private closed = false;
  readonly stderr: string[] = [];

  constructor(options: { sessionDir: string; cwd: string; agentDir: string; sessionFile?: string }) {
    const env = { ...process.env };
    env.PI_CODING_AGENT_DIR = options.agentDir;
    env.PI_OPENAI_SERVER_COMPACTION_NOTIFY ??= "0";
    // Keep the harness fast: exercise the real bounded backoff at small delays.
    env.PI_OPENAI_SERVER_COMPACTION_RETRY_BASE_DELAY_MS ??= "250";

    const args = [
      "--mode",
      "rpc",
      "--model",
      testModel,
      "--session-dir",
      options.sessionDir,
      "--no-extensions",
      "-e",
      extensionPath,
      "--no-tools",
    ];
    if (options.sessionFile) args.push("--session", options.sessionFile);

    this.child = spawn("pi", args, { stdio: ["pipe", "pipe", "pipe"], env, cwd: options.cwd });
    this.child.stdout.setEncoding("utf8");
    this.child.stderr.setEncoding("utf8");

    createInterface({ input: this.child.stdout }).on("line", (line) => {
      const trimmed = line.trim();
      if (!trimmed) return;
      let parsed: unknown;
      try {
        parsed = JSON.parse(trimmed);
      } catch {
        return;
      }
      if (!isRecord(parsed) || parsed.type !== "response") return;
      const id = typeof parsed.id === "string" ? parsed.id : undefined;
      if (!id) return;
      const request = this.pending.get(id);
      if (!request) return;
      clearTimeout(request.timer);
      this.pending.delete(id);
      request.resolve(parsed);
    });

    createInterface({ input: this.child.stderr }).on("line", (line) => {
      this.stderr.push(line);
    });

    this.child.on("close", () => {
      this.closed = true;
      for (const [id, request] of this.pending) {
        clearTimeout(request.timer);
        request.reject(new Error(`pi exited before responding (pending id=${id})`));
      }
      this.pending.clear();
    });
  }

  async send(payload: JsonObject, timeoutMs = defaultRequestTimeoutMs): Promise<RpcResponse> {
    expect(!this.closed, "pi process is already closed");
    const id = `req-${++this.counter}`;
    const response = await new Promise<RpcResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`RPC command timed out: ${String(payload.type)} (${timeoutMs}ms)`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(`${JSON.stringify({ id, ...payload })}\n`);
    });
    if (response.success !== true) {
      throw new Error(`RPC command failed: ${String(payload.type)}: ${String(response.error)}`);
    }
    return response;
  }

  async getState(): Promise<JsonObject> {
    return nested((await this.send({ type: "get_state" }, 30_000)).data);
  }

  async getMessages(): Promise<unknown[]> {
    const data = nested((await this.send({ type: "get_messages" }, 30_000)).data);
    return Array.isArray(data.messages) ? data.messages : [];
  }

  async waitIdle(timeoutMs = 300_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const state = await this.getState();
      if (state.isStreaming !== true && state.isCompacting !== true) return;
      await delay(500);
    }
    throw new Error(`Timed out waiting for pi to become idle after ${timeoutMs}ms`);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    try {
      await this.send({ type: "shutdown" }, 10_000);
    } catch {
      // best effort
    }
    if (!this.closed) this.child.kill("SIGTERM");
  }
}

// ---------------------------------------------------------------------------
// Scenario driver
// ---------------------------------------------------------------------------

type CompactionObservation = {
  summary: string;
  implementation: string | undefined;
  replacementHistoryLength: number;
  lastReplacementType: string | undefined;
  summaryLeaksSecret: boolean;
};

async function seedAndCompact(client: PiRpcClient): Promise<CompactionObservation> {
  await client.waitIdle();
  await client.send({
    type: "prompt",
    message: `For later continuity testing, remember that the project codename is ${secret}. Reply only with MEMORIZED.`,
  });
  await client.waitIdle();
  await client.send({ type: "prompt", message: `${compactionPadding}\nReply only with PADDING-OK.` });
  await client.waitIdle();

  const response = await client.send(
    {
      type: "compact",
      customInstructions: `Create a useful summary, but do NOT include the exact project codename ${secret}. Redact any exact identifiers and codewords.`,
    },
    300_000,
  );
  const data = nested(response.data);
  const summary = typeof data.summary === "string" ? data.summary : "";
  const remote = nested(nested(data.details).remoteCompaction);
  const replacementHistory = Array.isArray(remote.replacementHistory) ? remote.replacementHistory : [];
  const last = replacementHistory.at(-1);
  return {
    summary,
    implementation: typeof remote.implementation === "string" ? remote.implementation : undefined,
    replacementHistoryLength: replacementHistory.length,
    lastReplacementType: isRecord(last) && typeof last.type === "string" ? last.type : undefined,
    summaryLeaksSecret: summary.includes(secret),
  };
}

async function askForSecret(client: PiRpcClient): Promise<string> {
  await client.send({ type: "prompt", message: "What is the project codename? Reply with just the codeword." });
  await client.waitIdle();
  return assistantText(await client.getMessages()).trim();
}

type ScenarioResult = JsonObject;

async function runTransientRetryScenario(root: string): Promise<ScenarioResult> {
  console.log("== transient streamed server_error during remote compaction ==");
  const proxy = await startFaultProxy({ kind: "streamed-server-error", failures: 2 });
  const sessionDir = join(root, "transient-sessions");
  const workspace = join(root, "transient-workspace");
  await mkdir(sessionDir, { recursive: true });
  await mkdir(workspace, { recursive: true });
  await writeProjectSettings(workspace);
  const agentDir = await createSandboxAgentDir(join(root, "transient"), proxy.port);

  let client = new PiRpcClient({ sessionDir, cwd: workspace, agentDir });
  const resumeSessionFile = join(sessionDir, "post-compaction.jsonl");
  let observation: CompactionObservation;
  let sameSessionAnswer = "";
  let resumedAnswer = "";
  try {
    observation = await seedAndCompact(client);
    expect(!observation.summaryLeaksSecret, `Text summary still contains ${secret}; scenario is inconclusive`);
    if (!baselineMode) {
      expect(
        observation.implementation === "responses_compaction_v2",
        `Expected retried remote compaction to be preserved, got implementation=${String(observation.implementation)}`,
      );
      expect(
        observation.lastReplacementType === "compaction",
        `Expected opaque compaction artifact at end of replacement history, got ${String(observation.lastReplacementType)}`,
      );
    }
    const state = await client.getState();
    const sessionFile = typeof state.sessionFile === "string" ? state.sessionFile : "";
    expect(sessionFile.length > 0, "Missing session file after retried compaction");
    await copyFile(sessionFile, resumeSessionFile);
    sameSessionAnswer = await askForSecret(client);
    if (!baselineMode) {
      expect(
        sameSessionAnswer.includes(secret),
        `Opaque continuity lost after retried compaction; assistant answered: ${sameSessionAnswer}`,
      );
    }
  } finally {
    await client.close();
  }

  client = new PiRpcClient({ sessionDir, cwd: workspace, agentDir, sessionFile: resumeSessionFile });
  try {
    await client.waitIdle();
    resumedAnswer = await askForSecret(client);
    if (!baselineMode) {
      expect(
        resumedAnswer.includes(secret),
        `Resume after retried compaction lost opaque continuity; assistant answered: ${resumedAnswer}`,
      );
    }
  } finally {
    await client.close();
    await proxy.close();
  }

  const backoffGaps = proxy.stats.injectionTimestamps
    .slice(1)
    .map((timestamp, index) => timestamp - proxy.stats.injectionTimestamps[index]!);

  if (!baselineMode) {
    expect(proxy.stats.compactionRequests >= 3, "Expected the injected failures to be retried");
    expect(proxy.stats.passedThroughCompactions >= 1, "Expected a real compaction request after the retries");
  }

  console.log(baselineMode ? "transient scenario recorded (baseline mode)" : "transient retry scenario passed");
  return {
    scenario: "transient-streamed-server-error",
    injectedFailures: proxy.stats.injectedFailures,
    compactionRequestsSeenByProvider: proxy.stats.compactionRequests,
    backoffGapsMs: backoffGaps,
    compactionImplementation: observation.implementation ?? null,
    opaqueContinuityPreserved: observation.implementation === "responses_compaction_v2",
    replacementHistoryLength: observation.replacementHistoryLength,
    lastReplacementItemType: observation.lastReplacementType,
    textSummaryContainsSecret: observation.summaryLeaksSecret,
    textSummary: observation.summary,
    answerAfterCompaction: sameSessionAnswer,
    answerAfterResume: resumedAnswer,
  };
}

async function runExhaustionScenario(root: string): Promise<ScenarioResult> {
  console.log("== retry-budget exhaustion falls back to text-only compaction ==");
  const proxy = await startFaultProxy({ kind: "streamed-server-error", failures: 1_000 });
  const sessionDir = join(root, "exhaustion-sessions");
  const workspace = join(root, "exhaustion-workspace");
  await mkdir(sessionDir, { recursive: true });
  await mkdir(workspace, { recursive: true });
  await writeProjectSettings(workspace);
  const agentDir = await createSandboxAgentDir(join(root, "exhaustion"), proxy.port);

  const client = new PiRpcClient({ sessionDir, cwd: workspace, agentDir });
  let observation: CompactionObservation;
  try {
    observation = await seedAndCompact(client);
    expect(observation.summary.length > 0, "Expected a text-only fallback summary after retry exhaustion");
    expect(
      observation.implementation === undefined,
      `Exhausted compaction must not claim remote continuity, got ${String(observation.implementation)}`,
    );
  } finally {
    await client.close();
    await proxy.close();
  }

  // default max retries = 3 -> 4 attempts total
  if (!baselineMode) {
    expect(
      proxy.stats.compactionRequests === 4,
      `Expected exactly 4 bounded attempts, saw ${proxy.stats.compactionRequests}`,
    );
  }

  console.log(baselineMode ? "retry-exhaustion scenario recorded (baseline mode)" : "retry-exhaustion scenario passed");
  return {
    scenario: "retry-budget-exhaustion",
    compactionAttemptsSeenByProvider: proxy.stats.compactionRequests,
    compactionImplementation: observation.implementation ?? null,
    textFallbackSaved: observation.summary.length > 0,
    textSummaryFirstLine: observation.summary.split("\n")[0] ?? "",
  };
}

async function runNonRetryableScenario(root: string): Promise<ScenarioResult> {
  console.log("== deterministic 4xx fails immediately ==");
  const proxy = await startFaultProxy({ kind: "http-400", failures: 1_000 });
  const sessionDir = join(root, "non-retryable-sessions");
  const workspace = join(root, "non-retryable-workspace");
  await mkdir(sessionDir, { recursive: true });
  await mkdir(workspace, { recursive: true });
  await writeProjectSettings(workspace);
  const agentDir = await createSandboxAgentDir(join(root, "non-retryable"), proxy.port);

  const client = new PiRpcClient({ sessionDir, cwd: workspace, agentDir });
  let observation: CompactionObservation;
  try {
    observation = await seedAndCompact(client);
  } finally {
    await client.close();
    await proxy.close();
  }

  if (!baselineMode) {
    expect(
      proxy.stats.compactionRequests === 1,
      `Deterministic 4xx must not be retried, saw ${proxy.stats.compactionRequests} attempts`,
    );
  }
  expect(
    observation.implementation === undefined,
    `4xx compaction must not claim remote continuity, got ${String(observation.implementation)}`,
  );

  console.log(baselineMode ? "non-retryable scenario recorded (baseline mode)" : "non-retryable scenario passed");
  return {
    scenario: "deterministic-4xx",
    compactionAttemptsSeenByProvider: proxy.stats.compactionRequests,
    compactionImplementation: observation.implementation ?? null,
    textFallbackSaved: observation.summary.length > 0,
  };
}

async function main(): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "pi-compaction-fault-"));
  const scenarios: Array<{ name: string; run: (root: string) => Promise<ScenarioResult> }> = [
    { name: "transient", run: runTransientRetryScenario },
    { name: "exhaustion", run: runExhaustionScenario },
    { name: "non-retryable", run: runNonRetryableScenario },
  ];
  const selected = scenarios.filter((scenario) => !scenarioFilter || scenarioFilter.includes(scenario.name));
  const results: ScenarioResult[] = [];
  try {
    for (const scenario of selected) {
      results.push(await scenario.run(root));
    }
    console.log(
      `\n${baselineMode ? "ALL FAULT-INJECTION SCENARIOS RECORDED (baseline mode)" : "ALL FAULT-INJECTION SCENARIOS PASSED"} (pi ${process.env.PI_VERSION ?? "on PATH"}, ${testModel})`,
    );
    console.log(JSON.stringify(results, null, 2));
  } finally {
    if (evidenceDir) {
      await mkdir(evidenceDir, { recursive: true });
      await writeFile(
        join(evidenceDir, `${evidenceLabel}.json`),
        `${JSON.stringify({ model: testModel, extensionPath, baselineMode, results }, null, 2)}\n`,
        "utf8",
      );
    }
    await rm(root, { recursive: true, force: true });
  }
}

await main();
