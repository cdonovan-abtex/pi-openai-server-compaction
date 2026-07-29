import { execFileSync } from "node:child_process";
import { mkdirSync, existsSync, lstatSync, readFileSync, rmSync, symlinkSync } from "node:fs";
import assert from "node:assert/strict";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const localNodeModules = join(repoRoot, "node_modules");

function packagePathSegments(packageName) {
  return packageName.split("/");
}

function npmGlobalRoot() {
  try {
    return execFileSync("npm", ["root", "-g"], {
      cwd: repoRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
  } catch {
    return undefined;
  }
}

function candidateRoots() {
  const roots = new Set();
  roots.add(localNodeModules);

  const globalRoot = npmGlobalRoot();
  if (globalRoot) roots.add(globalRoot);

  const voltaPiRoot = join(
    homedir(),
    ".volta",
    "tools",
    "image",
    "packages",
    "@earendil-works",
    "pi-coding-agent",
    "lib",
    "node_modules",
  );
  roots.add(voltaPiRoot);
  roots.add(join(voltaPiRoot, "@earendil-works", "pi-coding-agent", "node_modules"));

  return [...roots];
}

function resolveInstalledPackageDir(packageName) {
  const segments = packagePathSegments(packageName);
  for (const root of candidateRoots()) {
    const dir = join(root, ...segments);
    const packageJsonPath = join(dir, "package.json");
    if (existsSync(packageJsonPath)) {
      return dir;
    }
  }
  return undefined;
}

function ensureLocalPeerLink(packageName) {
  const localDir = join(localNodeModules, ...packagePathSegments(packageName));
  if (existsSync(join(localDir, "package.json"))) {
    return;
  }

  const targetDir = resolveInstalledPackageDir(packageName);
  if (!targetDir) {
    throw new Error(
      `Unable to locate peer dependency ${packageName}. Install Pi or add the package locally before running smoke.`,
    );
  }

  mkdirSync(dirname(localDir), { recursive: true });
  if (existsSync(localDir)) {
    const stat = lstatSync(localDir);
    if (stat.isSymbolicLink() || stat.isDirectory()) {
      rmSync(localDir, { recursive: true, force: true });
    }
  }
  symlinkSync(targetDir, localDir, "dir");
}

for (const packageName of [
  "@earendil-works/pi-coding-agent",
  "@earendil-works/pi-agent-core",
  "@earendil-works/pi-ai",
]) {
  ensureLocalPeerLink(packageName);
}

const { default: extensionFactory, isTextOnlyFallbackCompaction } = await import(
  pathToFileURL(join(repoRoot, "src", "index.ts")).href
);
assert.equal(typeof extensionFactory, "function", "extension entrypoint should export a function");

const {
  buildCodexWebSocketHeaders,
  buildRemoteCompactionHeaders,
  buildRemoteCompactionDetails,
  buildRemoteCompactionRequestBody,
  buildRemoteCompactionV2History,
  callRemoteCompactionEndpoint,
  extractRemoteCompactionDetails,
  normalizeResponseItemsForPrompt,
  parseRemoteCompactionV2Events,
  processCompactedHistory,
  reconstructRemoteCompactionStateFromBranch,
  remoteCompactionV2EndpointUrl,
  sleepForRemoteCompactionRetry,
} = await import(pathToFileURL(join(repoRoot, "src", "remote-compaction.ts")).href);
const {
  selectInputItemsForContinuation,
} = await import(pathToFileURL(join(repoRoot, "src", "openai-ws-stream.ts")).href);

const targetModelKey = "openai:openai-responses:gpt-5.4-nano";
const reconstructed = reconstructRemoteCompactionStateFromBranch({
  branchEntries: [
    {
      type: "compaction",
      id: "cmp-1",
      details: {
        remoteCompaction: {
          version: 1,
          provider: "openai-responses-compact",
          modelKey: targetModelKey,
          replacementHistory: [
            {
              type: "compaction",
              encrypted_content: "ENCRYPTED",
            },
          ],
        },
      },
    },
    {
      type: "message",
      id: "user-a1",
      message: {
        role: "user",
        content: [{ type: "text", text: "KEEP_ME_ONE" }],
      },
    },
    {
      type: "message",
      id: "assistant-a1",
      message: {
        role: "assistant",
        provider: "openai",
        api: "openai-responses",
        model: "gpt-5.4-nano",
        content: [{ type: "text", text: "KEEP_REPLY_ONE" }],
      },
    },
    {
      type: "message",
      id: "user-b1",
      message: {
        role: "user",
        content: [{ type: "text", text: "DROP_ME" }],
      },
    },
    {
      type: "message",
      id: "assistant-b1",
      message: {
        role: "assistant",
        provider: "anthropic",
        api: "anthropic-messages",
        model: "claude-sonnet-4-6",
        content: [{ type: "text", text: "DROP_REPLY" }],
      },
    },
    {
      type: "message",
      id: "user-a2",
      message: {
        role: "user",
        content: [{ type: "text", text: "KEEP_ME_TWO" }],
      },
    },
    {
      type: "message",
      id: "assistant-a2",
      message: {
        role: "assistant",
        provider: "openai",
        api: "openai-responses",
        model: "gpt-5.4-nano",
        content: [{ type: "text", text: "KEEP_REPLY_TWO" }],
      },
    },
  ],
});
assert.ok(reconstructed, "expected reconstructed remote compaction state");
const reconstructedJson = JSON.stringify(reconstructed.explicitHistory);
assert.match(reconstructedJson, /KEEP_ME_ONE/);
assert.match(reconstructedJson, /KEEP_REPLY_ONE/);
assert.match(reconstructedJson, /KEEP_ME_TWO/);
assert.match(reconstructedJson, /KEEP_REPLY_TWO/);
assert.doesNotMatch(reconstructedJson, /DROP_ME/);
assert.doesNotMatch(reconstructedJson, /DROP_REPLY/);

const requestBody = buildRemoteCompactionRequestBody({
  model: {
    id: "gpt-5.4-nano",
  },
  input: [{ type: "compaction", encrypted_content: "ENCRYPTED" }],
  instructions: "system",
  tools: [{ type: "function", name: "read" }],
  parallelToolCalls: true,
  reasoning: { effort: "high", summary: "auto" },
  text: { verbosity: "medium" },
});
assert.equal(requestBody.model, "gpt-5.4-nano");
assert.equal(requestBody.stream, true);
assert.equal(requestBody.store, false);
assert.equal(requestBody.tool_choice, "auto");
assert.deepEqual(requestBody.include, ["reasoning.encrypted_content"]);
assert.deepEqual(requestBody.input.at(-1), { type: "compaction_trigger" });
assert.deepEqual(requestBody.reasoning, { effort: "high", summary: "auto" });
assert.deepEqual(requestBody.text, { verbosity: "medium" });
assert.equal(
  remoteCompactionV2EndpointUrl({
    provider: "openai",
    api: "openai-responses",
    baseUrl: "https://api.openai.com/v1",
  }),
  "https://api.openai.com/v1/responses",
);
assert.equal(
  remoteCompactionV2EndpointUrl({
    provider: "openai-codex",
    api: "openai-codex-responses",
    baseUrl: "https://chatgpt.com/backend-api",
  }),
  "https://chatgpt.com/backend-api/codex/responses",
);

const parsedV2Events = parseRemoteCompactionV2Events([
  {
    type: "response.output_item.done",
    item: { type: "compaction", encrypted_content: "V2_ENCRYPTED" },
  },
  {
    type: "response.completed",
    response: { usage: { input_tokens: 10, output_tokens: 2, total_tokens: 12 } },
  },
]);
assert.equal(parsedV2Events.compactionItem.type, "compaction");

const observedServerError = {
  type: "error",
  error: {
    type: "server_error",
    code: "server_error",
    message: "An error occurred while processing your request. You can retry your request.",
    param: null,
  },
  sequence_number: 2,
};
assert.throws(
  () => parseRemoteCompactionV2Events([observedServerError]),
  /\[server_error\].*You can retry your request/,
  "nested Responses errors should retain their actionable provider message",
);

function responseEventStream(events) {
  return new Response(
    `${events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join("")}data: [DONE]\n\n`,
    { status: 200, headers: { "content-type": "text/event-stream" } },
  );
}

const remoteRequest = {
  model: {
    provider: "openai",
    api: "openai-responses",
    id: "gpt-5.4-nano",
    baseUrl: "https://api.openai.com/v1",
    input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  },
  apiKey: "sk-test",
  input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "retain" }] }],
  tools: [],
  parallelToolCalls: true,
};
const successfulCompactionEvents = [
  {
    type: "response.output_item.done",
    item: { type: "compaction", encrypted_content: "RETRIED_ENCRYPTED" },
  },
  {
    type: "response.completed",
    response: { usage: { input_tokens: 10, output_tokens: 2, total_tokens: 12 } },
  },
];

let transientAttempts = 0;
const transientRetryDelays = [];
const retriedCompaction = await callRemoteCompactionEndpoint({
  ...remoteRequest,
  maxRetries: 2,
  retryBaseDelayMs: 25,
  fetchImpl: async () => {
    transientAttempts += 1;
    return transientAttempts < 3
      ? responseEventStream([observedServerError])
      : responseEventStream(successfulCompactionEvents);
  },
  sleepImpl: async (delayMs) => {
    transientRetryDelays.push(delayMs);
  },
});
assert.equal(transientAttempts, 3, "server_error should be retried within the bounded budget");
assert.deepEqual(transientRetryDelays, [25, 50], "retry backoff should be exponential");
assert.equal(retriedCompaction.output.at(-1)?.type, "compaction");

let nonRetryableAttempts = 0;
await assert.rejects(
  callRemoteCompactionEndpoint({
    ...remoteRequest,
    maxRetries: 3,
    retryBaseDelayMs: 0,
    fetchImpl: async () => {
      nonRetryableAttempts += 1;
      return new Response(JSON.stringify({ error: { message: "Invalid request payload" } }), {
        status: 400,
        statusText: "Bad Request",
      });
    },
    sleepImpl: async () => {
      throw new Error("non-retryable failures must not sleep");
    },
  }),
  /failed \(400\): Invalid request payload/,
);
assert.equal(nonRetryableAttempts, 1, "deterministic 4xx errors should fail immediately");

let exhaustedAttempts = 0;
await assert.rejects(
  callRemoteCompactionEndpoint({
    ...remoteRequest,
    maxRetries: 2,
    retryBaseDelayMs: 0,
    fetchImpl: async () => {
      exhaustedAttempts += 1;
      return responseEventStream([observedServerError]);
    },
    sleepImpl: async () => {},
  }),
  /after 3 attempts/,
);
assert.equal(exhaustedAttempts, 3, "retry exhaustion should remain bounded");

let abortedAttempts = 0;
let abortedBackoffDelayMs;
const abortController = new AbortController();
const abortedCompaction = callRemoteCompactionEndpoint({
  ...remoteRequest,
  signal: abortController.signal,
  maxRetries: 3,
  retryBaseDelayMs: 10_000,
  fetchImpl: async () => {
    abortedAttempts += 1;
    return responseEventStream([observedServerError]);
  },
  sleepImpl: (delayMs, signal) => {
    abortedBackoffDelayMs = delayMs;
    const sleeping = sleepForRemoteCompactionRetry(delayMs, signal);
    abortController.abort();
    return sleeping;
  },
});
await assert.rejects(abortedCompaction, /aborted/i);
assert.equal(abortedBackoffDelayMs, 10_000, "abort coverage should run inside the real backoff sleep");
assert.equal(abortedAttempts, 1, "abort during retry backoff should prevent another request");

let cappedBackoffAttempts = 0;
const cappedBackoffDelays = [];
await callRemoteCompactionEndpoint({
  ...remoteRequest,
  maxRetries: 2,
  retryBaseDelayMs: 60_000,
  fetchImpl: async () => {
    cappedBackoffAttempts += 1;
    return cappedBackoffAttempts < 3
      ? responseEventStream([observedServerError])
      : responseEventStream(successfulCompactionEvents);
  },
  sleepImpl: async (delayMs) => {
    cappedBackoffDelays.push(delayMs);
  },
});
assert.deepEqual(cappedBackoffDelays, [60_000, 60_000], "local exponential backoff should clamp to the ceiling");
assert.equal(cappedBackoffAttempts, 3, "clamped local backoff should not end retries early");

await assert.rejects(
  callRemoteCompactionEndpoint({
    ...remoteRequest,
    maxRetries: 2,
    retryBaseDelayMs: 1,
    fetchImpl: async () =>
      new Response("upstream is saturated", {
        status: 503,
        headers: { "retry-after": "3600" },
      }),
    sleepImpl: async () => {
      throw new Error("excessive provider retry-after must not sleep");
    },
  }),
  (error) => {
    assert.match(error.message, /failed \(503\): upstream is saturated/);
    assert.match(error.message, /provider requested a 3600s retry delay, maximum 60s/);
    assert.match(error.cause?.message ?? "", /failed \(503\): upstream is saturated/);
    return true;
  },
);

let retryAfterAttempts = 0;
const retryAfterDelays = [];
await callRemoteCompactionEndpoint({
  ...remoteRequest,
  maxRetries: 1,
  retryBaseDelayMs: 1,
  fetchImpl: async () => {
    retryAfterAttempts += 1;
    return retryAfterAttempts === 1
      ? new Response("temporary outage", {
          status: 503,
          headers: { "retry-after-ms": "125" },
        })
      : responseEventStream(successfulCompactionEvents);
  },
  sleepImpl: async (delayMs) => {
    retryAfterDelays.push(delayMs);
  },
});
assert.deepEqual(retryAfterDelays, [125], "provider retry-after guidance should override local backoff");

const v2History = buildRemoteCompactionV2History(
  [
    { type: "message", role: "user", content: [{ type: "input_text", text: "retain user" }] },
    { type: "message", role: "assistant", content: [{ type: "output_text", text: "summarize assistant" }] },
  ],
  parsedV2Events.compactionItem,
);
assert.deepEqual(v2History.map((item) => item.type), ["message", "compaction"]);
assert.equal(v2History[0].role, "user");

const normalizedPromptItems = normalizeResponseItemsForPrompt(
  [
    { type: "ghost_snapshot", data: "hidden" },
    {
      type: "message",
      role: "user",
      content: [{ type: "input_image", image_url: "data:image/png;base64,AAAA" }],
    },
    { type: "function_call", name: "read", call_id: "call-1", arguments: "{}" },
    { type: "function_call_output", call_id: "orphan", output: "drop" },
    { type: "image_generation_call", result: "base64" },
  ],
  { input: ["text"] },
);
assert.equal(normalizedPromptItems[0].type, "message");
assert.deepEqual(normalizedPromptItems[0].content, [
  { type: "input_text", text: "image content omitted because you do not support image input" },
]);
assert.deepEqual(normalizedPromptItems[2], {
  type: "function_call_output",
  call_id: "call-1",
  output: "aborted",
});
assert.equal(normalizedPromptItems[3].result, "");
assert.doesNotMatch(JSON.stringify(normalizedPromptItems), /orphan|ghost_snapshot/);

const compactedHistory = processCompactedHistory([
  { type: "message", role: "developer", content: [{ type: "input_text", text: "drop developer" }] },
  { type: "message", role: "user", content: [] },
  { type: "message", role: "user", content: [{ type: "input_text", text: "keep user" }] },
  { type: "message", role: "assistant", content: [{ type: "output_text", text: "keep assistant" }] },
  { type: "function_call", name: "read", call_id: "call-2", arguments: "{}" },
  { type: "compaction", encrypted_content: "keep" },
]);
assert.deepEqual(compactedHistory.map((item) => item.type), ["message", "message", "compaction"]);
assert.equal(compactedHistory[0].role, "user");
assert.equal(compactedHistory[1].role, "assistant");

const compactionHeaders = buildRemoteCompactionHeaders({
  model: {
    provider: "openai",
    api: "openai-responses",
    id: "gpt-5.4-nano",
  },
  apiKey: "sk-test",
  sessionId: "session-123",
  headers: { "x-extra": "yes" },
});
assert.equal(compactionHeaders.authorization, "Bearer sk-test");
assert.equal(compactionHeaders.session_id, "session-123");
assert.equal(compactionHeaders["x-codex-window-id"], "session-123:0");
assert.match(compactionHeaders["x-codex-installation-id"], /^[0-9a-f-]{36}$/);
assert.equal(compactionHeaders["x-extra"], "yes");
assert.equal(compactionHeaders["x-codex-beta-features"], "remote_compaction_v2");
assert.equal(compactionHeaders.accept, "text/event-stream");

const websocketHeaders = buildCodexWebSocketHeaders("session-123");
assert.equal(websocketHeaders["x-client-request-id"], "session-123");
assert.equal(websocketHeaders.session_id, "session-123");
assert.equal(websocketHeaders["x-codex-window-id"], "session-123:0");

const detailsRoundTrip = extractRemoteCompactionDetails({
  remoteCompaction: buildRemoteCompactionDetails(
    {
      provider: "openai",
      api: "openai-responses",
      id: "gpt-5.4-nano",
    },
    [{ type: "compaction", encrypted_content: "ENCRYPTED" }],
    {
      input: 10,
      output: 20,
      cacheRead: 30,
      cacheWrite: 40,
      totalTokens: 100,
      cost: { input: 1, output: 2, cacheRead: 3, cacheWrite: 4, total: 10 },
    },
  ),
});
assert.ok(detailsRoundTrip, "expected remote compaction details round trip");
assert.equal(detailsRoundTrip.usage?.cacheWrite, 40);
assert.equal(detailsRoundTrip.usage?.cost.total, 10);

const incrementalInput = selectInputItemsForContinuation({
  context: {
    messages: [
      {
        role: "user",
        content: [{ type: "text", text: "old user" }],
      },
      {
        role: "assistant",
        content: [{ type: "text", text: "old assistant" }],
      },
      {
        role: "user",
        content: [{ type: "text", text: "new user" }],
      },
    ],
  },
  model: { input: ["text"] },
  session: { lastContextLength: 2 },
  currentModelKey: targetModelKey,
  remoteCompactionState: undefined,
  previousResponseId: "resp_123",
});
assert.deepEqual(incrementalInput, [
  {
    type: "message",
    role: "user",
    content: "new user",
  },
]);

// --- Retry-exhaustion warning is surfaced on a surface the TUI cannot wipe ---
//
// Pi's compaction_end handler clears and rebuilds the chat container, so a
// ui.notify warning is destroyed whichever compaction hook emits it. The warning
// is withheld until session_compact (where the committed entry can be checked)
// and then written to an extension widget, which lives outside that container.
assert.equal(isTextOnlyFallbackCompaction({ fromHook: true }), true);
assert.equal(isTextOnlyFallbackCompaction({ fromHook: true, details: { localSummaryDetails: {} } }), true);
assert.equal(
  isTextOnlyFallbackCompaction({ fromHook: true, details: { remoteCompaction: { version: 1 } } }),
  false,
  "a compaction carrying opaque continuity is not a text-only fallback",
);
assert.equal(isTextOnlyFallbackCompaction({ details: {} }), false, "pi-generated compactions are not our fallback");

const compactionModel = {
  provider: "openai",
  api: "openai-responses",
  id: "gpt-5.4-nano",
  name: "gpt-5.4-nano",
  baseUrl: "https://api.openai.com/v1",
  input: ["text"],
  contextWindow: 400000,
  maxTokens: 100000,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
};

function installExtension() {
  const handlers = new Map();
  extensionFactory({
    registerProvider: () => {},
    on: (event, handler) => handlers.set(event, handler),
    getAllTools: () => [],
    getActiveTools: () => [],
    getThinkingLevel: () => undefined,
  });
  return handlers;
}

/**
 * `widgets` mirrors the durable extension-widget container; `notices` records
 * ui.notify calls, which the TUI wipes on compaction_end and so must stay empty.
 */
function createHookContext(sessionId, notices, widgets) {
  return {
    cwd: repoRoot,
    mode: "tui",
    hasUI: true,
    ui: {
      notify: (message, level) => notices.push({ message, level }),
      setWidget: (key, content) => {
        if (content === undefined) widgets.delete(key);
        else widgets.set(key, content);
      },
      theme: { fg: (_color, text) => text },
    },
    model: compactionModel,
    modelRegistry: { getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "sk-test" }) },
    sessionManager: { getSessionId: () => sessionId, getBranch: () => [] },
    getSystemPrompt: () => "system",
  };
}

function beforeCompactEvent() {
  return {
    type: "session_before_compact",
    preparation: { firstKeptEntryId: "entry-1", tokensBefore: 1000 },
    branchEntries: [
      { type: "message", id: "entry-0", message: { role: "user", content: [{ type: "text", text: "remember me" }] } },
    ],
    reason: "manual",
    willRetry: false,
    signal: new AbortController().signal,
  };
}

function compactEvent(compactionEntry) {
  return { type: "session_compact", compactionEntry, fromExtension: true, reason: "manual", willRetry: false };
}

const localSummaryEvents = [
  {
    type: "response.output_item.done",
    item: { type: "message", role: "assistant", content: [{ type: "output_text", text: "LOCAL_TEXT_SUMMARY" }] },
  },
  { type: "response.completed", response: { usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 } } },
];

const originalFetch = globalThis.fetch;
const previousCompactionEnv = {
  PI_OPENAI_SERVER_COMPACTION_ENABLED: process.env.PI_OPENAI_SERVER_COMPACTION_ENABLED,
  PI_OPENAI_SERVER_COMPACTION_MAX_RETRIES: process.env.PI_OPENAI_SERVER_COMPACTION_MAX_RETRIES,
  PI_OPENAI_SERVER_COMPACTION_RETRY_BASE_DELAY_MS:
    process.env.PI_OPENAI_SERVER_COMPACTION_RETRY_BASE_DELAY_MS,
};
process.env.PI_OPENAI_SERVER_COMPACTION_ENABLED = "1";
process.env.PI_OPENAI_SERVER_COMPACTION_MAX_RETRIES = "0";
process.env.PI_OPENAI_SERVER_COMPACTION_RETRY_BASE_DELAY_MS = "0";

/** Routes remote compaction requests separately from the local summary request. */
function stubFetch({ remoteCompactionSucceeds }) {
  globalThis.fetch = async (_url, init) => {
    const body = typeof init?.body === "string" ? init.body : "";
    if (!body.includes("compaction_trigger")) {
      return responseEventStream(localSummaryEvents);
    }
    return responseEventStream(
      remoteCompactionSucceeds ? successfulCompactionEvents : [observedServerError],
    );
  };
}

const warningWidgetKey = "openai-server-compaction:continuity-downgrade";
/** The single line the extension writes into its durable widget, if any. */
function widgetWarning(widgets) {
  return widgets.get(warningWidgetKey)?.join("\n");
}

try {
  // Retry exhaustion: the warning is withheld during the hook and written to the
  // durable widget once the compaction entry has been committed.
  stubFetch({ remoteCompactionSucceeds: false });
  let handlers = installExtension();
  let notices = [];
  let widgets = new Map();
  let ctx = createHookContext("session-warn", notices, widgets);

  const fallbackResult = await handlers.get("session_before_compact")(beforeCompactEvent(), ctx);
  assert.equal(fallbackResult.compaction.summary, "LOCAL_TEXT_SUMMARY");
  assert.equal(
    fallbackResult.compaction.details?.remoteCompaction,
    undefined,
    "an exhausted retry budget must not claim remote continuity",
  );
  assert.equal(widgetWarning(widgets), undefined, "the warning must wait until the compaction is committed");

  const fallbackEntry = {
    type: "compaction",
    id: "cmp-fallback",
    fromHook: true,
    summary: fallbackResult.compaction.summary,
    details: fallbackResult.compaction.details,
  };
  handlers.get("session_compact")(compactEvent(fallbackEntry), ctx);
  const warning = widgetWarning(widgets);
  assert.ok(warning, "the warning should surface once the compaction is committed");
  assert.match(warning, /^Warning: /);
  assert.match(warning, /Opaque continuity was not preserved/);
  assert.match(warning, /\[server_error\]/, "the provider failure detail should survive the hook boundary");
  assert.deepEqual(
    notices,
    [],
    "the warning must not go through ui.notify, which compaction_end wipes from the chat container",
  );

  // A committed compaction that still carries opaque continuity must not be
  // labelled a downgrade even if a warning is pending against it.
  widgets.clear();
  await handlers.get("session_before_compact")(beforeCompactEvent(), ctx);
  handlers.get("session_compact")(
    compactEvent({ ...fallbackEntry, details: { remoteCompaction: { version: 1 } } }),
    ctx,
  );
  assert.equal(widgetWarning(widgets), undefined, "a preserved-continuity entry must not raise the banner");

  // The banner is retracted when a later compaction starts, so a stale downgrade
  // cannot keep claiming continuity is degraded.
  await handlers.get("session_before_compact")(beforeCompactEvent(), ctx);
  handlers.get("session_compact")(compactEvent(fallbackEntry), ctx);
  assert.ok(widgetWarning(widgets), "expected the banner after a fresh downgrade");
  await handlers.get("session_before_compact")(beforeCompactEvent(), ctx);
  assert.equal(widgetWarning(widgets), undefined, "a new compaction must retract the previous banner");

  handlers.get("session_compact")(compactEvent(fallbackEntry), ctx);
  widgets.clear();
  handlers.get("session_compact")(compactEvent(fallbackEntry), ctx);
  assert.equal(widgetWarning(widgets), undefined, "a pending warning must be emitted at most once");

  // A warning recorded for an abandoned compaction must not leak into a later one.
  widgets.clear();
  await handlers.get("session_before_compact")(beforeCompactEvent(), ctx);
  handlers.get("session_before_switch")({ type: "session_before_switch", reason: "new" }, ctx);
  handlers.get("session_compact")(compactEvent(fallbackEntry), ctx);
  assert.equal(widgetWarning(widgets), undefined, "warnings from an abandoned compaction must not resurface");

  // Switching away from a downgraded session retracts its banner.
  await handlers.get("session_before_compact")(beforeCompactEvent(), ctx);
  handlers.get("session_compact")(compactEvent(fallbackEntry), ctx);
  assert.ok(widgetWarning(widgets), "expected the banner before the session change");
  handlers.get("session_before_switch")({ type: "session_before_switch", reason: "new" }, ctx);
  assert.equal(widgetWarning(widgets), undefined, "the banner must not follow the user into another session");

  // Successful remote compaction stays silent.
  stubFetch({ remoteCompactionSucceeds: true });
  handlers = installExtension();
  notices = [];
  widgets = new Map();
  ctx = createHookContext("session-ok", notices, widgets);

  const remoteResult = await handlers.get("session_before_compact")(beforeCompactEvent(), ctx);
  assert.ok(remoteResult.compaction.details.remoteCompaction, "expected opaque remote continuity");
  handlers.get("session_compact")(
    compactEvent({
      type: "compaction",
      id: "cmp-remote",
      fromHook: true,
      summary: remoteResult.compaction.summary,
      details: remoteResult.compaction.details,
    }),
    ctx,
  );
  assert.equal(widgetWarning(widgets), undefined, "preserved continuity must not warn about a downgrade");
  assert.deepEqual(notices, [], "preserved continuity must not warn about a downgrade");
} finally {
  globalThis.fetch = originalFetch;
  for (const [key, value] of Object.entries(previousCompactionEnv)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
}

console.log("smoke ok");
