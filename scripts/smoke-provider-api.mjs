import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { findPackageJSON } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";
import { WebSocketServer } from "ws";
import { getCurrentSystemPrompt, getCurrentTools, normalizeContext } from "@earendil-works/pi-ai";
import * as piAi from "@earendil-works/pi-ai/compat";
import {
  buildCodexWebSocketHeaders,
  buildRemoteCompactionHeaders,
  generateBestEffortLocalSummary,
} from "../src/remote-compaction.ts";
import { createOpenAIWebSocketStreamFn, releaseWsSession } from "../src/openai-ws-stream.ts";

async function testCompactionHeaders() {
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

  const codexKey = `test.${Buffer.from(JSON.stringify({
    "https://api.openai.com/auth": { chatgpt_account_id: "test-account" },
  })).toString("base64url")}.test`;
  for (const model of [
    { provider: "openai", api: "openai-responses", id: "test-model" },
    { provider: "openai-codex", api: "openai-codex-responses", id: "test-model" },
  ]) {
    const params = { model, apiKey: codexKey, sessionId: "session-123" };
    const defaults = buildRemoteCompactionHeaders(params);
    for (const name of Object.keys(defaults)) {
      const removed = buildRemoteCompactionHeaders({ ...params, headers: { [name.toUpperCase()]: null } });
      assert.equal(new Headers(removed).has(name), false, `${model.api}: null must suppress ${name}`);
      assert.deepEqual(removed, Object.fromEntries(Object.entries(defaults).filter(([key]) => key !== name)));
      const replaced = buildRemoteCompactionHeaders({ ...params, headers: { [name.toUpperCase()]: "override" } });
      assert.equal(new Headers(replaced).get(name), name === "x-codex-beta-features"
        ? "override,remote_compaction_v2" : "override");
      assert.equal(Object.keys(replaced).length, Object.keys(defaults).length);
    }
    const extras = buildRemoteCompactionHeaders({
      ...params,
      headers: { "X-Extra": "yes", "X-Absent": null, "X-Codex-Beta-Features": "other,remote_compaction_v2,other" },
    });
    assert.equal(extras["x-extra"], "yes");
    assert.equal(extras["x-absent"], undefined);
    assert.equal(extras["x-codex-beta-features"], "other,remote_compaction_v2");
  }
}

async function testSummaryHeaders() {
  const compactionAiPackage = findPackageJSON("@earendil-works/pi-ai", import.meta.resolve("@earendil-works/pi-coding-agent"));
  const compactionAi = await import(new URL("./dist/compat.js", pathToFileURL(compactionAiPackage)).href);
  const registries = new Set([piAi, compactionAi]);
  const api = "compaction-header-test";
  const model = { api, provider: "test", id: "test", reasoning: false, maxTokens: 4096 };
  const headers = { session_id: null, Authorization: null, "x-extra": "yes" };
  const messages = [{ role: "user", content: [{ type: "text", text: "remember me" }], timestamp: 0 }];
  for (const mode of ["portable", "fallback", "split-fallback"]) {
    const received = [];
    const summaryStream = (_model, _context, options) => {
      received.push(options.headers);
      return { result: async () => ({
        role: "assistant",
        content: [{ type: "text", text: "LOCAL_SUMMARY" }],
        stopReason: "stop",
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      }) };
    };
    const provider = {
      api,
      stream: (...args) => {
        if (mode === "portable") return summaryStream(...args);
        received.push(args[2].headers);
        throw new Error("Portable summary unavailable");
      },
      streamSimple: summaryStream,
    };
    for (const registry of registries) registry.registerApiProvider(provider, api);
    try {
      const result = await generateBestEffortLocalSummary({
        model, apiKey: "test-key", headers, messages, firstKeptEntryId: "entry-1", tokensBefore: 1000,
        preparation: {
          firstKeptEntryId: "entry-1", tokensBefore: 1000, messagesToSummarize: messages,
          turnPrefixMessages: mode === "split-fallback" ? messages : [],
          isSplitTurn: mode === "split-fallback",
          fileOps: { read: new Set(), written: new Set(), edited: new Set() },
          settings: { enabled: true, reserveTokens: 4096, keepRecentTokens: 100 },
        },
      });
      assert.match(result.summary, /LOCAL_SUMMARY/);
      assert.equal(result.firstKeptEntryId, "entry-1");
      assert.equal(received.length, mode === "portable" ? 1 : mode === "fallback" ? 2 : 3);
      for (const receivedHeaders of received) assert.deepEqual(receivedHeaders, headers);
    } finally {
      for (const registry of registries) registry.unregisterApiProviders(api);
    }
  }
}

async function testTranscriptWebSocket() {
  const transcript = normalizeContext({
    systemPrompt: "TRANSCRIPT_SYSTEM_PROMPT",
    tools: [{
      name: "transcript_tool",
      description: "Tool declared in Pi's transcript",
      parameters: { type: "object", properties: {} },
    }],
    messages: [{ role: "user", content: "hello from transcript", timestamp: 0 }],
  });
  const emptyTranscript = normalizeContext({ messages: [] });
  assert.equal(getCurrentSystemPrompt(emptyTranscript.messages), "");
  assert.deepEqual(getCurrentTools(emptyTranscript.messages), []);

  const previousEnv = {
    PI_OPENAI_SERVER_COMPACTION_ENABLED: process.env.PI_OPENAI_SERVER_COMPACTION_ENABLED,
    PI_OPENAI_SERVER_COMPACTION_PREVIOUS_RESPONSE_ID: process.env.PI_OPENAI_SERVER_COMPACTION_PREVIOUS_RESPONSE_ID,
  };
  const originalFetch = globalThis.fetch;
  const controller = new AbortController();
  let timeoutId;
  const deadline = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      controller.abort();
      reject(new Error("Transcript WebSocket fixture timed out"));
    }, 5000);
  });
  const websocketServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  const requests = [];
  websocketServer.once("connection", (socket) => {
    socket.on("message", (message) => {
      requests.push(JSON.parse(message.toString()));
      socket.send(JSON.stringify({
        type: "response.completed",
        response: {
          id: `resp_transcript_test_${requests.length}`,
          object: "response",
          created_at: 0,
          status: "completed",
          model: "gpt-transcript-test",
          output: [],
          usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
        },
      }));
    });
  });

  try {
    process.env.PI_OPENAI_SERVER_COMPACTION_ENABLED = "1";
    process.env.PI_OPENAI_SERVER_COMPACTION_PREVIOUS_RESPONSE_ID = "1";
    globalThis.fetch = async () => {
      throw new Error("Transcript WebSocket fixture must not use HTTP");
    };
    await Promise.race([once(websocketServer, "listening", { signal: controller.signal }), deadline]);
    const { port } = websocketServer.address();
    const model = {
      api: "openai-responses",
      provider: "openai",
      id: "gpt-transcript-test",
      baseUrl: "https://api.openai.com/v1",
      reasoning: false,
      input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    };
    const stream = createOpenAIWebSocketStreamFn({ url: `ws://127.0.0.1:${port}` })(
      model,
      transcript,
      { apiKey: "test-key", sessionId: "transcript-test", transport: "websocket", signal: controller.signal },
    );
    const result = await Promise.race([stream.result(), deadline]);
    assert.equal(result.stopReason, "stop", result.errorMessage);
    const emptyStream = createOpenAIWebSocketStreamFn({ url: `ws://127.0.0.1:${port}` })(
      model,
      emptyTranscript,
      { apiKey: "test-key", sessionId: "transcript-test", transport: "websocket", signal: controller.signal },
    );
    const emptyResult = await Promise.race([emptyStream.result(), deadline]);
    assert.equal(emptyResult.stopReason, "stop", emptyResult.errorMessage);
    assert.equal(requests.length, 2);

    assert.equal(requests[0].type, "response.create");
    assert.equal(requests[0].instructions, "TRANSCRIPT_SYSTEM_PROMPT");
    assert.deepEqual(requests[0].tools, [{
      type: "function",
      name: "transcript_tool",
      description: "Tool declared in Pi's transcript",
      parameters: { type: "object", properties: {} },
    }]);
    assert.equal(requests[1].instructions, undefined);
    assert.equal(requests[1].tools, undefined);
  } finally {
    clearTimeout(timeoutId);
    controller.abort();
    globalThis.fetch = originalFetch;
    for (const [key, value] of Object.entries(previousEnv)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    releaseWsSession("transcript-test");
    for (const socket of websocketServer.clients) socket.terminate();
    await new Promise((resolve) => websocketServer.close(resolve));
  }
}

const scratchRoot = fileURLToPath(new URL("../.pi/", import.meta.url));
mkdirSync(scratchRoot, { recursive: true });
const testHome = mkdtempSync(`${scratchRoot}provider-api-`);
const previousCodexHome = process.env.CODEX_HOME;
process.env.CODEX_HOME = testHome;
try {
  await testCompactionHeaders();
  await testSummaryHeaders();
  await testTranscriptWebSocket();
} finally {
  if (previousCodexHome === undefined) delete process.env.CODEX_HOME;
  else process.env.CODEX_HOME = previousCodexHome;
  rmSync(testHome, { recursive: true, force: true });
}
console.log("provider API smoke ok");
