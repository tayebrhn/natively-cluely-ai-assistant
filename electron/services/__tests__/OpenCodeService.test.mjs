// electron/services/__tests__/OpenCodeService.test.mjs
//
// Unit tests for OpenCodeService — the fetch-based HTTP client for a running
// `opencode serve` instance. Unlike the Codex provider (which authenticates a
// ChatGPT subscription against a hosted endpoint), OpenCode is a local agent
// server the user runs themselves; Natively is a plain HTTP client of it.
//
// The service is pure `fetch()` + SSE with ZERO process.platform branches, so
// macOS and Windows exercise byte-for-byte the same code path (see CLAUDE.md).
// These tests install a `globalThis.fetch` stub and drive the public surface
// (normalizeConfig / run / stream), which in turn exercises every module-private
// helper (splitModel, buildPromptBody, buildHeaders, extractPartDelta,
// parseSseEvent, extractTextFromParts) without needing to export them.
//
// What's covered:
//   1. normalizeConfig — defaults, timeout coercion, baseUrl slash-strip, username
//   2. isOpenCodeConnectionError — matches the two actionable messages
//   3. request-body shape — parts[], model split into { providerID, modelID }
//   4. system prompt retains system priority and all coding tools are disabled
//   5. Basic-auth header present only when a password is set
//   6. stream() — SSE 'delta' mode and 'diff' (full-text) mode
//   7. run() — synchronous /message endpoint text extraction
//   8. stream() → run() fallback when the event bus fails before first token
//   9. pre-aborted signal throws
//
// Run via: npm run build:electron && node --test electron/services/__tests__/OpenCodeService.test.mjs

import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const compiledPath = path.resolve(__dirname, '../../../dist-electron/electron/services/OpenCodeService.js');
const mod = await import(pathToFileURL(compiledPath).href);
const {
  OpenCodeService,
  DEFAULT_OPENCODE_CONFIG,
  OPENCODE_NOT_CONFIGURED_MESSAGE,
  OPENCODE_CONNECTION_FAILED_MESSAGE,
  isOpenCodeConnectionError,
} = mod;

const BASE = 'http://127.0.0.1:4096';

// =============================================================================
// Fetch stub + response builders
// =============================================================================

/** Install a fetch stub that routes each call through `handler(call, init)`.
 *  Returns { calls, restore }. Always restore in a finally. */
function installFetch(handler) {
  const original = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, init = {}) => {
    const method = (init.method || 'GET').toUpperCase();
    const call = { url: String(url), method, headers: init.headers || {}, body: init.body };
    calls.push(call);
    return handler(call, init);
  };
  return {
    calls,
    restore: () => { globalThis.fetch = original; },
  };
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

/** Build an SSE Response whose body emits `events` (each a full `data: …\n\n`
 *  frame) then closes cleanly. */
function sseResponse(events, status = 200) {
  const encoder = new TextEncoder();
  const chunks = events.map(e => encoder.encode(e));
  const stream = new ReadableStream({
    start(controller) {
      for (const c of chunks) controller.enqueue(c);
      controller.close();
    },
  });
  return new Response(stream, { status, headers: { 'Content-Type': 'text/event-stream' } });
}

/** Delay event creation until after prompt_async has been called. The production
 * client opens GET /event first, then submits the prompt with its messageID. */
function deferredSseResponse(makeEvents) {
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    start(controller) {
      setImmediate(() => {
        for (const event of makeEvents()) controller.enqueue(encoder.encode(event));
        controller.close();
      });
    },
  });
  return new Response(stream, { status: 200, headers: { 'Content-Type': 'text/event-stream' } });
}

function sseFrame(obj) {
  return `data: ${JSON.stringify(obj)}\n\n`;
}

/** Let fire-and-forget DELETE session cleanup run against the stub before the
 *  caller restores the real fetch. */
async function flushMicrotasks() {
  await new Promise(r => setImmediate(r));
}

async function drain(gen) {
  const out = [];
  for await (const d of gen) out.push(d);
  return out;
}

// =============================================================================
// normalizeConfig
// =============================================================================

test('DEFAULT_OPENCODE_CONFIG has the expected shape', () => {
  assert.equal(DEFAULT_OPENCODE_CONFIG.enabled, false);
  assert.equal(DEFAULT_OPENCODE_CONFIG.baseUrl, 'http://127.0.0.1:4096');
  assert.equal(DEFAULT_OPENCODE_CONFIG.username, 'opencode');
  assert.equal(DEFAULT_OPENCODE_CONFIG.model, '');
  assert.equal(DEFAULT_OPENCODE_CONFIG.fastModel, '');
  assert.equal(DEFAULT_OPENCODE_CONFIG.timeoutMs, 120_000);
});

test('normalizeConfig: empty input returns defaults', () => {
  assert.deepEqual(OpenCodeService.normalizeConfig({}), DEFAULT_OPENCODE_CONFIG);
  assert.deepEqual(OpenCodeService.normalizeConfig(), DEFAULT_OPENCODE_CONFIG);
});

test('normalizeConfig: enabled is coerced to boolean', () => {
  assert.equal(OpenCodeService.normalizeConfig({ enabled: 1 }).enabled, true);
  assert.equal(OpenCodeService.normalizeConfig({ enabled: 0 }).enabled, false);
  assert.equal(OpenCodeService.normalizeConfig({ enabled: 'yes' }).enabled, true);
});

test('normalizeConfig: strips a trailing slash from baseUrl, falls back to default', () => {
  assert.equal(OpenCodeService.normalizeConfig({ baseUrl: 'http://localhost:9999/' }).baseUrl, 'http://localhost:9999');
  assert.equal(OpenCodeService.normalizeConfig({ baseUrl: 'http://localhost:9999///' }).baseUrl, 'http://localhost:9999');
  assert.equal(OpenCodeService.normalizeConfig({ baseUrl: '   ' }).baseUrl, 'http://127.0.0.1:4096');
  assert.equal(OpenCodeService.normalizeConfig({ baseUrl: undefined }).baseUrl, 'http://127.0.0.1:4096');
});

test('normalizeConfig: username falls back to "opencode" when blank', () => {
  assert.equal(OpenCodeService.normalizeConfig({ username: '  ' }).username, 'opencode');
  assert.equal(OpenCodeService.normalizeConfig({ username: 'me' }).username, 'me');
});

test('normalizeConfig: model/fastModel are trimmed; empty stays empty', () => {
  assert.equal(OpenCodeService.normalizeConfig({ model: '  anthropic/claude  ' }).model, 'anthropic/claude');
  assert.equal(OpenCodeService.normalizeConfig({ model: '' }).model, '');
  assert.equal(OpenCodeService.normalizeConfig({ fastModel: '  openai/gpt  ' }).fastModel, 'openai/gpt');
});

test('normalizeConfig: invalid timeouts fall back to the default', () => {
  assert.equal(OpenCodeService.normalizeConfig({ timeoutMs: null }).timeoutMs, 120_000);
  assert.equal(OpenCodeService.normalizeConfig({ timeoutMs: -1 }).timeoutMs, 120_000);
  assert.equal(OpenCodeService.normalizeConfig({ timeoutMs: 0 }).timeoutMs, 120_000);
  assert.equal(OpenCodeService.normalizeConfig({ timeoutMs: 'abc' }).timeoutMs, 120_000);
  assert.equal(OpenCodeService.normalizeConfig({ timeoutMs: 30_000 }).timeoutMs, 30_000);
});

// =============================================================================
// isOpenCodeConnectionError
// =============================================================================

test('isOpenCodeConnectionError: matches the two actionable messages, nothing else', () => {
  assert.equal(isOpenCodeConnectionError(new Error(OPENCODE_NOT_CONFIGURED_MESSAGE)), true);
  assert.equal(isOpenCodeConnectionError(new Error(OPENCODE_CONNECTION_FAILED_MESSAGE)), true);
  assert.equal(isOpenCodeConnectionError(new Error('some other error')), false);
  assert.equal(isOpenCodeConnectionError(null), false);
  assert.equal(isOpenCodeConnectionError(undefined), false);
  assert.equal(isOpenCodeConnectionError('a string'), false);
});

// =============================================================================
// run() — synchronous /message endpoint
// =============================================================================

test('run: creates a session, POSTs /message, extracts joined text parts', async () => {
  const stub = installFetch((call) => {
    if (call.method === 'POST' && call.url === `${BASE}/session`) return jsonResponse({ id: 'sess-run' });
    if (call.method === 'POST' && call.url === `${BASE}/session/sess-run/message`) {
      return jsonResponse({ info: {}, parts: [
        { type: 'text', text: 'Hello ' },
        { type: 'reasoning', text: 'ignored' },
        { type: 'text', text: 'world' },
      ] });
    }
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    const text = await OpenCodeService.run('', { prompt: 'hi', baseUrl: BASE, timeoutMs: 5_000 });
    assert.equal(text, 'Hello world');
    await flushMicrotasks();
    // Throwaway session must be cleaned up.
    assert.ok(stub.calls.some(c => c.method === 'DELETE' && c.url === `${BASE}/session/sess-run`),
      'run() must best-effort DELETE the throwaway session');
  } finally {
    stub.restore();
  }
});

test('run: body carries the prompt as a text part, chat system, disabled tools, and no model when unset', async () => {
  const stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 's' });
    if (call.url === `${BASE}/session/s/message`) return jsonResponse({ parts: [{ type: 'text', text: 'ok' }] });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    await OpenCodeService.run('', { prompt: 'the question', baseUrl: BASE, timeoutMs: 5_000 });
    const msg = stub.calls.find(c => c.url === `${BASE}/session/s/message`);
    const body = JSON.parse(msg.body);
    assert.deepEqual(body.parts, [{ type: 'text', text: 'the question' }]);
    assert.match(body.system, /general-purpose conversational AI assistant/);
    assert.deepEqual(body.tools, { '*': false }, 'all OpenCode coding tools must be disabled');
    assert.equal(body.model, undefined, 'no model configured → body.model omitted');
  } finally {
    stub.restore();
  }
});

test('run: splits model on the FIRST slash into { providerID, modelID }', async () => {
  const stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 's' });
    if (call.url === `${BASE}/session/s/message`) return jsonResponse({ parts: [{ type: 'text', text: 'ok' }] });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    await OpenCodeService.run('', {
      prompt: 'q',
      baseUrl: BASE,
      timeoutMs: 5_000,
      // Model IDs can themselves contain slashes (openrouter-style); only the
      // FIRST slash separates provider from model.
      model: 'openrouter/anthropic/claude-3.5-sonnet',
    });
    const body = JSON.parse(stub.calls.find(c => c.url === `${BASE}/session/s/message`).body);
    assert.deepEqual(body.model, { providerID: 'openrouter', modelID: 'anthropic/claude-3.5-sonnet' });
  } finally {
    stub.restore();
  }
});

test('run: a model with no usable slash is omitted (server default is used)', async () => {
  const stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 's' });
    if (call.url === `${BASE}/session/s/message`) return jsonResponse({ parts: [{ type: 'text', text: 'ok' }] });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    for (const bad of ['nomeaning', '/leading', 'trailing/']) {
      stub.calls.length = 0;
      await OpenCodeService.run('', { prompt: 'q', baseUrl: BASE, timeoutMs: 5_000, model: bad });
      const body = JSON.parse(stub.calls.find(c => c.url === `${BASE}/session/s/message`).body);
      assert.equal(body.model, undefined, `model "${bad}" has no valid split → omitted`);
    }
  } finally {
    stub.restore();
  }
});

test('run: task instructions retain system priority instead of becoming user content', async () => {
  const stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 's' });
    if (call.url === `${BASE}/session/s/message`) return jsonResponse({ parts: [{ type: 'text', text: 'ok' }] });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    await OpenCodeService.run('', { prompt: 'user question', instructions: 'be terse', baseUrl: BASE, timeoutMs: 5_000 });
    const body = JSON.parse(stub.calls.find(c => c.url === `${BASE}/session/s/message`).body);
    assert.deepEqual(body.parts, [{ type: 'text', text: 'user question' }]);
    assert.match(body.system, /general-purpose conversational AI assistant/);
    assert.match(body.system, /be terse/);
    assert.deepEqual(body.tools, { '*': false });
  } finally {
    stub.restore();
  }
});

// =============================================================================
// Basic-auth header
// =============================================================================

test('Basic-auth header present only when a password is set', async () => {
  // With password.
  let stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 's' });
    if (call.url === `${BASE}/session/s/message`) return jsonResponse({ parts: [{ type: 'text', text: 'ok' }] });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    await OpenCodeService.run('', { prompt: 'q', baseUrl: BASE, timeoutMs: 5_000, username: 'me', password: 's3cret' });
    const expected = 'Basic ' + Buffer.from('me:s3cret').toString('base64');
    assert.equal(stub.calls[0].headers.Authorization, expected,
      'Authorization header must be HTTP Basic base64(user:pass)');
  } finally {
    stub.restore();
  }

  // Without password.
  stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 's' });
    if (call.url === `${BASE}/session/s/message`) return jsonResponse({ parts: [{ type: 'text', text: 'ok' }] });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    await OpenCodeService.run('', { prompt: 'q', baseUrl: BASE, timeoutMs: 5_000, username: 'me' });
    assert.equal(stub.calls[0].headers.Authorization, undefined,
      'no password → no Authorization header (username alone must not trigger auth)');
  } finally {
    stub.restore();
  }
});

// =============================================================================
// stream() — SSE event bus
// =============================================================================

test('stream: yields incremental text in delta mode, stops on session.idle', async () => {
  const stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 'sess-s' });
    if (call.url === `${BASE}/event` && call.method === 'GET') return deferredSseResponse(() => {
      const prompt = stub.calls.find(c => c.url.includes('/prompt_async'));
      const userID = JSON.parse(prompt.body).messageID;
      return [
        sseFrame({ type: 'server.connected', properties: {} }),
        sseFrame({ type: 'message.updated', properties: { info: { id: 'assistant-1', sessionID: 'sess-s', role: 'assistant', parentID: userID } } }),
        sseFrame({ type: 'message.part.updated', properties: { part: { type: 'text', id: 'p1', messageID: 'assistant-1', sessionID: 'sess-s', text: 'Hello ' }, delta: 'Hello ' } }),
        sseFrame({ type: 'message.part.updated', properties: { part: { type: 'text', id: 'p1', messageID: 'assistant-1', sessionID: 'sess-s', text: 'Hello world' }, delta: 'world' } }),
        sseFrame({ type: 'session.idle', properties: { sessionID: 'sess-s' } }),
      ];
    });
    if (call.url.includes('/prompt_async')) return jsonResponse({ ok: true });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    const out = await drain(OpenCodeService.stream('', { prompt: 'hi', baseUrl: BASE, timeoutMs: 5_000 }));
    assert.equal(out.join(''), 'Hello world');
    // The prompt is fired via prompt_async (not the blocking /message endpoint).
    const prompt = stub.calls.find(c => c.url.includes('/prompt_async'));
    assert.ok(prompt, 'stream() must POST to /prompt_async');
    const body = JSON.parse(prompt.body);
    assert.deepEqual(body.parts, [{ type: 'text', text: 'hi' }]);
    assert.match(body.messageID, /^msg_/, 'streaming prompts need a correlatable OpenCode message ID');
    assert.match(body.system, /general-purpose conversational AI assistant/);
    assert.deepEqual(body.tools, { '*': false }, 'streaming requests must disable all coding tools too');
    await flushMicrotasks();
  } finally {
    stub.restore();
  }
});

test('stream: diff mode — server sends full part.text each update, service emits the suffix', async () => {
  const stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 'sess-d' });
    if (call.url === `${BASE}/event` && call.method === 'GET') return deferredSseResponse(() => {
      const prompt = stub.calls.find(c => c.url.includes('/prompt_async'));
      const userID = JSON.parse(prompt.body).messageID;
      return [
        sseFrame({ type: 'message.updated', properties: { info: { id: 'assistant-d', sessionID: 'sess-d', role: 'assistant', parentID: userID } } }),
        // No `delta` field: emit only the appended tail of each full update.
        sseFrame({ type: 'message.part.updated', properties: { part: { type: 'text', id: 'p1', messageID: 'assistant-d', sessionID: 'sess-d', text: 'Hello ' } } }),
        sseFrame({ type: 'message.part.updated', properties: { part: { type: 'text', id: 'p1', messageID: 'assistant-d', sessionID: 'sess-d', text: 'Hello world' } } }),
        sseFrame({ type: 'session.idle', properties: { sessionID: 'sess-d' } }),
      ];
    });
    if (call.url.includes('/prompt_async')) return jsonResponse({ ok: true });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    const out = await drain(OpenCodeService.stream('', { prompt: 'hi', baseUrl: BASE, timeoutMs: 5_000 }));
    assert.equal(out.join(''), 'Hello world');
    assert.deepEqual(out, ['Hello ', 'world'], 'diff mode must emit only the newly-appended suffix');
    await flushMicrotasks();
  } finally {
    stub.restore();
  }
});

test('stream: excludes the submitted user prompt and emits only its assistant reply', async () => {
  const stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 'sess-role' });
    if (call.url === `${BASE}/event` && call.method === 'GET') return deferredSseResponse(() => {
      const prompt = stub.calls.find(c => c.url.includes('/prompt_async'));
      const userID = JSON.parse(prompt.body).messageID;
      return [
        sseFrame({ type: 'message.updated', properties: { info: { id: userID, sessionID: 'sess-role', role: 'user' } } }),
        sseFrame({ type: 'message.part.updated', properties: { part: { type: 'text', id: 'user-part', messageID: userID, sessionID: 'sess-role', text: '# Question\n\nhello' } } }),
        sseFrame({ type: 'message.updated', properties: { info: { id: 'assistant-role', sessionID: 'sess-role', role: 'assistant', parentID: userID } } }),
        sseFrame({ type: 'message.part.updated', properties: { part: { type: 'text', id: 'assistant-part', messageID: 'assistant-role', sessionID: 'sess-role', text: 'Hello! How can I help?' } } }),
        sseFrame({ type: 'session.idle', properties: { sessionID: 'sess-role' } }),
      ];
    });
    if (call.url.includes('/prompt_async')) return jsonResponse({ ok: true });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    const out = await drain(OpenCodeService.stream('', { prompt: '# Question\n\nhello', baseUrl: BASE, timeoutMs: 5_000 }));
    assert.deepEqual(out, ['Hello! How can I help?']);
  } finally {
    stub.restore();
  }
});

test('stream: supports message.part.delta without duplicating the final full part', async () => {
  const stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 'sess-v2' });
    if (call.url === `${BASE}/event` && call.method === 'GET') return deferredSseResponse(() => {
      const prompt = stub.calls.find(c => c.url.includes('/prompt_async'));
      const userID = JSON.parse(prompt.body).messageID;
      return [
        sseFrame({ type: 'message.updated', properties: { info: { id: 'assistant-v2', sessionID: 'sess-v2', role: 'assistant', parentID: userID } } }),
        sseFrame({ type: 'message.part.updated', properties: { part: { type: 'text', id: 'p-v2', messageID: 'assistant-v2', sessionID: 'sess-v2', text: '' } } }),
        sseFrame({ type: 'message.part.delta', properties: { sessionID: 'sess-v2', messageID: 'assistant-v2', partID: 'p-v2', field: 'text', delta: 'Hello ' } }),
        sseFrame({ type: 'message.part.delta', properties: { sessionID: 'sess-v2', messageID: 'assistant-v2', partID: 'p-v2', field: 'text', delta: 'world' } }),
        sseFrame({ type: 'message.part.updated', properties: { part: { type: 'text', id: 'p-v2', messageID: 'assistant-v2', sessionID: 'sess-v2', text: 'Hello world' } } }),
        sseFrame({ type: 'session.idle', properties: { sessionID: 'sess-v2' } }),
      ];
    });
    if (call.url.includes('/prompt_async')) return jsonResponse({ ok: true });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    const out = await drain(OpenCodeService.stream('', { prompt: 'hi', baseUrl: BASE, timeoutMs: 5_000 }));
    assert.deepEqual(out, ['Hello ', 'world']);
  } finally {
    stub.restore();
  }
});

test('stream: falls back to run() when the event bus fails before the first token', async () => {
  const stub = installFetch((call) => {
    if (call.url === `${BASE}/session` && call.method === 'POST') return jsonResponse({ id: 'sess-f' });
    // Event bus is unavailable (500) — streamViaEvents throws before any yield.
    if (call.url === `${BASE}/event` && call.method === 'GET') return sseResponse([], 500);
    // Fallback path: synchronous /message returns the whole answer.
    if (call.url === `${BASE}/session/sess-f/message`) return jsonResponse({ parts: [{ type: 'text', text: 'Fallback answer' }] });
    if (call.method === 'DELETE') return jsonResponse({ ok: true });
    throw new Error(`unexpected ${call.method} ${call.url}`);
  });
  try {
    const out = await drain(OpenCodeService.stream('', { prompt: 'hi', baseUrl: BASE, timeoutMs: 5_000 }));
    assert.equal(out.join(''), 'Fallback answer');
    assert.ok(stub.calls.some(c => c.url === `${BASE}/session/sess-f/message`),
      'fallback must hit the synchronous /message endpoint');
    await flushMicrotasks();
  } finally {
    stub.restore();
  }
});

// =============================================================================
// Abort handling
// =============================================================================

test('stream: a pre-aborted signal throws on first iteration', async () => {
  const ac = new AbortController();
  ac.abort();
  await assert.rejects(
    () => drain(OpenCodeService.stream('', { prompt: 'hi', baseUrl: BASE, timeoutMs: 5_000, signal: ac.signal })),
    err => /aborted/i.test(err.message),
  );
});

test('run: a pre-aborted signal throws before any request', async () => {
  const ac = new AbortController();
  ac.abort();
  await assert.rejects(
    () => OpenCodeService.run('', { prompt: 'hi', baseUrl: BASE, timeoutMs: 5_000, signal: ac.signal }),
    err => /aborted/i.test(err.message),
  );
});

test('stream: an empty baseUrl throws the "not configured" message', async () => {
  await assert.rejects(
    () => drain(OpenCodeService.stream('', { prompt: 'hi', baseUrl: '   ', timeoutMs: 5_000 })),
    err => err.message === OPENCODE_NOT_CONFIGURED_MESSAGE,
  );
});
