import { randomUUID } from 'node:crypto';

/**
 * OpenCodeService — HTTP client for a running `opencode serve` instance.
 *
 * WHAT THIS IS (and is NOT):
 *   OpenCode (opencode.ai, by SST) is a local coding agent. Running it starts a
 *   local HTTP server (default http://127.0.0.1:4096) that exposes an OpenAPI
 *   surface. Unlike the Codex provider — which authenticates a ChatGPT
 *   subscription and calls a HOSTED inference endpoint — OpenCode does NOT
 *   provide a model. It CONSUMES whatever provider you configured inside it via
 *   `opencode auth login`. Natively is therefore a *client* of the user's own
 *   running server; it does not spawn, bundle, or manage the opencode binary.
 *
 * CROSS-PLATFORM (see CLAUDE.md):
 *   This module is pure `fetch()` + SSE. There are NO `process.platform`
 *   branches, no child processes, no native modules, and no filesystem paths.
 *   macOS and Windows execute byte-for-byte the same code path — the only
 *   platform-variable thing is the server URL, which is user-supplied. That is
 *   the entire reason we connect to a running server instead of using the
 *   `@opencode-ai/sdk` `createOpencode()` helper (which spawns a local binary
 *   and would drag in binary-bundling + Windows-support questions).
 *
 * TWO TRANSPORTS, reliability-first:
 *   - streamViaEvents(): subscribe to `GET /event` (SSE bus), fire the prompt
 *     with `POST /session/:id/prompt_async`, and yield assistant text as
 *     `message.part.updated` events arrive; terminate on `session.idle`. This
 *     is the progressive-UI path.
 *   - runViaMessage(): `POST /session/:id/message`, which blocks until the
 *     assistant turn is complete and returns `{ info, parts }`; we extract the
 *     text parts. This is the simple, documented, reliable path.
 *   `stream()` tries the event path first and, if it fails BEFORE emitting any
 *   text, transparently falls back to `runViaMessage()` so the provider always
 *   produces output when the server is reachable. `run()` uses the message
 *   endpoint directly.
 *
 * Wire schema (pinned against the generated SDK types, sst/opencode
 * packages/sdk/js/src/gen/types.gen.ts):
 *   - Prompt body: { system, tools: {'*': false}, model?, parts: [{ type:'text', text }] }
 *   - Message event: type='message.updated', properties={ info: Message }
 *   - Stream events: 'message.part.updated' (full part / legacy delta) and
 *     'message.part.delta' (current incremental event)
 *   - Text part:    part.type='text', part.text: string, part.sessionID, part.id
 *   - Terminal:     type='session.idle' (properties.sessionID); 'session.error' → throw
 *   - Sync reply:   POST /session/:id/message → { info: AssistantMessage, parts: Part[] }
 */

// The two actionable, user-facing failures. Matched via isOpenCodeConnectionError()
// rather than re-typed at call sites so a reword here can't silently stop matching.
export const OPENCODE_NOT_CONFIGURED_MESSAGE =
  'OpenCode server URL is not configured. Set it in Settings → AI Providers → OpenCode.';
export const OPENCODE_CONNECTION_FAILED_MESSAGE =
  'Could not reach the OpenCode server. Make sure `opencode serve` is running and the URL is correct.';

/** True when `err` is one of the actionable OpenCode connection failures above. */
export function isOpenCodeConnectionError(err: unknown): boolean {
  const message = (err as { message?: unknown } | null | undefined)?.message;
  if (typeof message !== 'string') return false;
  return message.includes(OPENCODE_NOT_CONFIGURED_MESSAGE)
    || message.includes(OPENCODE_CONNECTION_FAILED_MESSAGE);
}

export interface OpenCodeConfig {
  enabled: boolean;
  /** Base URL of the running `opencode serve` instance, e.g. http://127.0.0.1:4096. */
  baseUrl: string;
  /** HTTP Basic auth username (opencode's default is "opencode"). Only used when
   *  a password is configured; the password itself is a secret and lives in
   *  CredentialsManager (keytar), never here. */
  username: string;
  /** Model as `providerID/modelID` (e.g. "anthropic/claude-3-5-sonnet-20241022").
   *  Empty string → omit `model` from the request so OpenCode uses the model its
   *  own default agent is configured with. */
  model: string;
  /** Fast-mode model, same `providerID/modelID` form (or empty for the default). */
  fastModel: string;
  /** Idle timeout in ms: abort if the server sends no bytes/events for this long.
   *  Not a wall-clock cap — it resets on every delta, so a slow-but-progressing
   *  answer is never cut off. */
  timeoutMs: number;
}

export interface OpenCodeRunOptions {
  prompt: string;
  /** Optional task-specific system prompt, appended to the chat-only identity. */
  instructions?: string;
  /** Per-call overrides sourced from OpenCodeConfig + CredentialsManager by the
   *  caller. The service is intentionally decoupled from settings/keytar so it
   *  can be unit-tested with plain values. */
  baseUrl: string;
  username?: string;
  password?: string;
  model?: string;
  timeoutMs: number;
  signal?: AbortSignal;
  /** Reuse an existing OpenCode session instead of creating (and disposing) a
   *  fresh one for this call. When omitted, a throwaway session is created and
   *  best-effort deleted afterwards so the server doesn't accumulate sessions. */
  sessionId?: string;
}

export const DEFAULT_OPENCODE_CONFIG: OpenCodeConfig = {
  enabled: false,
  baseUrl: 'http://127.0.0.1:4096',
  username: 'opencode',
  model: '',
  fastModel: '',
  // Local agents can pause on tool calls; 120s idle is generous without letting
  // a truly stuck connection hang forever.
  timeoutMs: 120_000,
};

// Retry policy for transient upstream failures (network blips / 5xx / 429).
// Mirrors CodexCliService's shape.
const TRANSIENT_RETRY_MAX = 3;
const TRANSIENT_RETRY_BASE_MS = 500;
const TRANSIENT_RETRY_CAP_MS = 8_000;

const OPENCODE_CHAT_SYSTEM_PROMPT = [
  'You are a general-purpose conversational AI assistant.',
  'Answer the user directly and naturally. Do not act as a coding agent, inspect or modify files, run commands, or attempt to use tools.',
].join(' ');

export class OpenCodeService {
  public static normalizeConfig(config: Partial<OpenCodeConfig> = {}): OpenCodeConfig {
    const timeoutMs = Number(config.timeoutMs);
    return {
      enabled: !!config.enabled,
      baseUrl: normalizeBaseUrl(config.baseUrl) || DEFAULT_OPENCODE_CONFIG.baseUrl,
      username: (config.username || DEFAULT_OPENCODE_CONFIG.username).trim() || DEFAULT_OPENCODE_CONFIG.username,
      // Models are free-form `providerID/modelID` strings validated inside
      // OpenCode; we only trim. Empty is valid (→ server default).
      model: (config.model || '').trim(),
      fastModel: (config.fastModel || '').trim(),
      timeoutMs: Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : DEFAULT_OPENCODE_CONFIG.timeoutMs,
    };
  }

  /**
   * Collect the full response into a single string via the synchronous message
   * endpoint (the most reliable transport). Prefer `stream()` for user-visible
   * surfaces so the UI shows progress.
   */
  public static async run(_unused: string, options: OpenCodeRunOptions): Promise<string> {
    if (options.signal?.aborted) throw new Error('OpenCode request aborted before start.');
    return this.runViaMessage(options);
  }

  /**
   * Stream the OpenCode response as text deltas.
   *
   * Tries the SSE event bus first. If that path fails BEFORE any text has been
   * emitted (server without the event bus, connection drop during setup, parse
   * failure), it falls back to the synchronous message endpoint and yields the
   * whole answer as one chunk — so the provider still works. Once any delta has
   * been yielded, a later failure is re-thrown (we can't safely restart a
   * half-shown answer without duplicating it).
   *
   * `_unused` mirrors CodexCliService.stream's leading positional arg so the two
   * services share a call shape at the LLMHelper dispatch sites.
   */
  public static async *stream(_unused: string, options: OpenCodeRunOptions): AsyncGenerator<string, void, unknown> {
    if (options.signal?.aborted) throw new Error('OpenCode request aborted before start.');
    if (!normalizeBaseUrl(options.baseUrl)) throw new Error(OPENCODE_NOT_CONFIGURED_MESSAGE);

    let emitted = 0;
    try {
      for await (const delta of this.streamViaEvents(options)) {
        emitted += delta.length;
        yield delta;
      }
      return;
    } catch (e: any) {
      if (isAbortError(e)) throw new Error('OpenCode request aborted.');
      if (emitted > 0) {
        // Partial answer already shown — restarting would duplicate it.
        throw e;
      }
      // Nothing shown yet: fall back to the reliable synchronous transport.
      console.warn('[OpenCodeService] Event-stream path failed before first token; falling back to synchronous message endpoint.', e?.message || e);
    }

    const full = await this.runViaMessage(options);
    if (full) yield full;
  }

  // ---------------------------------------------------------------------------
  // Transport 1: SSE event bus (progressive)
  // ---------------------------------------------------------------------------

  private static async *streamViaEvents(options: OpenCodeRunOptions): AsyncGenerator<string, void, unknown> {
    const base = normalizeBaseUrl(options.baseUrl);
    const headers = buildHeaders(options);

    // Idle-timeout guard: abort if no bytes arrive for timeoutMs. Resets on
    // every yielded delta (see OpenCodeConfig.timeoutMs).
    const deadlineController = new AbortController();
    let deadlineTimer: ReturnType<typeof setTimeout> = setTimeout(() => deadlineController.abort(), options.timeoutMs);
    const resetDeadline = () => {
      clearTimeout(deadlineTimer);
      deadlineTimer = setTimeout(() => deadlineController.abort(), options.timeoutMs);
    };
    const combined = combineSignals(options.signal, deadlineController.signal);
    const cleanup = () => { clearTimeout(deadlineTimer); combined.dispose(); };

    // Create (or reuse) the session used for this turn.
    const ownSession = !options.sessionId;
    let sessionId: string;
    try {
      sessionId = options.sessionId || await createSession(base, headers, combined.signal);
    } catch (e) {
      cleanup();
      throw e;
    }

    let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;
    try {
      // 1) Open the event bus BEFORE sending the prompt so no early deltas are
      //    missed. The first event is `server.connected`.
      const eventResp = await fetch(`${base}/event`, {
        method: 'GET',
        headers: { ...headers, Accept: 'text/event-stream' },
        signal: combined.signal,
      });
      if (!eventResp.ok || !eventResp.body) {
        throw new Error(`OpenCode event stream ${eventResp.status}`);
      }
      reader = eventResp.body.getReader();

      // 2) Fire the prompt without waiting for the full turn (204 No Content).
      //    prompt_async lets the reply arrive as bus events.
      const userMessageId = `msg_${randomUUID()}`;
      const body = buildPromptBody(options, userMessageId);
      const promptResp = await fetch(`${base}/session/${encodeURIComponent(sessionId)}/prompt_async`, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
        signal: combined.signal,
      });
      if (!promptResp.ok) {
        const text = await safeReadText(promptResp);
        throw new Error(extractErrorMessage(text) || `OpenCode prompt ${promptResp.status}`);
      }

      // 3) Read events, yield text-part deltas for OUR session, stop on idle.
      const decoder = new TextDecoder('utf-8');
      let buffer = '';
      const partState: PartDeltaState = { text: Object.create(null) };
      const messageEligibility = new Map<string, boolean>();
      const pendingPartEvents = new Map<string, Array<{ type: string; props: any }>>();

      while (true) {
        if (combined.signal.aborted) throw new Error('OpenCode request aborted.');
        const { value, done } = await reader.read();
        if (done) break; // bus closed — treat as end of turn
        buffer += decoder.decode(value, { stream: true });

        let idx: number;
        // eslint-disable-next-line no-cond-assign
        while ((idx = buffer.search(/\r?\n\r?\n/)) >= 0) {
          const raw = buffer.slice(0, idx);
          buffer = buffer.slice(idx + (buffer[idx] === '\r' ? 4 : 2));
          const parsed = parseSseEvent(raw);
          if (!parsed || !parsed.data || parsed.data === '[DONE]') continue;

          let json: any;
          try { json = JSON.parse(parsed.data); } catch { continue; }

          const type = json?.type;
          const props = json?.properties ?? {};

          if (type === 'message.updated') {
            const info = props?.info;
            if (!info || (info.sessionID ?? info.sessionId) !== sessionId || typeof info.id !== 'string') continue;
            const eligible = info.role === 'assistant' && info.parentID === userMessageId;
            messageEligibility.set(info.id, eligible);
            const pending = pendingPartEvents.get(info.id) || [];
            pendingPartEvents.delete(info.id);
            if (!eligible) continue;
            for (const event of pending) {
              const delta = extractStreamText(event.type, event.props, partState);
              if (delta) { resetDeadline(); yield delta; }
            }
            continue;
          }
          if (type === 'message.part.updated' || type === 'message.part.delta') {
            const messageId = getPartEventMessageId(type, props);
            if (!messageId || !partEventBelongsToSession(type, props, sessionId)) continue;
            if (!messageEligibility.has(messageId)) {
              const pending = pendingPartEvents.get(messageId) || [];
              pending.push({ type, props });
              pendingPartEvents.set(messageId, pending);
              continue;
            }
            if (!messageEligibility.get(messageId)) continue;
            const delta = extractStreamText(type, props, partState);
            if (delta) { resetDeadline(); yield delta; }
            continue;
          }
          if (type === 'session.error' && matchesSession(props, sessionId)) {
            throw new Error(extractSessionError(props) || 'OpenCode session error.');
          }
          if (type === 'session.idle' && matchesSession(props, sessionId)) {
            return; // assistant turn complete
          }
        }
      }
    } catch (e: any) {
      if (isAbortError(e)) throw new Error('OpenCode request aborted.');
      throw e;
    } finally {
      if (reader) reader.cancel().catch(() => { /* already torn down */ });
      cleanup();
      // Dispose the throwaway session so the server doesn't accumulate sessions.
      if (ownSession) deleteSession(base, headers, sessionId).catch(() => { /* best effort */ });
    }
  }

  // ---------------------------------------------------------------------------
  // Transport 2: synchronous message endpoint (reliable)
  // ---------------------------------------------------------------------------

  private static async runViaMessage(options: OpenCodeRunOptions): Promise<string> {
    const base = normalizeBaseUrl(options.baseUrl);
    if (!base) throw new Error(OPENCODE_NOT_CONFIGURED_MESSAGE);
    const headers = buildHeaders(options);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), options.timeoutMs);
    const combined = combineSignals(options.signal, controller.signal);

    const ownSession = !options.sessionId;
    let sessionId = '';
    try {
      sessionId = options.sessionId || await createSession(base, headers, combined.signal);
      const body = buildPromptBody(options);

      let attempt = 0;
      while (true) {
        if (combined.signal.aborted) throw new Error('OpenCode request aborted.');
        let resp: Response;
        try {
          resp = await fetch(`${base}/session/${encodeURIComponent(sessionId)}/message`, {
            method: 'POST',
            headers,
            body: JSON.stringify(body),
            signal: combined.signal,
          });
        } catch (e: any) {
          if (isAbortError(e)) throw new Error('OpenCode request aborted.');
          if (attempt >= TRANSIENT_RETRY_MAX) throw new Error(OPENCODE_CONNECTION_FAILED_MESSAGE);
          await sleepWithJitter(attempt++);
          continue;
        }

        if (resp.status === 429 || (resp.status >= 500 && resp.status < 600)) {
          if (attempt >= TRANSIENT_RETRY_MAX) {
            const text = await safeReadText(resp);
            throw new Error(`OpenCode upstream ${resp.status}: ${truncate(text, 300)}`);
          }
          await sleepWithJitter(attempt++, parseRetryAfter(resp.headers.get('retry-after')));
          continue;
        }
        if (!resp.ok) {
          const text = await safeReadText(resp);
          throw new Error(extractErrorMessage(text) || `OpenCode upstream ${resp.status}`);
        }

        const data = await resp.json().catch((): any => null);
        return extractTextFromParts(data?.parts);
      }
    } catch (e: any) {
      if (isAbortError(e)) throw new Error('OpenCode request aborted.');
      // A connection failure to a not-running server surfaces as a friendly,
      // actionable message rather than a raw fetch error.
      if (isLikelyConnectionError(e)) throw new Error(OPENCODE_CONNECTION_FAILED_MESSAGE);
      throw e;
    } finally {
      clearTimeout(timer);
      combined.dispose();
      if (ownSession && sessionId) deleteSession(base, headers, sessionId).catch(() => { /* best effort */ });
    }
  }
}

// =============================================================================
// Module-private helpers
// =============================================================================

/** Trim + strip a single trailing slash. Returns '' for empty/whitespace. */
function normalizeBaseUrl(url?: string): string {
  const trimmed = (url || '').trim();
  if (!trimmed) return '';
  return trimmed.replace(/\/+$/, '');
}

/** Split "providerID/modelID" on the FIRST slash. Returns undefined when there's
 *  no valid split (empty, no slash, or slash at an edge) → caller omits `model`
 *  so OpenCode uses its own default agent model. */
function splitModel(model?: string): { providerID: string; modelID: string } | undefined {
  const trimmed = (model || '').trim();
  if (!trimmed) return undefined;
  const slash = trimmed.indexOf('/');
  if (slash <= 0 || slash >= trimmed.length - 1) return undefined;
  return { providerID: trimmed.slice(0, slash), modelID: trimmed.slice(slash + 1) };
}

/** Build the prompt request body shared by /message and /prompt_async. */
function buildPromptBody(options: OpenCodeRunOptions, messageId?: string): Record<string, unknown> {
  const instructions = options.instructions?.trim();
  const body: Record<string, unknown> = {
    system: instructions
      ? `${OPENCODE_CHAT_SYSTEM_PROMPT}\n\n${instructions}`
      : OPENCODE_CHAT_SYSTEM_PROMPT,
    // OpenCode is used as a chat transport, not as its default coding agent.
    // The wildcard deny overrides agent permissions and strips every tool from
    // the model request, including tools added by future OpenCode versions.
    tools: { '*': false },
    parts: [{ type: 'text', text: options.prompt }],
  };
  if (messageId) body.messageID = messageId;
  const model = splitModel(options.model);
  if (model) body.model = model;
  return body;
}

/** Build request headers, adding HTTP Basic auth only when a password is set. */
function buildHeaders(options: OpenCodeRunOptions): Record<string, string> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    Accept: 'application/json',
  };
  if (options.password) {
    const user = (options.username || 'opencode').trim() || 'opencode';
    const token = Buffer.from(`${user}:${options.password}`).toString('base64');
    headers.Authorization = `Basic ${token}`;
  }
  return headers;
}

/** POST /session → returns the new session id. */
async function createSession(base: string, headers: Record<string, string>, signal: AbortSignal): Promise<string> {
  let resp: Response;
  try {
    resp = await fetch(`${base}/session`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ title: 'Natively' }),
      signal,
    });
  } catch (e: any) {
    if (isAbortError(e)) throw new Error('OpenCode request aborted.');
    throw new Error(OPENCODE_CONNECTION_FAILED_MESSAGE);
  }
  if (!resp.ok) {
    const text = await safeReadText(resp);
    throw new Error(extractErrorMessage(text) || `OpenCode could not create a session (${resp.status}).`);
  }
  const data = await resp.json().catch((): any => null);
  const id = data?.id ?? data?.sessionID ?? data?.session?.id;
  if (!id || typeof id !== 'string') {
    throw new Error('OpenCode returned an unexpected session response.');
  }
  return id;
}

/** DELETE /session/:id — best effort; failures are swallowed by the caller. */
async function deleteSession(base: string, headers: Record<string, string>, sessionId: string): Promise<void> {
  await fetch(`${base}/session/${encodeURIComponent(sessionId)}`, { method: 'DELETE', headers });
}

interface PartDeltaState { text: Record<string, string>; }

function getPartEventMessageId(type: string, props: any): string {
  const id = type === 'message.part.delta'
    ? props?.messageID ?? props?.messageId
    : props?.part?.messageID ?? props?.part?.messageId;
  return typeof id === 'string' ? id : '';
}

function partEventBelongsToSession(type: string, props: any, sessionId: string): boolean {
  const source = type === 'message.part.delta' ? props : props?.part;
  const sid = source?.sessionID ?? source?.sessionId;
  return sid === sessionId;
}

function matchesSession(props: any, sessionId: string): boolean {
  const sid = props?.sessionID ?? props?.sessionId ?? props?.session?.id;
  return !sid || sid === sessionId;
}

/** Extract text from both the generated SDK's legacy event and current event-v2. */
function extractStreamText(type: string, props: any, state: PartDeltaState): string {
  if (type === 'message.part.delta') {
    if (props?.field !== 'text' || typeof props?.delta !== 'string') return '';
    const key = `${getPartEventMessageId(type, props)}:${String(props?.partID ?? props?.partId ?? 'default')}`;
    state.text[key] = (state.text[key] || '') + props.delta;
    return props.delta;
  }

  const part = props?.part;
  if (!part || part.type !== 'text' || part.ignored === true) return '';
  const key = `${getPartEventMessageId(type, props)}:${String(part.id ?? 'default')}`;
  const full = typeof part.text === 'string' ? part.text : '';
  const previous = state.text[key] || '';
  if (typeof props.delta === 'string') {
    state.text[key] = full || previous + props.delta;
    return props.delta;
  }
  state.text[key] = full;
  if (full.startsWith(previous)) {
    return full.slice(previous.length);
  }
  return '';
}

/** Join the text of all text parts in a synchronous `{ info, parts }` reply. */
function extractTextFromParts(parts: any): string {
  if (!Array.isArray(parts)) return '';
  return parts
    .filter(p => p && p.type === 'text' && typeof p.text === 'string')
    .map(p => p.text)
    .join('')
    .trim();
}

function extractSessionError(props: any): string {
  const err = props?.error ?? props?.session?.error;
  if (!err) return '';
  if (typeof err === 'string') return err;
  if (typeof err.message === 'string') return err.message;
  if (typeof err.data?.message === 'string') return err.data.message;
  return '';
}

function extractErrorMessage(text: string): string {
  if (!text) return '';
  try {
    const json = JSON.parse(text);
    if (json?.error?.message) return String(json.error.message);
    if (typeof json?.message === 'string') return json.message;
    if (typeof json?.data?.message === 'string') return json.data.message;
  } catch { /* not JSON */ }
  return text;
}

function isAbortError(e: any): boolean {
  return e?.name === 'AbortError' || /aborted/i.test(String(e?.message || ''));
}

/** Node's fetch surfaces a not-running server as a TypeError ("fetch failed")
 *  with an ECONNREFUSED cause. Map those to the friendly message. */
function isLikelyConnectionError(e: any): boolean {
  const msg = String(e?.message || '');
  const code = String(e?.cause?.code || e?.code || '');
  return /fetch failed|ECONNREFUSED|ENOTFOUND|EAI_AGAIN|network|socket hang up/i.test(`${msg} ${code}`);
}

function sleepWithJitter(attempt: number, retryAfterMs?: number): Promise<void> {
  if (retryAfterMs && retryAfterMs > 0) {
    return new Promise(r => setTimeout(r, Math.min(retryAfterMs, TRANSIENT_RETRY_CAP_MS * 4)));
  }
  const base = Math.min(TRANSIENT_RETRY_CAP_MS, TRANSIENT_RETRY_BASE_MS * 2 ** attempt);
  const jitter = Math.random() * Math.min(TRANSIENT_RETRY_CAP_MS, base);
  return new Promise(r => setTimeout(r, jitter));
}

function parseRetryAfter(header: string | null | undefined): number | undefined {
  if (!header) return undefined;
  const seconds = Number(header);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.floor(seconds * 1000);
  return undefined;
}

function safeReadText(response: Response): Promise<string> {
  return response.text().catch(() => '');
}

function truncate(s: string, n: number): string {
  return s.length <= n ? s : s.slice(0, n) + '…';
}

interface ParsedSse { event?: string; data: string; }

function parseSseEvent(raw: string): ParsedSse | null {
  if (!raw.trim()) return null;
  let event: string | undefined;
  const dataLines: string[] = [];
  for (const line of raw.split(/\r?\n/)) {
    if (!line || line.startsWith(':')) continue;
    const colon = line.indexOf(':');
    if (colon < 0) continue;
    const field = line.slice(0, colon);
    let value = line.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    if (field === 'event') event = value;
    else if (field === 'data') dataLines.push(value);
  }
  if (!dataLines.length) return null;
  return { event, data: dataLines.join('\n') };
}

// AbortSignal.any() is Node 20+; keep our own combiner for Electron-version compat.
interface CombinedSignal { readonly signal: AbortSignal; dispose(): void; }

function combineSignals(...signals: (AbortSignal | undefined)[]): CombinedSignal {
  const filtered = signals.filter((s): s is AbortSignal => !!s);
  const ctrl = new AbortController();
  const onAbort = (e: Event) => ctrl.abort((e.target as AbortSignal)?.reason);
  for (const s of filtered) {
    if (s.aborted) { ctrl.abort(s.reason); break; }
    s.addEventListener('abort', onAbort, { once: true });
  }
  return {
    signal: ctrl.signal,
    dispose() {
      for (const s of filtered) {
        try { s.removeEventListener('abort', onAbort); } catch { /* swallow */ }
      }
    },
  };
}
