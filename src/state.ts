/**
 * In-memory per-session runtime state.
 *
 * This data is intentionally ephemeral. Persisted remote compaction artifacts
 * live in Pi session entries; this module only caches the currently active
 * continuation and reconstructed replay state for the running process.
 */
import type {
  RemoteCompactionSessionState,
  ResponsesReasoningConfig,
  ResponsesTextConfig,
} from "./remote-compaction.ts";

export type ContinuationState = {
  responseId: string;
  modelKey: string;
  updatedAt: number;
  contextLength?: number;
};

export type ResponsesRequestShapeState = {
  updatedAt: number;
  reasoning?: ResponsesReasoningConfig;
  text?: ResponsesTextConfig;
};

/**
 * A remote-compaction failure recorded during `session_before_compact` so it can
 * be surfaced after the compaction is committed. Notifications emitted from
 * inside `session_before_compact` are discarded by the TUI re-render that
 * follows the commit, so the warning has to survive the hook boundary.
 */
export type PendingCompactionWarning = {
  message: string;
  /** True when the extension saved its own text-only summary in place of opaque continuity. */
  textOnlyFallback: boolean;
};

const continuationBySessionId = new Map<string, ContinuationState>();
const remoteCompactionBySessionId = new Map<string, RemoteCompactionSessionState>();
const requestShapeBySessionId = new Map<string, ResponsesRequestShapeState>();
const pendingCompactionWarningBySessionId = new Map<string, PendingCompactionWarning>();

export function getContinuationState(sessionId: string): ContinuationState | undefined {
  return continuationBySessionId.get(sessionId);
}

export function setContinuationState(sessionId: string, state: ContinuationState): void {
  continuationBySessionId.set(sessionId, state);
}

export function clearContinuationState(sessionId: string | undefined): void {
  if (!sessionId) return;
  continuationBySessionId.delete(sessionId);
}

export function getRemoteCompactionState(
  sessionId: string,
): RemoteCompactionSessionState | undefined {
  return remoteCompactionBySessionId.get(sessionId);
}

export function setRemoteCompactionState(
  sessionId: string,
  state: RemoteCompactionSessionState,
): void {
  remoteCompactionBySessionId.set(sessionId, state);
}

export function clearRemoteCompactionState(sessionId: string | undefined): void {
  if (!sessionId) return;
  remoteCompactionBySessionId.delete(sessionId);
}

export function getResponsesRequestShapeState(
  sessionId: string,
): ResponsesRequestShapeState | undefined {
  return requestShapeBySessionId.get(sessionId);
}

export function setResponsesRequestShapeState(
  sessionId: string,
  state: ResponsesRequestShapeState,
): void {
  requestShapeBySessionId.set(sessionId, state);
}

export function clearResponsesRequestShapeState(sessionId: string | undefined): void {
  if (!sessionId) return;
  requestShapeBySessionId.delete(sessionId);
}

export function setPendingCompactionWarning(
  sessionId: string,
  warning: PendingCompactionWarning,
): void {
  pendingCompactionWarningBySessionId.set(sessionId, warning);
}

/** Reads and removes the pending warning so it can never be emitted twice. */
export function takePendingCompactionWarning(
  sessionId: string,
): PendingCompactionWarning | undefined {
  const warning = pendingCompactionWarningBySessionId.get(sessionId);
  pendingCompactionWarningBySessionId.delete(sessionId);
  return warning;
}

export function clearPendingCompactionWarning(sessionId: string | undefined): void {
  if (!sessionId) return;
  pendingCompactionWarningBySessionId.delete(sessionId);
}

export function clearAllContinuationState(): void {
  continuationBySessionId.clear();
  remoteCompactionBySessionId.clear();
  requestShapeBySessionId.clear();
  pendingCompactionWarningBySessionId.clear();
}
