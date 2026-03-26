import type { Connection, ConnectionCreate, ConnectionTestResult, ColumnSchema } from "@/types/connection";
import type { Pipeline, PipelineCreate } from "@/types/pipeline-definition";
import type { PipelineStep, PipelineStepCreate, PipelineStepUpdate, PipelineTestResult } from "@/types/pipeline";
import type {
  Transformation,
  TransformationCreate,
  AISuggestionRequest,
  AISuggestionResponse,
} from "@/types/transformation";

const BASE = "/api/v1";

// Simple in-memory cache for tables per connection
const _tablesCache = new Map<string, string[]>();

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(body.detail || `HTTP ${res.status}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json();
}

// Connections
export const api = {
  connections: {
    list: () => request<Connection[]>("/connections"),
    get: (id: string) => request<Connection>(`/connections/${id}`),
    create: (data: ConnectionCreate) =>
      request<Connection>("/connections", { method: "POST", body: JSON.stringify(data) }),
    update: (id: string, data: Partial<ConnectionCreate>) =>
      request<Connection>(`/connections/${id}`, { method: "PUT", body: JSON.stringify(data) }),
    delete: (id: string) =>
      request<void>(`/connections/${id}`, { method: "DELETE" }),
    test: (id: string) =>
      request<ConnectionTestResult>(`/connections/${id}/test`, { method: "POST" }),
    tables: async (id: string, refresh = false) => {
      if (!refresh && _tablesCache.has(id)) return _tablesCache.get(id)!;
      const tables = await request<string[]>(`/connections/${id}/tables`);
      _tablesCache.set(id, tables);
      return tables;
    },
    schema: (id: string, table: string) =>
      request<ColumnSchema[]>(`/connections/${id}/tables/${table}/schema`),
    sample: (id: string, table: string, rows = 10) =>
      request<Record<string, unknown>[]>(`/connections/${id}/tables/${table}/sample?rows=${rows}`),
    databases: (id: string) => request<string[]>(`/connections/${id}/databases`),
    testConfig: (data: ConnectionCreate) =>
      request<ConnectionTestResult>("/connections/test-config", { method: "POST", body: JSON.stringify(data) }),
    discoverDatabases: (data: ConnectionCreate) =>
      request<string[]>("/connections/discover-databases", { method: "POST", body: JSON.stringify(data) }),
  },

  pipelines: {
    list: () => request<Pipeline[]>("/pipelines"),
    get: (id: string) => request<Pipeline>(`/pipelines/${id}`),
    create: (data: PipelineCreate) =>
      request<Pipeline>("/pipelines", { method: "POST", body: JSON.stringify(data) }),
    update: (id: string, data: Partial<PipelineCreate>) =>
      request<Pipeline>(`/pipelines/${id}`, { method: "PUT", body: JSON.stringify(data) }),
    delete: (id: string) =>
      request<void>(`/pipelines/${id}`, { method: "DELETE" }),
    test: (id: string) =>
      request<PipelineTestResult>(`/pipelines/${id}/test`, { method: "POST" }),
    testStream: (id: string, onEvent: (event: Record<string, unknown>) => void): { abort: () => void } => {
      const controller = new AbortController();
      fetch(`${BASE}/pipelines/${id}/test-stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        signal: controller.signal,
      }).then(async (res) => {
        if (!res.ok || !res.body) {
          const body = await res.json().catch(() => ({ detail: res.statusText }));
          onEvent({ type: "error", error: body.detail || `HTTP ${res.status}` });
          onEvent({ type: "done" });
          return;
        }
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";
          for (const line of lines) {
            if (line.startsWith("data: ")) {
              try { onEvent(JSON.parse(line.slice(6))); } catch { /* ignore parse errors */ }
            }
          }
        }
      }).catch((err) => {
        if (err.name !== "AbortError") {
          onEvent({ type: "error", error: String(err) });
          onEvent({ type: "done" });
        }
      });
      return { abort: () => controller.abort() };
    },
    runStream: (id: string, onEvent: (event: Record<string, unknown>) => void): { abort: () => void } => {
      const controller = new AbortController();
      fetch(`${BASE}/pipelines/${id}/run-stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        signal: controller.signal,
      }).then(async (res) => {
        if (!res.ok || !res.body) {
          const body = await res.json().catch(() => ({ detail: res.statusText }));
          onEvent({ type: "error", error: body.detail || `HTTP ${res.status}` });
          onEvent({ type: "done" });
          return;
        }
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";
          for (const line of lines) {
            if (line.startsWith("data: ")) {
              try { onEvent(JSON.parse(line.slice(6))); } catch { /* ignore parse errors */ }
            }
          }
        }
      }).catch((err: Error) => {
        if (err.name !== "AbortError") {
          onEvent({ type: "error", error: String(err) });
          onEvent({ type: "done" });
        }
      });
      return { abort: () => controller.abort() };
    },
    validation: (id: string) =>
      request<{ pipeline_id: string; validation_status: string; validated_hash: string | null; last_validated_at: string | null }>(
        `/pipelines/${id}/validation`
      ),
  },

  transformations: {
    add: (pipelineId: string, data: TransformationCreate) =>
      request<Transformation>(`/pipelines/${pipelineId}/transformations`, {
        method: "POST", body: JSON.stringify(data),
      }),
    update: (pipelineId: string, id: string, data: { config: Record<string, unknown> }) =>
      request<Transformation>(`/pipelines/${pipelineId}/transformations/${id}`, {
        method: "PUT", body: JSON.stringify(data),
      }),
    delete: (pipelineId: string, id: string) =>
      request<void>(`/pipelines/${pipelineId}/transformations/${id}`, { method: "DELETE" }),
    reorder: (pipelineId: string, order: string[]) =>
      request<{ status: string }>(`/pipelines/${pipelineId}/transformations/reorder`, {
        method: "PUT", body: JSON.stringify({ order }),
      }),
  },

  pipelineSteps: {
    add: (pipelineId: string, data: PipelineStepCreate) =>
      request<PipelineStep>(`/pipelines/${pipelineId}/steps`, {
        method: "POST", body: JSON.stringify(data),
      }),
    update: (pipelineId: string, stepId: string, data: PipelineStepUpdate) =>
      request<PipelineStep>(`/pipelines/${pipelineId}/steps/${stepId}`, {
        method: "PUT", body: JSON.stringify(data),
      }),
    delete: (pipelineId: string, stepId: string) =>
      request<void>(`/pipelines/${pipelineId}/steps/${stepId}`, { method: "DELETE" }),
  },

  ai: {
    suggest: (data: AISuggestionRequest) =>
      request<AISuggestionResponse>("/ai/suggest-transformation", {
        method: "POST", body: JSON.stringify(data),
      }),
  },
};
