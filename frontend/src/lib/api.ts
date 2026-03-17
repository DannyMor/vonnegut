import type { Connection, ConnectionCreate, ConnectionTestResult } from "@/types/connection";
import type { Migration, MigrationCreate, MigrationTestResult } from "@/types/migration";
import type {
  Transformation,
  TransformationCreate,
  AISuggestionRequest,
  AISuggestionResponse,
} from "@/types/transformation";

const BASE = "/api/v1";

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
    tables: (id: string) => request<string[]>(`/connections/${id}/tables`),
    schema: (id: string, table: string) =>
      request<{ column: string; type: string; nullable: boolean; is_primary_key: boolean }[]>(
        `/connections/${id}/tables/${table}/schema`
      ),
    sample: (id: string, table: string, rows = 10) =>
      request<Record<string, unknown>[]>(`/connections/${id}/tables/${table}/sample?rows=${rows}`),
  },

  migrations: {
    list: () => request<Migration[]>("/migrations"),
    get: (id: string) => request<Migration>(`/migrations/${id}`),
    create: (data: MigrationCreate) =>
      request<Migration>("/migrations", { method: "POST", body: JSON.stringify(data) }),
    update: (id: string, data: Partial<MigrationCreate>) =>
      request<Migration>(`/migrations/${id}`, { method: "PUT", body: JSON.stringify(data) }),
    delete: (id: string) =>
      request<void>(`/migrations/${id}`, { method: "DELETE" }),
    test: (id: string) =>
      request<MigrationTestResult>(`/migrations/${id}/test`, { method: "POST" }),
    run: (id: string) =>
      request<{ status: string; migration_id: string }>(`/migrations/${id}/run`, { method: "POST" }),
    cancel: (id: string) =>
      request<{ status: string }>(`/migrations/${id}/cancel`, { method: "POST" }),
    status: (id: string) =>
      request<{ status: string; rows_processed: number | null; total_rows: number | null; error_message: string | null }>(
        `/migrations/${id}/status`
      ),
  },

  transformations: {
    add: (migrationId: string, data: TransformationCreate) =>
      request<Transformation>(`/migrations/${migrationId}/transformations`, {
        method: "POST", body: JSON.stringify(data),
      }),
    update: (migrationId: string, id: string, data: { config: Record<string, unknown> }) =>
      request<Transformation>(`/migrations/${migrationId}/transformations/${id}`, {
        method: "PUT", body: JSON.stringify(data),
      }),
    delete: (migrationId: string, id: string) =>
      request<void>(`/migrations/${migrationId}/transformations/${id}`, { method: "DELETE" }),
    reorder: (migrationId: string, order: string[]) =>
      request<{ status: string }>(`/migrations/${migrationId}/transformations/reorder`, {
        method: "PUT", body: JSON.stringify({ order }),
      }),
  },

  ai: {
    suggest: (data: AISuggestionRequest) =>
      request<AISuggestionResponse>("/ai/suggest-transformation", {
        method: "POST", body: JSON.stringify(data),
      }),
  },
};
