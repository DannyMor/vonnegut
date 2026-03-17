export interface ConnectionConfig {
  // Direct connection
  host?: string;
  port?: number;
  // Pod connection
  namespace?: string;
  pod_selector?: string;
  pick_strategy?: "first_ready" | "name_contains";
  pick_filter?: string;
  container?: string;
  local_port?: number;
  // Shared
  database?: string;
  user?: string;
  password?: string;
}

export interface Connection {
  id: string;
  name: string;
  type: "postgres_direct" | "postgres_pod";
  config: ConnectionConfig;
  created_at: string;
  updated_at: string;
}

export interface ConnectionCreate {
  name: string;
  type: "postgres_direct" | "postgres_pod";
  config: ConnectionConfig;
}

export interface ConnectionTestResult {
  status: "ok" | "error";
  message: string;
}
