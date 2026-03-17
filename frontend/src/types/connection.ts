export interface ConnectionConfig {
  host?: string;
  port?: number;
  database?: string;
  user?: string;
  password?: string;
  namespace?: string;
  pod_name?: string;
  container?: string;
  local_port?: number;
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
