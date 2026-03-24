export interface PostgresDirectConfig {
  type: "postgres_direct";
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export interface PostgresPodConfig {
  type: "postgres_pod";
  namespace: string;
  pod_selector: string;
  pick_strategy: "first_ready" | "name_contains";
  pick_filter?: string;
  container?: string;
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export type ConnectionConfig = PostgresDirectConfig | PostgresPodConfig;

export interface Connection {
  id: string;
  name: string;
  config: ConnectionConfig;
  created_at: string;
  updated_at: string;
}

export interface ConnectionCreate {
  name: string;
  config: ConnectionConfig;
}

export interface ConnectionTestResult {
  status: "ok" | "error";
  message: string;
}

export interface ColumnSchema {
  name: string;
  type: string;
  category: string;
  nullable: boolean;
  default: string | null;
  is_primary_key: boolean;
  foreign_key: string | null;
  is_unique: boolean;
}
