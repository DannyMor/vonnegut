import type { Transformation } from "./transformation";

export type MigrationStatus =
  | "draft"
  | "testing"
  | "running"
  | "completed"
  | "failed"
  | "cancelled";

export interface Migration {
  id: string;
  name: string;
  source_connection_id: string;
  target_connection_id: string;
  source_table: string;
  target_table: string;
  status: MigrationStatus;
  truncate_target: boolean;
  rows_processed: number | null;
  total_rows: number | null;
  error_message: string | null;
  created_at: string;
  updated_at: string;
  transformations: Transformation[];
}

export interface MigrationCreate {
  name: string;
  source_connection_id: string;
  target_connection_id: string;
  source_table: string;
  target_table: string;
  truncate_target?: boolean;
}

export interface MigrationTestResult {
  before: Record<string, unknown>[];
  after: Record<string, unknown>[];
}
