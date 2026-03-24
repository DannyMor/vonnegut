import type { Transformation } from "./transformation";
import type { PipelineStep, ColumnDef } from "./pipeline";

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
  source_query: string;
  source_schema: ColumnDef[];
  status: MigrationStatus;
  truncate_target: boolean;
  rows_processed: number | null;
  total_rows: number | null;
  error_message: string | null;
  created_at: string;
  updated_at: string;
  transformations: Transformation[];
  pipeline_steps: PipelineStep[];
  // Frontend-only display fields (not persisted)
  source_label?: string;
  target_label?: string;
  source_description?: string;
  target_description?: string;
}

export interface MigrationCreate {
  name: string;
  source_connection_id: string;
  target_connection_id: string;
  source_table: string;
  target_table: string;
  source_query?: string;
  source_schema?: ColumnDef[];
  truncate_target?: boolean;
}

export interface MigrationTestResult {
  before: Record<string, unknown>[];
  after: Record<string, unknown>[];
}
