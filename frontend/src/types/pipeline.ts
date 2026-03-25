export type StepType = "sql" | "code" | "ai";

export interface ColumnDef {
  name: string;
  type: string;
}

export interface PipelineStep {
  id: string;
  migration_id: string;
  name: string;
  description: string | null;
  position: number;
  step_type: StepType;
  config: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface PipelineStepCreate {
  step_type: StepType;
  name: string;
  description?: string;
  config: Record<string, unknown>;
  insert_after?: string;
}

export interface PipelineStepUpdate {
  name?: string;
  description?: string;
  step_type?: StepType;
  config?: Record<string, unknown>;
}

export interface ValidationError {
  type: "missing_column" | "type_mismatch" | "execution_error";
  column?: string;
  expected?: string;
  actual?: string | null;
  message: string;
}

export interface StepResult {
  node_id: string;
  status: "ok" | "error";
  schema: ColumnDef[];
  sample_data: Record<string, unknown>[];
  validation: {
    valid: boolean;
    errors?: ValidationError[];
  };
}

export interface PipelineTestResult {
  steps: StepResult[];
}

export type ValidationStatus = "DRAFT" | "VALIDATING" | "VALID" | "INVALID";
