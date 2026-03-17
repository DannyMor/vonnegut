export type TransformationType =
  | "column_mapping"
  | "sql_expression"
  | "ai_generated";

export interface Transformation {
  id: string;
  migration_id: string;
  order: number;
  type: TransformationType;
  config: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface TransformationCreate {
  type: TransformationType;
  config: Record<string, unknown>;
}

export interface AISuggestionRequest {
  prompt: string;
  source_schema: { column: string; type: string }[];
  sample_data: Record<string, unknown>[];
  target_schema: { column: string; type: string }[] | null;
}

export interface AISuggestionResponse {
  expression: string;
  output_column: string;
  explanation: string;
}
