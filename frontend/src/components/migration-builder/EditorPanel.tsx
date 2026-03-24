import { useState, useEffect } from "react";
import { X } from "lucide-react";
import { SchemaPanel } from "./SchemaPanel";
import { SourceEditor } from "./editors/SourceEditor";
import { TargetEditor } from "./editors/TargetEditor";
import { SqlEditor } from "./editors/SqlEditor";
import { CodeEditor } from "./editors/CodeEditor";
import { AiEditor } from "./editors/AiEditor";
import type { Migration } from "@/types/migration";
import type { PipelineStep, ColumnDef } from "@/types/pipeline";
import type { Connection } from "@/types/connection";

/** Input that uses local state while editing, commits on blur. Restores fallback if left empty. */
function EditableLabel({ value, fallback, onChange, className }: {
  value: string;
  fallback: string;
  onChange: (v: string) => void;
  className?: string;
}) {
  const [local, setLocal] = useState(value || fallback);
  const [editing, setEditing] = useState(false);

  // Sync from external when not editing
  useEffect(() => {
    if (!editing) setLocal(value || fallback);
  }, [value, fallback, editing]);

  return (
    <input
      className={className}
      value={local}
      onFocus={() => setEditing(true)}
      onChange={(e) => setLocal(e.target.value)}
      onBlur={() => {
        setEditing(false);
        const trimmed = local.trim();
        if (trimmed) {
          onChange(trimmed === fallback ? "" : trimmed);
        } else {
          onChange("");
          setLocal(fallback);
        }
      }}
    />
  );
}

interface Props {
  nodeId: string;
  migration: Migration;
  connections: Connection[];
  step: PipelineStep | null;
  inputSchema: ColumnDef[];
  outputSchema: ColumnDef[];
  onClose: () => void;
  onUpdateMigration: (updates: Partial<Migration>) => void;
  onUpdateStep: (stepId: string, updates: Record<string, unknown>) => void;
}

export function EditorPanel({
  nodeId, migration, connections, step,
  inputSchema, outputSchema,
  onClose, onUpdateMigration, onUpdateStep,
}: Props) {
  const isSource = nodeId === "source";
  const isTarget = nodeId === "target";
  const nodeType = isSource ? "source" : isTarget ? "target" : step?.step_type || "sql";

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-2 border-b shrink-0">
        <span className="text-xs font-medium text-muted-foreground uppercase">{nodeType}</span>
        {step ? (
          <>
            <EditableLabel
              className="font-medium text-sm bg-transparent border-none outline-none"
              value={step.name}
              fallback={`${step.step_type.toUpperCase()} Transform`}
              onChange={(v) => onUpdateStep(step.id, { name: v || `${step.step_type.toUpperCase()} Transform` })}
            />
            <input
              className="text-xs text-muted-foreground bg-transparent border-none outline-none flex-1"
              value={step.description || ""}
              placeholder="Add description..."
              onChange={(e) => onUpdateStep(step.id, { description: e.target.value || null })}
              onBlur={(e) => onUpdateStep(step.id, { description: e.target.value.trim() || null })}
            />
          </>
        ) : (
          <>
            <EditableLabel
              className="font-medium text-sm bg-transparent border-none outline-none"
              value={isSource ? (migration.source_label || "") : (migration.target_label || "")}
              fallback={isSource ? "Source" : "Target"}
              onChange={(v) => onUpdateMigration(isSource ? { source_label: v || undefined } : { target_label: v || undefined })}
            />
            <input
              className="text-xs text-muted-foreground bg-transparent border-none outline-none flex-1"
              value={(isSource ? migration.source_description : migration.target_description) || ""}
              placeholder="Add description..."
              onChange={(e) => onUpdateMigration(
                isSource ? { source_description: e.target.value || undefined } : { target_description: e.target.value || undefined }
              )}
              onBlur={(e) => { const v = e.target.value.trim() || undefined; onUpdateMigration(isSource ? { source_description: v } : { target_description: v }); }}
            />
          </>
        )}
        <button onClick={onClose} className="hover:bg-muted rounded p-1">
          <X className="h-4 w-4" />
        </button>
      </div>

      {/* Three-column layout */}
      <div className="flex flex-1 min-h-0">
        {!isSource && (
          <SchemaPanel title="Input Schema" schema={inputSchema} side="left" />
        )}

        <div className="flex-1 overflow-auto p-4">
          {isSource && (
            <SourceEditor
              migration={migration}
              connections={connections}
              onUpdate={onUpdateMigration}
            />
          )}
          {isTarget && (
            <TargetEditor
              migration={migration}
              connections={connections}
              onUpdate={onUpdateMigration}
            />
          )}
          {step?.step_type === "sql" && (
            <SqlEditor step={step} onUpdate={(config) => onUpdateStep(step.id, { config })} />
          )}
          {step?.step_type === "code" && (
            <CodeEditor step={step} onUpdate={(config) => onUpdateStep(step.id, { config })} />
          )}
          {step?.step_type === "ai" && (
            <AiEditor
              step={step}
              inputSchema={inputSchema}
              onUpdate={(updates) => onUpdateStep(step.id, updates)}
            />
          )}
        </div>

        {!isTarget && (
          <SchemaPanel title="Output Schema" schema={outputSchema} side="right" />
        )}
      </div>
    </div>
  );
}
