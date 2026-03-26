import { Handle, Position, type NodeProps, type Node } from "@xyflow/react";
import { X, CheckCircle2, XCircle } from "lucide-react";
import { icons } from "@/config/iconRegistry";
import { nodeTheme, type NodeType } from "@/config/nodeTheme";
import type { StepType } from "@/types/pipeline";

export interface PipelineNodeData {
  name: string;
  stepType: StepType;
  description: string | null;
  schemaCount: number | null;
  validationStatus: "ok" | "error" | null;
  onDelete: (nodeId: string) => void;
  [key: string]: unknown;
}

type PipelineNodeType = Node<PipelineNodeData>;

export function PipelineNode({ id, data, selected }: NodeProps<PipelineNodeType>) {
  const d = data;
  const Icon = icons[d.stepType] || icons.sql;
  const theme = nodeTheme[d.stepType as NodeType];

  return (
    <div
      className={`rounded-lg border-2 p-3 w-[180px] relative group ${theme.color} ${selected ? "ring-2 ring-primary" : ""}`}
      title={d.description || undefined}
    >
      <Handle type="target" position={Position.Left} />
      <div className="flex items-center gap-2">
        <Icon className={`h-4 w-4 shrink-0 ${theme.accent}`} />
        <span className="font-medium text-sm truncate">{d.name}</span>
        {d.validationStatus === "ok" && <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-green-600" />}
        {d.validationStatus === "error" && <XCircle className="h-3.5 w-3.5 shrink-0 text-red-600" />}
        <button
          onClick={(e) => { e.stopPropagation(); d.onDelete(id); }}
          className="absolute top-1 right-1 opacity-0 group-hover:opacity-100 h-4 w-4 rounded hover:bg-destructive/20 flex items-center justify-center"
        >
          <X className="h-3 w-3" />
        </button>
      </div>
      <div className="flex items-center gap-2 mt-1">
        <span className="text-xs text-muted-foreground">{d.stepType}</span>
        {d.schemaCount !== null && (
          <span className={`text-xs px-1.5 py-0.5 rounded ${theme.badge}`}>{d.schemaCount} cols</span>
        )}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
