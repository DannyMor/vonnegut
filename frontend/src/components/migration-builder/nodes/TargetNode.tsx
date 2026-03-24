import { Handle, Position, type NodeProps, type Node } from "@xyflow/react";
import { icons } from "@/config/iconRegistry";
import { nodeTheme } from "@/config/nodeTheme";
import { CircleCheck, CircleX } from "lucide-react";

export interface TargetNodeData {
  connectionName: string;
  table: string;
  schemaCount: number | null;
  validationStatus: "valid" | "invalid" | "unknown";
  label?: string;
  [key: string]: unknown;
}

type TargetNodeType = Node<TargetNodeData>;

export function TargetNode({ data, selected }: NodeProps<TargetNodeType>) {
  const d = data;
  const Icon = icons.target;
  const theme = nodeTheme.target;

  return (
    <div className={`rounded-lg border-2 p-3 w-[180px] ${theme.color} ${selected ? "ring-2 ring-primary" : ""}`}>
      <Handle type="target" position={Position.Left} />
      <div className="flex items-center gap-2">
        <Icon className={`h-4 w-4 shrink-0 ${theme.accent}`} />
        <span className="font-medium text-sm truncate">{d.label || "Target"}</span>
        {d.validationStatus === "valid" && <CircleCheck className="h-3.5 w-3.5 shrink-0 text-green-600" />}
        {d.validationStatus === "invalid" && <CircleX className="h-3.5 w-3.5 shrink-0 text-red-600" />}
      </div>
      {d.connectionName && <div className="text-xs text-muted-foreground mt-1 truncate">{d.connectionName}</div>}
      {d.table && <div className="text-xs font-mono mt-0.5 truncate" title={d.table}>{d.table}</div>}
      {d.schemaCount !== null && (
        <span className={`text-xs px-1.5 py-0.5 rounded mt-1 inline-block ${theme.badge}`}>{d.schemaCount} cols</span>
      )}
    </div>
  );
}
