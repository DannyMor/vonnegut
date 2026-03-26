import { Handle, Position, type NodeProps, type Node } from "@xyflow/react";
import { icons } from "@/config/iconRegistry";
import { nodeTheme } from "@/config/nodeTheme";

export interface SourceNodeData {
  connectionName: string;
  table: string;
  schemaCount: number | null;
  label?: string;
  [key: string]: unknown;
}

type SourceNodeType = Node<SourceNodeData>;

export function SourceNode({ data, selected }: NodeProps<SourceNodeType>) {
  const d = data;
  const Icon = icons.source;
  const theme = nodeTheme.source;

  return (
    <div className={`rounded-lg border-2 p-3 w-[180px] ${theme.color} ${selected ? "ring-2 ring-primary" : ""}`}>
      <div className="flex items-center gap-2">
        <Icon className={`h-4 w-4 shrink-0 ${theme.accent}`} />
        <span className="font-medium text-sm truncate">{d.label || "Source"}</span>
      </div>
      {d.connectionName && <div className="text-xs text-muted-foreground mt-1 truncate">{d.connectionName}</div>}
      {d.table && <div className="text-xs font-mono mt-0.5 truncate" title={d.table}>{d.table}</div>}
      {d.schemaCount !== null && (
        <span className={`text-xs px-1.5 py-0.5 rounded mt-1 inline-block ${theme.badge}`}>{d.schemaCount} cols</span>
      )}
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
