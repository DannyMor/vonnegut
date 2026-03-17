import { Handle, Position, type NodeProps } from "@xyflow/react";
import { icons } from "@/config/iconRegistry";
import { nodeTheme } from "@/config/nodeTheme";
import { HoverCard, HoverCardContent, HoverCardTrigger } from "@/components/ui/hover-card";

export interface SourceNodeData {
  label: string;
  connectionName?: string;
  database?: string;
  table?: string;
  columns?: { column: string; type: string }[];
  rowCount?: number;
}

export function SourceNode({ data }: NodeProps) {
  const d = data as unknown as SourceNodeData;
  const Icon = icons.source;
  const theme = nodeTheme.source;

  return (
    <HoverCard>
      <HoverCardTrigger>
        <div className={`rounded-lg border-2 p-3 min-w-[180px] ${theme.color}`}>
          <div className="flex items-center gap-2 mb-1">
            <Icon className={`h-4 w-4 ${theme.accent}`} />
            <span className="font-medium text-sm">{d.label || "Source"}</span>
          </div>
          {d.table && <div className="text-xs text-muted-foreground">{d.table}</div>}
          <Handle type="source" position={Position.Right} />
        </div>
      </HoverCardTrigger>
      <HoverCardContent side="top" className="w-64">
        <div className="text-sm">
          {d.connectionName && <div><span className="font-medium">Connection:</span> {d.connectionName}</div>}
          {d.database && <div><span className="font-medium">Database:</span> {d.database}</div>}
          {d.table && <div><span className="font-medium">Table:</span> {d.table}</div>}
          {d.rowCount != null && <div><span className="font-medium">Rows:</span> {d.rowCount.toLocaleString()}</div>}
          {d.columns && d.columns.length > 0 && (
            <div className="mt-2">
              <span className="font-medium">Columns:</span>
              <div className="mt-1 font-mono text-xs">
                {d.columns.slice(0, 8).map((c) => (
                  <div key={c.column}>{c.column}: {c.type}</div>
                ))}
                {d.columns.length > 8 && <div className="text-muted-foreground">... +{d.columns.length - 8} more</div>}
              </div>
            </div>
          )}
        </div>
      </HoverCardContent>
    </HoverCard>
  );
}
