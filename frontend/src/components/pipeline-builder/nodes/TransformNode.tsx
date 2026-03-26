import { Handle, Position, type NodeProps } from "@xyflow/react";
import { icons } from "@/config/iconRegistry";
import { nodeTheme, type NodeType } from "@/config/nodeTheme";
import { HoverCard, HoverCardContent, HoverCardTrigger } from "@/components/ui/hover-card";
import type { TransformationType } from "@/types/transformation";

export interface TransformNodeData {
  label: string;
  transformType: TransformationType;
  config: Record<string, unknown>;
  previewRows?: Record<string, unknown>[];
}

export function TransformNode({ data }: NodeProps) {
  const d = data as unknown as TransformNodeData;
  const Icon = icons[d.transformType];
  const theme = nodeTheme[d.transformType as NodeType];

  return (
    <HoverCard>
      <HoverCardTrigger>
        <div className={`rounded-lg border-2 p-3 min-w-[180px] ${theme.color}`}>
          <Handle type="target" position={Position.Left} />
          <div className="flex items-center gap-2 mb-1">
            <Icon className={`h-4 w-4 ${theme.accent}`} />
            <span className="font-medium text-sm">{d.label}</span>
          </div>
          <div className="text-xs text-muted-foreground">{d.transformType.replace("_", " ")}</div>
          <Handle type="source" position={Position.Right} />
        </div>
      </HoverCardTrigger>
      <HoverCardContent side="top" className="w-72">
        <div className="text-sm">
          <div><span className="font-medium">Type:</span> {d.transformType.replace("_", " ")}</div>
          {typeof d.config.expression === "string" && d.config.expression && (
            <div className="mt-1 font-mono text-xs bg-muted p-2 rounded">{d.config.expression}</div>
          )}
          {Array.isArray(d.config.mappings) && (
            <div className="mt-1 text-xs">
              {(d.config.mappings as Array<{source_col: string; target_col: string; drop: boolean}>).slice(0, 3).map((m, i) => (
                <div key={i}>{m.drop ? `drop ${m.source_col}` : `${m.source_col} → ${m.target_col}`}</div>
              ))}
            </div>
          )}
          {d.previewRows && d.previewRows.length > 0 && (
            <div className="mt-2 text-xs font-mono">
              <div className="font-medium mb-1">Preview:</div>
              {d.previewRows.slice(0, 2).map((row, i) => (
                <div key={i}>{JSON.stringify(row).slice(0, 60)}</div>
              ))}
            </div>
          )}
        </div>
      </HoverCardContent>
    </HoverCard>
  );
}
