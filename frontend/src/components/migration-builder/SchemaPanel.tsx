import { useState } from "react";
import { ChevronLeft, ChevronRight, TableProperties } from "lucide-react";
import type { ColumnDef } from "@/types/pipeline";

interface Props {
  title: "Input Schema" | "Output Schema";
  schema: ColumnDef[];
  side: "left" | "right";
}

export function SchemaPanel({ title, schema, side }: Props) {
  const [collapsed, setCollapsed] = useState(false);
  const ExpandIcon = side === "left" ? ChevronRight : ChevronLeft;
  const CollapseIcon = side === "left" ? ChevronLeft : ChevronRight;

  if (collapsed) {
    return (
      <button
        onClick={() => setCollapsed(false)}
        className="w-8 flex flex-col items-center justify-center border-x hover:bg-muted/50"
        title={`Expand ${title}`}
      >
        <ExpandIcon className="h-3 w-3 mb-1" />
        <span className="text-xs [writing-mode:vertical-lr] text-muted-foreground">{title}</span>
      </button>
    );
  }

  return (
    <div className="w-56 flex flex-col border-x">
      <div className="flex items-center justify-between px-3 py-2 border-b">
        <div className="flex items-center gap-1.5">
          <TableProperties className="h-3.5 w-3.5 text-muted-foreground" />
          <span className="text-xs font-medium">{title}</span>
        </div>
        <button onClick={() => setCollapsed(true)} className="hover:bg-muted rounded p-0.5">
          <CollapseIcon className="h-3 w-3" />
        </button>
      </div>
      <div className="flex-1 overflow-y-auto p-2">
        {schema.length > 0 ? (
          <div className="flex flex-col gap-0.5">
            {schema.map((col) => (
              <div key={col.name} className="flex items-center justify-between px-2 py-1 text-xs rounded hover:bg-muted/50">
                <span className="font-mono truncate">{col.name}</span>
                <span className="text-muted-foreground ml-2 shrink-0">{col.type}</span>
              </div>
            ))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-8 text-muted-foreground">
            <TableProperties className="h-6 w-6 mb-2 opacity-30" />
            <span className="text-xs">Run test to infer schema</span>
          </div>
        )}
      </div>
    </div>
  );
}
