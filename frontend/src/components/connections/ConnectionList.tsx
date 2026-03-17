// frontend/src/components/connections/ConnectionList.tsx
import type { Connection } from "@/types/connection";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { icons } from "@/config/iconRegistry";
import { Trash2, Pencil, Play } from "lucide-react";

interface Props {
  connections: Connection[];
  testResults: Record<string, "ok" | "error" | "testing">;
  onEdit: (conn: Connection) => void;
  onDelete: (id: string) => void;
  onTest: (id: string) => void;
}

export function ConnectionList({ connections, testResults, onEdit, onDelete, onTest }: Props) {
  const StatusIcon = (id: string) => {
    const status = testResults[id];
    if (status === "ok") return <icons.connection_ok className="h-4 w-4 text-green-500" />;
    if (status === "error") return <icons.connection_error className="h-4 w-4 text-red-500" />;
    if (status === "testing") return <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />;
    return null;
  };

  return (
    <div className="grid gap-3">
      {connections.map((conn) => (
        <Card key={conn.id} className="flex items-center justify-between p-4">
          <div className="flex items-center gap-3">
            {StatusIcon(conn.id)}
            <div>
              <div className="font-medium">{conn.name}</div>
              <div className="text-sm text-muted-foreground">
                {conn.type === "postgres_direct"
                  ? `${conn.config.host}:${conn.config.port}/${conn.config.database}`
                  : `${conn.config.namespace} | ${conn.config.pod_selector}`}
              </div>
            </div>
            <Badge variant="outline">{conn.type.replace("postgres_", "")}</Badge>
          </div>
          <div className="flex items-center gap-1">
            <Button variant="ghost" size="icon" onClick={() => onTest(conn.id)}>
              <Play className="h-4 w-4" />
            </Button>
            <Button variant="ghost" size="icon" onClick={() => onEdit(conn)}>
              <Pencil className="h-4 w-4" />
            </Button>
            <Button variant="ghost" size="icon" onClick={() => onDelete(conn.id)}>
              <Trash2 className="h-4 w-4 text-destructive" />
            </Button>
          </div>
        </Card>
      ))}
      {connections.length === 0 && (
        <p className="text-center text-muted-foreground py-8">No connections yet. Create one to get started.</p>
      )}
    </div>
  );
}
