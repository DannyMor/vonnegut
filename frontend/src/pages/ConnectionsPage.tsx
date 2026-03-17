// frontend/src/pages/ConnectionsPage.tsx
import { useEffect, useState, useCallback } from "react";
import { PageHeader } from "@/components/layout/PageHeader";
import { ConnectionList } from "@/components/connections/ConnectionList";
import { ConnectionForm } from "@/components/connections/ConnectionForm";
import { icons } from "@/config/iconRegistry";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Plus } from "lucide-react";
import type { Connection, ConnectionCreate } from "@/types/connection";

export function ConnectionsPage() {
  const [connections, setConnections] = useState<Connection[]>([]);
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<Connection | null>(null);
  const [testResults, setTestResults] = useState<Record<string, "ok" | "error" | "testing">>({});

  const load = useCallback(async () => {
    setConnections(await api.connections.list());
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleSave = async (data: ConnectionCreate) => {
    if (editing) {
      await api.connections.update(editing.id, data);
    } else {
      await api.connections.create(data);
    }
    setEditing(null);
    load();
  };

  const handleDelete = async (id: string) => {
    await api.connections.delete(id);
    load();
  };

  const handleTest = async (id: string) => {
    setTestResults((prev) => ({ ...prev, [id]: "testing" }));
    const result = await api.connections.test(id);
    setTestResults((prev) => ({ ...prev, [id]: result.status }));
  };

  return (
    <div className="flex flex-1 flex-col">
      <PageHeader
        icon={icons.nav_connections}
        title="Connections"
        description="Manage database connections"
        actions={
          <Button onClick={() => { setEditing(null); setFormOpen(true); }}>
            <Plus className="h-4 w-4 mr-2" /> New Connection
          </Button>
        }
      />
      <div className="p-6 max-w-3xl">
        <ConnectionList
          connections={connections}
          testResults={testResults}
          onEdit={(conn) => { setEditing(conn); setFormOpen(true); }}
          onDelete={handleDelete}
          onTest={handleTest}
        />
      </div>
      <ConnectionForm
        key={editing?.id ?? "new"}
        open={formOpen}
        onClose={() => { setFormOpen(false); setEditing(null); }}
        onSave={handleSave}
        initial={editing}
      />
    </div>
  );
}
