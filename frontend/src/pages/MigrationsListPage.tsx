// frontend/src/pages/MigrationsListPage.tsx
import { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router";
import { PageHeader } from "@/components/layout/PageHeader";
import { icons } from "@/config/iconRegistry";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Plus, Trash2, ExternalLink } from "lucide-react";
import type { Migration, MigrationStatus } from "@/types/migration";

const statusColors: Record<MigrationStatus, string> = {
  draft: "bg-gray-100 text-gray-700",
  testing: "bg-blue-100 text-blue-700",
  running: "bg-yellow-100 text-yellow-700",
  completed: "bg-green-100 text-green-700",
  failed: "bg-red-100 text-red-700",
  cancelled: "bg-orange-100 text-orange-700",
};

export function MigrationsListPage() {
  const [migrations, setMigrations] = useState<Migration[]>([]);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const load = useCallback(async () => {
    try {
      setMigrations(await api.migrations.list());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load migrations");
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleCreate = () => {
    navigate("/migrations/new");
  };

  const handleDelete = async (id: string) => {
    try {
      await api.migrations.delete(id);
      load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete migration");
    }
  };

  return (
    <div className="flex flex-1 flex-col">
      <PageHeader
        icon={icons.nav_migrations}
        title="Migrations"
        description="Build and run migration pipelines"
        actions={
          <Button onClick={handleCreate}>
            <Plus className="h-4 w-4 mr-2" /> New Migration
          </Button>
        }
      />
      {error && (
        <div className="mx-6 mt-4 rounded-lg border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive">
          {error}
        </div>
      )}
      <div className="p-6 max-w-4xl">
        <div className="grid gap-3">
          {migrations.map((mig) => (
            <Card key={mig.id} className="flex items-center justify-between p-4 cursor-pointer hover:bg-muted/50 transition-colors" onDoubleClick={() => navigate(`/migrations/${mig.id}`)}>
              <div>
                <div className="font-medium">{mig.name}</div>
                <div className="text-sm text-muted-foreground">
                  {mig.source_table || "—"} → {mig.target_table || "—"}
                </div>
              </div>
              <div className="flex items-center gap-3">
                <Badge className={statusColors[mig.status]}>{mig.status}</Badge>
                <Button variant="ghost" size="icon" onClick={() => navigate(`/migrations/${mig.id}`)}>
                  <ExternalLink className="h-4 w-4" />
                </Button>
                <Button variant="ghost" size="icon" onClick={() => handleDelete(mig.id)}>
                  <Trash2 className="h-4 w-4 text-destructive" />
                </Button>
              </div>
            </Card>
          ))}
          {migrations.length === 0 && (
            <p className="text-center text-muted-foreground py-8">No migrations yet.</p>
          )}
        </div>
      </div>
    </div>
  );
}
