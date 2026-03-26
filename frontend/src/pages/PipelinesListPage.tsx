// frontend/src/pages/PipelinesListPage.tsx
import { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router";
import { PageHeader } from "@/components/layout/PageHeader";
import { icons } from "@/config/iconRegistry";
import { api } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Plus, Trash2, ExternalLink } from "lucide-react";
import type { Pipeline, PipelineStatus } from "@/types/pipeline-definition";

const statusColors: Record<PipelineStatus, string> = {
  draft: "bg-gray-100 text-gray-700",
  testing: "bg-blue-100 text-blue-700",
  running: "bg-yellow-100 text-yellow-700",
  completed: "bg-green-100 text-green-700",
  failed: "bg-red-100 text-red-700",
  cancelled: "bg-orange-100 text-orange-700",
};

export function PipelinesListPage() {
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const load = useCallback(async () => {
    try {
      setPipelines(await api.pipelines.list());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load pipelines");
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleCreate = () => {
    navigate("/pipelines/new");
  };

  const handleDelete = async (id: string) => {
    try {
      await api.pipelines.delete(id);
      load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete pipeline");
    }
  };

  return (
    <div className="flex flex-1 flex-col">
      <PageHeader
        icon={icons.nav_pipelines}
        title="Pipelines"
        description="Build and run data pipelines"
        actions={
          <Button onClick={handleCreate}>
            <Plus className="h-4 w-4 mr-2" /> New Pipeline
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
          {pipelines.map((pipeline) => (
            <Card key={pipeline.id} className="flex items-center justify-between p-4 cursor-pointer hover:bg-muted/50 transition-colors" onDoubleClick={() => navigate(`/pipelines/${pipeline.id}`)}>
              <div>
                <div className="font-medium">{pipeline.name}</div>
                <div className="text-sm text-muted-foreground">
                  {pipeline.source_table || "—"} → {pipeline.target_table || "—"}
                </div>
              </div>
              <div className="flex items-center gap-3">
                <Badge className={statusColors[pipeline.status]}>{pipeline.status}</Badge>
                <Button variant="ghost" size="icon" onClick={() => navigate(`/pipelines/${pipeline.id}`)}>
                  <ExternalLink className="h-4 w-4" />
                </Button>
                <Button variant="ghost" size="icon" onClick={() => handleDelete(pipeline.id)}>
                  <Trash2 className="h-4 w-4 text-destructive" />
                </Button>
              </div>
            </Card>
          ))}
          {pipelines.length === 0 && (
            <p className="text-center text-muted-foreground py-8">No pipelines yet.</p>
          )}
        </div>
      </div>
    </div>
  );
}
