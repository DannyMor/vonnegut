import { useEffect, useState, useCallback } from "react";
import { useParams } from "react-router";
import { api } from "@/lib/api";
import { Canvas } from "@/components/migration-builder/Canvas";
import { Button } from "@/components/ui/button";
import { Plus, Play, FlaskConical } from "lucide-react";
import type { Migration } from "@/types/migration";

export function MigrationBuilderPage() {
  const { id } = useParams<{ id: string }>();
  const [migration, setMigration] = useState<Migration | null>(null);

  const load = useCallback(async () => {
    if (!id) return;
    setMigration(await api.migrations.get(id));
  }, [id]);

  useEffect(() => { load(); }, [load]);

  const handleAddTransform = async (type: "column_mapping" | "sql_expression" | "ai_generated") => {
    if (!id) return;
    const defaultConfigs = {
      column_mapping: { mappings: [] },
      sql_expression: { expression: "", output_column: "" },
      ai_generated: { prompt: "", generated_expression: "", approved: false },
    };
    await api.transformations.add(id, { type, config: defaultConfigs[type] });
    load();
  };

  const handleTest = async () => {
    if (!id) return;
    const result = await api.migrations.test(id);
    // TODO: show result in a modal/panel
    console.log("Test result:", result);
  };

  const handleRun = async () => {
    if (!id) return;
    await api.migrations.run(id);
    load();
  };

  const handleNodeClick = (_nodeId: string, _nodeType: string) => {
    // TODO: open config panel
  };

  if (!migration) return <div className="p-6">Loading...</div>;

  return (
    <div className="flex flex-1 flex-col">
      {/* Toolbar */}
      <div className="flex items-center gap-2 border-b px-4 py-2">
        <span className="font-semibold mr-4">{migration.name}</span>
        <Button variant="outline" size="sm" onClick={() => handleAddTransform("column_mapping")}>
          <Plus className="h-3 w-3 mr-1" /> Mapping
        </Button>
        <Button variant="outline" size="sm" onClick={() => handleAddTransform("sql_expression")}>
          <Plus className="h-3 w-3 mr-1" /> Expression
        </Button>
        <Button variant="outline" size="sm" onClick={() => handleAddTransform("ai_generated")}>
          <Plus className="h-3 w-3 mr-1" /> AI Transform
        </Button>
        <div className="flex-1" />
        <Button variant="outline" size="sm" onClick={handleTest}>
          <FlaskConical className="h-3 w-3 mr-1" /> Test
        </Button>
        <Button size="sm" onClick={handleRun}>
          <Play className="h-3 w-3 mr-1" /> Run
        </Button>
      </div>

      {/* Canvas */}
      <div className="flex-1">
        <Canvas migration={migration} onNodeClick={handleNodeClick} />
      </div>
    </div>
  );
}
