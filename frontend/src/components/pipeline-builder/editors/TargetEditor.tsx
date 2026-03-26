import { useEffect, useMemo, useState, useCallback } from "react";
import { RefreshCw } from "lucide-react";
import { api } from "@/lib/api";
import { SearchableSelect } from "@/components/ui/searchable-select";
import type { Pipeline } from "@/types/pipeline-definition";
import type { Connection, ColumnSchema } from "@/types/connection";

interface Props {
  pipeline: Pipeline;
  connections: Connection[];
  onUpdate: (updates: Partial<Pipeline>) => void;
}

export function TargetEditor({ pipeline, connections, onUpdate }: Props) {
  const [tables, setTables] = useState<string[]>([]);
  const [schema, setSchema] = useState<ColumnSchema[]>([]);
  const [loading, setLoading] = useState(false);

  const connectionOptions = useMemo(
    () => connections.map((c) => ({ label: c.name, value: c.id })),
    [connections],
  );

  const loadTables = useCallback((connId: string, refresh = false) => {
    setLoading(true);
    api.connections.tables(connId, refresh)
      .then(setTables)
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!pipeline.target_connection_id) return;
    loadTables(pipeline.target_connection_id);
  }, [pipeline.target_connection_id, loadTables]);

  useEffect(() => {
    if (!pipeline.target_connection_id || !pipeline.target_table) return;
    api.connections.schema(pipeline.target_connection_id, pipeline.target_table).then(setSchema).catch(() => {});
  }, [pipeline.target_connection_id, pipeline.target_table]);

  return (
    <div className="flex flex-col gap-4">
      <div>
        <label className="text-xs font-medium text-muted-foreground block mb-1">Connection</label>
        <SearchableSelect
          options={connectionOptions}
          value={pipeline.target_connection_id}
          onChange={(v) => onUpdate({ target_connection_id: v, target_table: "" })}
          placeholder="Search connections..."
        />
      </div>

      <div>
        <div className="flex items-center justify-between mb-1">
          <label className="text-xs font-medium text-muted-foreground">Table</label>
          {pipeline.target_connection_id && (
            <button
              type="button"
              onClick={() => loadTables(pipeline.target_connection_id, true)}
              disabled={loading}
              className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1"
            >
              <RefreshCw className={`h-3 w-3 ${loading ? "animate-spin" : ""}`} />
              Refresh
            </button>
          )}
        </div>
        <SearchableSelect
          items={tables}
          value={pipeline.target_table}
          onChange={(v) => onUpdate({ target_table: v })}
          placeholder="Search tables..."
          disabled={!pipeline.target_connection_id}
          loading={loading}
        />
      </div>

      {schema.length > 0 && (
        <div>
          <label className="text-xs font-medium text-muted-foreground block mb-1">Target Schema</label>
          <div className="border rounded p-2 text-xs font-mono max-h-[200px] overflow-y-auto">
            {schema.map((col) => (
              <div key={col.name} className="flex justify-between py-0.5">
                <span>{col.name}</span>
                <span className="text-muted-foreground">{col.type}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
