import { useEffect, useMemo, useState, useCallback } from "react";
import { RefreshCw } from "lucide-react";
import { api } from "@/lib/api";
import { SearchableSelect } from "@/components/ui/searchable-select";
import { CodeEditor, languageForConnection } from "@/components/ui/code-editor";
import type { Pipeline } from "@/types/pipeline-definition";
import type { Connection } from "@/types/connection";

interface Props {
  pipeline: Pipeline;
  connections: Connection[];
  onUpdate: (updates: Partial<Pipeline>) => void;
}

export function SourceEditor({ pipeline, connections, onUpdate }: Props) {
  const [tables, setTables] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);

  const connectionOptions = useMemo(
    () => connections.map((c) => ({ label: c.name, value: c.id })),
    [connections],
  );

  const sourceConnection = useMemo(
    () => connections.find((c) => c.id === pipeline.source_connection_id) ?? null,
    [connections, pipeline.source_connection_id],
  );

  const sqlLanguage = useMemo(
    () => languageForConnection(sourceConnection?.config),
    [sourceConnection],
  );

  const loadTables = useCallback((connId: string, refresh = false) => {
    setLoading(true);
    api.connections.tables(connId, refresh)
      .then(setTables)
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!pipeline.source_connection_id) return;
    loadTables(pipeline.source_connection_id);
  }, [pipeline.source_connection_id, loadTables]);

  const handleConnectionChange = (connId: string) => {
    onUpdate({ source_connection_id: connId, source_table: "", source_query: "", source_schema: [] });
  };

  const handleTableChange = async (table: string) => {
    if (pipeline.source_connection_id && table) {
      const schema = await api.connections.schema(pipeline.source_connection_id, table);
      const columns = schema.map((c) => c.name).join(", ");
      const query = `SELECT ${columns} FROM ${table}`;
      onUpdate({
        source_table: table,
        source_query: query,
        source_schema: schema.map((c) => ({ name: c.name, type: c.type })),
      });
    } else {
      onUpdate({ source_table: table });
    }
  };

  return (
    <div className="flex flex-col gap-4 h-full">
      <div className="shrink-0">
        <label className="text-xs font-medium text-muted-foreground block mb-1">Connection</label>
        <SearchableSelect
          options={connectionOptions}
          value={pipeline.source_connection_id}
          onChange={handleConnectionChange}
          placeholder="Search connections..."
        />
      </div>

      <div className="shrink-0">
        <div className="flex items-center justify-between mb-1">
          <label className="text-xs font-medium text-muted-foreground">Table</label>
          {pipeline.source_connection_id && (
            <button
              type="button"
              onClick={() => loadTables(pipeline.source_connection_id, true)}
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
          value={pipeline.source_table}
          onChange={handleTableChange}
          placeholder="Search tables..."
          disabled={!pipeline.source_connection_id}
          loading={loading}
        />
      </div>

      <div className="flex flex-col flex-1 min-h-0">
        <label className="text-xs font-medium text-muted-foreground block mb-1 shrink-0">Source Query</label>
        <CodeEditor
          value={pipeline.source_query}
          onChange={(v) => onUpdate({ source_query: v })}
          language={sqlLanguage}
          placeholder="SELECT col1, col2 FROM table"
          flexGrow
        />
      </div>
    </div>
  );
}
