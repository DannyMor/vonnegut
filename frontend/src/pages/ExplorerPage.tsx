// frontend/src/pages/ExplorerPage.tsx
import { useEffect, useMemo, useRef, useState } from "react";
import { PageHeader } from "@/components/layout/PageHeader";
import { icons } from "@/config/iconRegistry";
import { api } from "@/lib/api";
import { fuzzyMatch } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { SearchableSelect } from "@/components/ui/searchable-select";
import { Search, Database, TableProperties, Inbox } from "lucide-react";
import type { Connection, ColumnSchema } from "@/types/connection";

export function ExplorerPage() {
  const [connections, setConnections] = useState<Connection[]>([]);
  const [selectedConn, setSelectedConn] = useState<string>("");
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>("");
  const [schema, setSchema] = useState<ColumnSchema[]>([]);
  const [sample, setSample] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tableFilter, setTableFilter] = useState("");
  const filterRef = useRef<HTMLInputElement>(null);

  const filteredTables = useMemo(() => {
    if (!tableFilter) return tables;
    return tables.filter((t) => fuzzyMatch(t, tableFilter));
  }, [tables, tableFilter]);

  const selectedConnection = useMemo(
    () => connections.find((c) => c.id === selectedConn),
    [connections, selectedConn],
  );

  useEffect(() => {
    api.connections.list().then(setConnections);
  }, []);

  useEffect(() => {
    if (!selectedConn) return;
    setTables([]);
    setSelectedTable("");
    setSchema([]);
    setSample([]);
    setError(null);
    setTableFilter("");
    setLoading(true);
    api.connections.tables(selectedConn)
      .then((t) => {
        setTables(t);
        setTimeout(() => filterRef.current?.focus(), 0);
      })
      .catch((e) => setError(e instanceof Error ? e.message : "Failed to load tables"))
      .finally(() => setLoading(false));
  }, [selectedConn]);

  useEffect(() => {
    if (!selectedConn || !selectedTable) return;
    setSchema([]);
    setSample([]);
    setError(null);
    setLoading(true);
    Promise.all([
      api.connections.schema(selectedConn, selectedTable),
      api.connections.sample(selectedConn, selectedTable),
    ]).then(([s, d]) => {
      setSchema(s);
      setSample(d);
    }).catch((e) => setError(e instanceof Error ? e.message : "Failed to load table data"))
      .finally(() => setLoading(false));
  }, [selectedConn, selectedTable]);

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <PageHeader icon={icons.nav_explorer} title="Explorer" description="Browse schemas and preview data" />
      <div className="flex flex-1 min-h-0">
        {/* Left sidebar: connection + table picker */}
        <div className="w-72 border-r p-4 flex flex-col min-h-0">
          <SearchableSelect
            options={connections.map((c) => ({ label: c.name, value: c.id }))}
            value={selectedConn}
            onChange={setSelectedConn}
            placeholder="Search connections..."
            className="mb-3"
          />
          {tables.length > 0 && (
            <div className="relative mb-2">
              <Search className="absolute left-2.5 top-2 h-3.5 w-3.5 text-muted-foreground" />
              <Input
                ref={filterRef}
                value={tableFilter}
                onChange={(e) => setTableFilter(e.target.value)}
                placeholder="Filter tables..."
                className="pl-8 h-7 text-xs"
              />
            </div>
          )}
          {tables.length > 0 && (
            <div className="text-xs text-muted-foreground mb-1 px-1">
              {filteredTables.length} of {tables.length} tables
            </div>
          )}
          <div className="flex-1 min-h-0 overflow-y-auto">
            <div className="flex flex-col gap-0.5">
              {filteredTables.map((t) => (
                <button
                  key={t}
                  onClick={() => setSelectedTable(t)}
                  title={t}
                  className={`text-left px-3 py-1.5 rounded text-xs font-mono truncate ${
                    t === selectedTable ? "bg-primary/10 text-primary font-medium" : "hover:bg-muted"
                  }`}
                >
                  {t}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Main area: schema + sample data */}
        <div className="flex-1 p-6 flex flex-col overflow-hidden">
          {error && (
            <div className="mb-4 rounded-lg border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive shrink-0">
              {error}
            </div>
          )}
          {loading && (
            <p className="text-muted-foreground mb-4 shrink-0">Loading...</p>
          )}
          {selectedTable ? (
            <div className="flex flex-col gap-4 min-h-0 flex-1">
              {/* Table title with database breadcrumb */}
              <div className="shrink-0">
                {selectedConnection && (
                  <div className="flex items-center gap-1.5 text-xs text-muted-foreground mb-1">
                    <Database className="h-3 w-3" />
                    <span>{selectedConnection.config.database || "default"}</span>
                    <span className="text-muted-foreground/50">/</span>
                    <span>public</span>
                  </div>
                )}
                <h2 className="text-lg font-semibold font-mono">{selectedTable}</h2>
              </div>

              {/* Schema — fixed header + scrollable body */}
              {schema.length > 0 && (
                <div className="flex flex-col min-h-0 max-h-[50vh] shrink-0">
                  <h3 className="text-sm font-medium text-muted-foreground mb-2 shrink-0 flex items-center gap-1.5">
                    <TableProperties className="h-3.5 w-3.5" />
                    Schema ({schema.length} columns)
                  </h3>
                  <div className="rounded-lg border flex flex-col min-h-0 flex-1">
                    {/* Static header */}
                    <div className="shrink-0 border-b border-foreground">
                      <table className="w-full text-sm table-fixed">
                        <colgroup><col className="w-[40%]" /><col className="w-[20%]" /><col className="w-[25%]" /><col className="w-[15%]" /></colgroup>
                        <thead>
                          <tr>
                            <th className="h-10 px-2 text-left align-middle font-medium whitespace-nowrap text-foreground">Column</th>
                            <th className="h-10 px-2 text-left align-middle font-medium whitespace-nowrap text-foreground">Type</th>
                            <th className="h-10 px-2 text-left align-middle font-medium whitespace-nowrap text-foreground">Default</th>
                            <th className="h-10 px-2 text-left align-middle font-medium whitespace-nowrap text-foreground">Constraints</th>
                          </tr>
                        </thead>
                      </table>
                    </div>
                    {/* Scrollable body */}
                    <div className="overflow-auto min-h-0 flex-1">
                      <table className="w-full text-sm table-fixed">
                        <colgroup><col className="w-[40%]" /><col className="w-[20%]" /><col className="w-[25%]" /><col className="w-[15%]" /></colgroup>
                        <tbody className="[&_tr:last-child]:border-0">
                          {schema.map((col) => {
                            const TypeIcon = icons[`type_${col.category}`] || icons.type_unknown;
                            return (
                              <tr key={col.name} className="border-b hover:bg-muted/50">
                                <td className="p-2 align-middle">
                                  <div className="flex items-center gap-2 truncate" title={col.name}>
                                    <TypeIcon className="h-4 w-4 text-muted-foreground shrink-0" />
                                    <span className={`truncate ${col.nullable ? "text-muted-foreground" : "font-medium"}`}>
                                      {col.name}
                                    </span>
                                  </div>
                                </td>
                                <td className="p-2 align-middle"><Badge variant="outline">{col.type}</Badge></td>
                                <td className="p-2 align-middle overflow-hidden">
                                  {col.default && (
                                    <span className="text-xs text-muted-foreground font-mono block truncate" title={col.default}>{col.default}</span>
                                  )}
                                </td>
                                <td className="p-2 align-middle whitespace-nowrap">
                                  <div className="flex items-center gap-1">
                                    {col.is_primary_key && (
                                      <icons.constraint_pk className="h-3.5 w-3.5 text-amber-500" />
                                    )}
                                    {col.foreign_key && (
                                      <span title={`→ ${col.foreign_key}`}>
                                        <icons.constraint_fk className="h-3.5 w-3.5 text-blue-500" />
                                      </span>
                                    )}
                                    {col.is_unique && (
                                      <icons.constraint_unique className="h-3.5 w-3.5 text-purple-500" />
                                    )}
                                  </div>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              )}

              {/* Sample data — bounded scrollable box */}
              {!loading && (
                <div className="flex flex-col min-h-0 shrink-0 max-h-[40vh]">
                  <h3 className="text-sm font-medium text-muted-foreground mb-2 shrink-0">
                    Sample{sample.length > 0 ? ` (${sample.length} rows)` : ""}
                  </h3>
                  {sample.length > 0 ? (
                    <div className="rounded-lg border flex flex-col min-h-0 flex-1">
                      {/* Static header */}
                      <div className="shrink-0 border-b border-foreground overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead>
                            <tr>
                              {Object.keys(sample[0]).map((key) => (
                                <th key={key} className="h-10 px-2 text-left align-middle font-medium whitespace-nowrap text-foreground font-mono text-xs">{key}</th>
                              ))}
                            </tr>
                          </thead>
                        </table>
                      </div>
                      {/* Scrollable body */}
                      <div className="overflow-auto min-h-0 flex-1">
                        <table className="w-full text-sm">
                          <tbody className="[&_tr:last-child]:border-0">
                            {sample.map((row, i) => (
                              <tr key={i} className="border-b hover:bg-muted/50">
                                {Object.values(row).map((val, j) => {
                                  const str = String(val ?? "null");
                                  return (
                                    <td key={j} className="p-2 align-middle overflow-hidden max-w-[200px]">
                                      <span
                                        className="text-xs font-mono block truncate cursor-default"
                                        title={str}
                                      >
                                        {str}
                                      </span>
                                    </td>
                                  );
                                })}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  ) : (
                    <div className="rounded-lg border border-dashed flex items-center justify-center py-8">
                      <div className="flex flex-col items-center gap-2 text-muted-foreground">
                        <Inbox className="h-8 w-8 text-muted-foreground/40" />
                        <span className="text-sm">No rows in this table</span>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          ) : (
            <p className="text-muted-foreground">Select a table to view its schema and data.</p>
          )}
        </div>
      </div>
    </div>
  );
}
