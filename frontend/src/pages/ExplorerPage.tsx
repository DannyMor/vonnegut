// frontend/src/pages/ExplorerPage.tsx
import { useEffect, useState } from "react";
import { PageHeader } from "@/components/layout/PageHeader";
import { icons } from "@/config/iconRegistry";
import { api } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { Connection } from "@/types/connection";

export function ExplorerPage() {
  const [connections, setConnections] = useState<Connection[]>([]);
  const [selectedConn, setSelectedConn] = useState<string>("");
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>("");
  const [schema, setSchema] = useState<{ column: string; type: string; nullable: boolean; is_primary_key: boolean }[]>([]);
  const [sample, setSample] = useState<Record<string, unknown>[]>([]);

  useEffect(() => {
    api.connections.list().then(setConnections);
  }, []);

  useEffect(() => {
    if (!selectedConn) return;
    setTables([]);
    setSelectedTable("");
    setSchema([]);
    setSample([]);
    api.connections.tables(selectedConn).then(setTables);
  }, [selectedConn]);

  useEffect(() => {
    if (!selectedConn || !selectedTable) return;
    setSchema([]);
    setSample([]);
    Promise.all([
      api.connections.schema(selectedConn, selectedTable),
      api.connections.sample(selectedConn, selectedTable),
    ]).then(([s, d]) => {
      setSchema(s);
      setSample(d);
    });
  }, [selectedConn, selectedTable]);

  return (
    <div className="flex flex-1 flex-col">
      <PageHeader icon={icons.nav_explorer} title="Explorer" description="Browse schemas and preview data" />
      <div className="flex flex-1">
        {/* Left sidebar: connection + table picker */}
        <div className="w-64 border-r p-4">
          <select
            className="w-full border rounded px-3 py-2 text-sm mb-4"
            value={selectedConn}
            onChange={(e) => setSelectedConn(e.target.value)}
          >
            <option value="">Select connection...</option>
            {connections.map((c) => (
              <option key={c.id} value={c.id}>{c.name}</option>
            ))}
          </select>
          <ScrollArea className="h-[calc(100vh-200px)]">
            <div className="flex flex-col gap-1">
              {tables.map((t) => (
                <button
                  key={t}
                  onClick={() => setSelectedTable(t)}
                  className={`text-left px-3 py-1.5 rounded text-sm ${
                    t === selectedTable ? "bg-primary/10 text-primary font-medium" : "hover:bg-muted"
                  }`}
                >
                  {t}
                </button>
              ))}
            </div>
          </ScrollArea>
        </div>

        {/* Main area: schema + sample data */}
        <div className="flex-1 p-6 overflow-auto">
          {selectedTable ? (
            <>
              <h2 className="text-lg font-semibold mb-4">{selectedTable}</h2>

              {/* Schema */}
              {schema.length > 0 && (
                <div className="mb-6">
                  <h3 className="text-sm font-medium mb-2 text-muted-foreground">Schema</h3>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Column</TableHead>
                        <TableHead>Type</TableHead>
                        <TableHead>Nullable</TableHead>
                        <TableHead>PK</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {schema.map((col) => (
                        <TableRow key={col.column}>
                          <TableCell className="font-mono">{col.column}</TableCell>
                          <TableCell><Badge variant="outline">{col.type}</Badge></TableCell>
                          <TableCell>{col.nullable ? "Yes" : "No"}</TableCell>
                          <TableCell>{col.is_primary_key ? "Yes" : ""}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}

              {/* Sample data */}
              {sample.length > 0 && (
                <div>
                  <h3 className="text-sm font-medium mb-2 text-muted-foreground">Sample Data ({sample.length} rows)</h3>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          {Object.keys(sample[0]).map((key) => (
                            <TableHead key={key} className="font-mono">{key}</TableHead>
                          ))}
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {sample.map((row, i) => (
                          <TableRow key={i}>
                            {Object.values(row).map((val, j) => (
                              <TableCell key={j}>{String(val ?? "null")}</TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </div>
              )}
            </>
          ) : (
            <p className="text-muted-foreground">Select a table to view its schema and data.</p>
          )}
        </div>
      </div>
    </div>
  );
}
