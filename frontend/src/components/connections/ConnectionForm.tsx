// frontend/src/components/connections/ConnectionForm.tsx
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { api } from "@/lib/api";
import type { Connection, ConnectionCreate } from "@/types/connection";

interface Props {
  open: boolean;
  onClose: () => void;
  onSave: (data: ConnectionCreate) => void;
  initial?: Connection | null;
}

export function ConnectionForm({ open, onClose, onSave, initial }: Props) {
  const [name, setName] = useState(initial?.name ?? "");
  const [type, setType] = useState<"postgres_direct" | "postgres_pod">(
    initial?.type ?? "postgres_direct"
  );
  // Direct fields
  const [host, setHost] = useState(initial?.config.host ?? "localhost");
  const [port, setPort] = useState(String(initial?.config.port ?? 5432));
  // Pod fields
  const [namespace, setNamespace] = useState(initial?.config.namespace ?? "default");
  const [podSelector, setPodSelector] = useState(initial?.config.pod_selector ?? "");
  const [pickStrategy, setPickStrategy] = useState<"first_ready" | "name_contains">(
    initial?.config.pick_strategy ?? "first_ready"
  );
  const [pickFilter, setPickFilter] = useState(initial?.config.pick_filter ?? "");
  const [container, setContainer] = useState(initial?.config.container ?? "");
  // Shared fields
  const [database, setDatabase] = useState(initial?.config.database ?? "");
  const [user, setUser] = useState(initial?.config.user ?? "");
  const [password, setPassword] = useState("");
  // Database discovery
  const [databases, setDatabases] = useState<string[]>([]);
  const [discovering, setDiscovering] = useState(false);
  const [discoverError, setDiscoverError] = useState<string | null>(null);

  const handleDiscover = async () => {
    if (!initial?.id) {
      setDiscoverError("Save the connection first, then use Discover.");
      return;
    }
    setDiscovering(true);
    setDiscoverError(null);
    try {
      const result = await api.connections.databases(initial.id);
      setDatabases(result);
    } catch (e: unknown) {
      setDiscoverError(e instanceof Error ? e.message : "Discovery failed");
    } finally {
      setDiscovering(false);
    }
  };

  const handleSubmit = () => {
    const config =
      type === "postgres_direct"
        ? { host, port: Number(port), database, user, password }
        : {
            namespace,
            pod_selector: podSelector,
            pick_strategy: pickStrategy,
            ...(pickStrategy === "name_contains" && pickFilter ? { pick_filter: pickFilter } : {}),
            ...(container ? { container } : {}),
            database,
            user,
            password,
          };
    onSave({ name, type, config });
    onClose();
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>{initial ? "Edit Connection" : "New Connection"}</DialogTitle>
        </DialogHeader>
        <div className="grid gap-4 py-4">
          <div className="grid gap-2">
            <Label>Name</Label>
            <Input value={name} onChange={(e) => setName(e.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label>Type</Label>
            <select
              className="border rounded px-3 py-2 text-sm"
              value={type}
              onChange={(e) => setType(e.target.value as typeof type)}
            >
              <option value="postgres_direct">Direct</option>
              <option value="postgres_pod">Kubernetes Pod</option>
            </select>
          </div>

          {type === "postgres_direct" ? (
            <div className="grid grid-cols-2 gap-2">
              <div><Label>Host</Label><Input value={host} onChange={(e) => setHost(e.target.value)} /></div>
              <div><Label>Port</Label><Input value={port} onChange={(e) => setPort(e.target.value)} /></div>
            </div>
          ) : (
            <>
              <div className="grid grid-cols-2 gap-2">
                <div><Label>Namespace</Label><Input value={namespace} onChange={(e) => setNamespace(e.target.value)} /></div>
                <div>
                  <Label>Pod Selector</Label>
                  <Input
                    value={podSelector}
                    onChange={(e) => setPodSelector(e.target.value)}
                    placeholder="app=postgres"
                  />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <Label>Pick Strategy</Label>
                  <select
                    className="w-full border rounded px-3 py-2 text-sm"
                    value={pickStrategy}
                    onChange={(e) => setPickStrategy(e.target.value as typeof pickStrategy)}
                  >
                    <option value="first_ready">First Ready</option>
                    <option value="name_contains">Name Contains</option>
                  </select>
                </div>
                {pickStrategy === "name_contains" && (
                  <div>
                    <Label>Name Filter</Label>
                    <Input
                      value={pickFilter}
                      onChange={(e) => setPickFilter(e.target.value)}
                      placeholder="primary"
                    />
                  </div>
                )}
              </div>
              <div>
                <Label>Container <span className="text-xs text-muted-foreground">(optional, for multi-container pods)</span></Label>
                <Input
                  value={container}
                  onChange={(e) => setContainer(e.target.value)}
                  placeholder="Leave empty for single-container pods"
                />
              </div>
            </>
          )}

          <div className="grid grid-cols-2 gap-2">
            <div><Label>User</Label><Input value={user} onChange={(e) => setUser(e.target.value)} /></div>
            <div><Label>Password</Label><Input type="password" value={password} onChange={(e) => setPassword(e.target.value)} /></div>
          </div>

          {/* Database field with discovery */}
          <div className="grid gap-2">
            <div className="flex items-center justify-between">
              <Label>Database</Label>
              {initial?.id && (
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={handleDiscover}
                  disabled={discovering}
                  className="text-xs h-6"
                >
                  {discovering ? "Discovering..." : "Discover databases"}
                </Button>
              )}
            </div>
            {databases.length > 0 ? (
              <select
                className="border rounded px-3 py-2 text-sm"
                value={database}
                onChange={(e) => setDatabase(e.target.value)}
              >
                <option value="">Select a database...</option>
                {databases.map((db) => (
                  <option key={db} value={db}>{db}</option>
                ))}
              </select>
            ) : (
              <Input
                value={database}
                onChange={(e) => setDatabase(e.target.value)}
                placeholder="Enter database name or save first to discover"
              />
            )}
            {discoverError && (
              <p className="text-xs text-destructive">{discoverError}</p>
            )}
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={handleSubmit}>Save</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
