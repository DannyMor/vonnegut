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
  const [host, setHost] = useState(initial?.config.host ?? "localhost");
  const [port, setPort] = useState(String(initial?.config.port ?? 5432));
  const [database, setDatabase] = useState(initial?.config.database ?? "");
  const [user, setUser] = useState(initial?.config.user ?? "");
  const [password, setPassword] = useState("");
  const [namespace, setNamespace] = useState(initial?.config.namespace ?? "default");
  const [podName, setPodName] = useState(initial?.config.pod_name ?? "");
  const [container, setContainer] = useState(initial?.config.container ?? "postgres");

  const handleSubmit = () => {
    const config =
      type === "postgres_direct"
        ? { host, port: Number(port), database, user, password }
        : { namespace, pod_name: podName, container, database, user, password };
    onSave({ name, type, config });
    onClose();
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent>
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
            <>
              <div className="grid grid-cols-2 gap-2">
                <div><Label>Host</Label><Input value={host} onChange={(e) => setHost(e.target.value)} /></div>
                <div><Label>Port</Label><Input value={port} onChange={(e) => setPort(e.target.value)} /></div>
              </div>
            </>
          ) : (
            <>
              <div className="grid grid-cols-2 gap-2">
                <div><Label>Namespace</Label><Input value={namespace} onChange={(e) => setNamespace(e.target.value)} /></div>
                <div><Label>Pod Name</Label><Input value={podName} onChange={(e) => setPodName(e.target.value)} /></div>
              </div>
              <div><Label>Container</Label><Input value={container} onChange={(e) => setContainer(e.target.value)} /></div>
            </>
          )}
          <div><Label>Database</Label><Input value={database} onChange={(e) => setDatabase(e.target.value)} /></div>
          <div className="grid grid-cols-2 gap-2">
            <div><Label>User</Label><Input value={user} onChange={(e) => setUser(e.target.value)} /></div>
            <div><Label>Password</Label><Input type="password" value={password} onChange={(e) => setPassword(e.target.value)} /></div>
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
