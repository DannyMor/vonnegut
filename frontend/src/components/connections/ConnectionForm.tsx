// frontend/src/components/connections/ConnectionForm.tsx
import { useReducer } from "react";
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { SearchableSelect } from "@/components/ui/searchable-select";
import { api } from "@/lib/api";
import type { Connection, ConnectionCreate, ConnectionConfig } from "@/types/connection";

interface Props {
  open: boolean;
  onClose: () => void;
  onSave: (data: ConnectionCreate) => Promise<void>;
  initial?: Connection | null;
}

interface FormState {
  name: string;
  connType: "postgres_direct" | "postgres_pod";
  host: string;
  port: string;
  namespace: string;
  podSelector: string;
  pickStrategy: "first_ready" | "name_contains";
  pickFilter: string;
  container: string;
  database: string;
  user: string;
  password: string;
  passwordTouched: boolean;
  databases: string[];
  discovering: boolean;
  discoverError: string | null;
  testing: boolean;
  testResult: { status: string; message: string } | null;
}

type FormAction =
  | { type: "set"; field: keyof FormState; value: string | boolean }
  | { type: "discover_start" }
  | { type: "discover_done"; databases: string[] }
  | { type: "discover_error"; error: string }
  | { type: "test_start" }
  | { type: "test_done"; result: { status: string; message: string } };

function formReducer(state: FormState, action: FormAction): FormState {
  switch (action.type) {
    case "set":
      return { ...state, [action.field]: action.value };
    case "discover_start":
      return { ...state, discovering: true, discoverError: null };
    case "discover_done":
      return { ...state, discovering: false, databases: action.databases };
    case "discover_error":
      return { ...state, discovering: false, discoverError: action.error };
    case "test_start":
      return { ...state, testing: true, testResult: null };
    case "test_done":
      return { ...state, testing: false, testResult: action.result };
    default:
      return state;
  }
}

function initState(initial?: Connection | null): FormState {
  const cfg = initial?.config;
  const isPod = cfg?.type === "postgres_pod";
  return {
    name: initial?.name ?? "",
    connType: cfg?.type ?? "postgres_direct",
    host: cfg?.host ?? "localhost",
    port: String(cfg?.port ?? 5432),
    namespace: isPod && cfg.type === "postgres_pod" ? cfg.namespace : "default",
    podSelector: isPod && cfg.type === "postgres_pod" ? cfg.pod_selector : "",
    pickStrategy: isPod && cfg.type === "postgres_pod" ? cfg.pick_strategy : "first_ready",
    pickFilter: isPod && cfg.type === "postgres_pod" ? cfg.pick_filter ?? "" : "",
    container: isPod && cfg.type === "postgres_pod" ? cfg.container ?? "" : "",
    database: initial?.config.database ?? "",
    user: initial?.config.user ?? "",
    password: "",
    passwordTouched: false,
    databases: [],
    discovering: false,
    discoverError: null,
    testing: false,
    testResult: null,
  };
}

function parseConnectionString(raw: string): Partial<Record<"host" | "port" | "user" | "password" | "database", string>> {
  const result: Partial<Record<"host" | "port" | "user" | "password" | "database", string>> = {};
  let uri = raw.trim();
  // Normalize scheme so URL constructor can parse it
  if (!uri.includes("://")) uri = `postgres://${uri}`;
  uri = uri.replace(/^postgres(ql)?:\/\//, "http://");
  try {
    const url = new URL(uri);
    if (url.hostname) result.host = url.hostname;
    if (url.port) result.port = url.port;
    if (url.username) result.user = decodeURIComponent(url.username);
    if (url.password) result.password = decodeURIComponent(url.password);
    const db = url.pathname.replace(/^\//, "");
    if (db) result.database = decodeURIComponent(db);
  } catch {
    // not a valid URI — ignore
  }
  return result;
}

export function ConnectionForm({ open, onClose, onSave, initial }: Props) {
  const [s, dispatch] = useReducer(formReducer, initial, initState);

  const set = (field: keyof FormState, value: string | boolean) =>
    dispatch({ type: "set", field, value });

  const buildConfig = (): ConnectionConfig => {
    const pw = (!initial || s.passwordTouched) ? s.password : "";
    return s.connType === "postgres_direct"
      ? { type: "postgres_direct" as const, host: s.host, port: Number(s.port), database: s.database, user: s.user, password: pw }
      : {
          type: "postgres_pod" as const,
          host: s.host,
          port: Number(s.port),
          database: s.database,
          user: s.user,
          password: pw,
          namespace: s.namespace,
          pod_selector: s.podSelector,
          pick_strategy: s.pickStrategy,
          ...(s.pickStrategy === "name_contains" && s.pickFilter ? { pick_filter: s.pickFilter } : {}),
          ...(s.container ? { container: s.container } : {}),
        };
  };

  const handleTest = async () => {
    dispatch({ type: "test_start" });
    try {
      const result = await api.connections.testConfig({ name: s.name || "test", config: buildConfig() });
      dispatch({ type: "test_done", result });
    } catch (e: unknown) {
      dispatch({ type: "test_done", result: { status: "error", message: e instanceof Error ? e.message : "Test failed" } });
    }
  };

  const handleDiscover = async () => {
    dispatch({ type: "discover_start" });
    try {
      const result = await api.connections.discoverDatabases({ name: s.name || "discover", config: buildConfig() });
      dispatch({ type: "discover_done", databases: result });
    } catch (e: unknown) {
      dispatch({ type: "discover_error", error: e instanceof Error ? e.message : "Discovery failed" });
    }
  };

  const handleSubmit = async () => {
    try {
      await onSave({ name: s.name, config: buildConfig() });
      onClose();
    } catch (e) {
      dispatch({ type: "test_done", result: { status: "error", message: e instanceof Error ? e.message : "Save failed" } });
    }
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-lg max-h-[90vh] overflow-hidden">
        <DialogHeader>
          <DialogTitle>{initial ? "Edit Connection" : "New Connection"}</DialogTitle>
        </DialogHeader>
        <div className="grid gap-4 py-4 overflow-y-auto max-h-[calc(90vh-10rem)]">
          <div className="grid gap-2">
            <Label>Name</Label>
            <Input value={s.name} onChange={(e) => set("name", e.target.value)} onBlur={(e) => set("name", e.target.value.trim())} />
          </div>
          <div className="grid gap-2">
            <Label>Connection String <span className="text-xs text-muted-foreground font-normal">(optional — paste to auto-fill)</span></Label>
            <Input
              placeholder="postgres://user:pass@host:5432/dbname"
              onChange={(e) => {
                const parsed = parseConnectionString(e.target.value);
                if (parsed.host) set("host", parsed.host);
                if (parsed.port) set("port", parsed.port);
                if (parsed.user) set("user", parsed.user);
                if (parsed.password) { set("password", parsed.password); set("passwordTouched", true); }
                if (parsed.database) set("database", parsed.database);
              }}
            />
          </div>
          <div className="grid gap-2">
            <Label>Type</Label>
            <Select value={s.connType} onValueChange={(v: string | null) => { if (v) set("connType", v); }}>
              <SelectTrigger className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="postgres_direct">Direct</SelectItem>
                <SelectItem value="postgres_pod">Kubernetes Pod</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {s.connType === "postgres_pod" && (
            <div className="rounded-lg border p-4">
              <h3 className="text-sm font-medium mb-3 text-muted-foreground">Pod Access</h3>
              <div className="grid gap-3">
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <Label>Namespace</Label>
                    <Input value={s.namespace} onChange={(e) => set("namespace", e.target.value)} />
                  </div>
                  <div>
                    <Label>Pod Selector</Label>
                    <Input
                      value={s.podSelector}
                      onChange={(e) => set("podSelector", e.target.value)}
                      placeholder="app=postgres"
                    />
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <Label>Pick Strategy</Label>
                    <Select value={s.pickStrategy} onValueChange={(v: string | null) => { if (v) set("pickStrategy", v); }}>
                      <SelectTrigger className="w-full">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="first_ready">First Ready</SelectItem>
                        <SelectItem value="name_contains">Name Contains</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  {s.pickStrategy === "name_contains" && (
                    <div>
                      <Label>Pick Filter</Label>
                      <Input
                        value={s.pickFilter}
                        onChange={(e) => set("pickFilter", e.target.value)}
                        placeholder="primary"
                      />
                    </div>
                  )}
                </div>
                <div>
                  <Label>Container <span className="text-xs text-muted-foreground">(optional)</span></Label>
                  <Input
                    value={s.container}
                    onChange={(e) => set("container", e.target.value)}
                    placeholder="For multi-container pods"
                  />
                </div>
              </div>
            </div>
          )}

          <div className="rounded-lg border p-4">
            <h3 className="text-sm font-medium mb-3 text-muted-foreground">Database</h3>
            <div className="grid gap-3">
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <Label>Host</Label>
                  <Input value={s.host} onChange={(e) => set("host", e.target.value)} />
                </div>
                <div>
                  <Label>Port</Label>
                  <Input value={s.port} onChange={(e) => set("port", e.target.value)} />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <Label>User</Label>
                  <Input value={s.user} onChange={(e) => set("user", e.target.value)} />
                </div>
                <div>
                  <Label>Password</Label>
                  <Input
                    type="password"
                    value={s.password}
                    onChange={(e) => { set("password", e.target.value); set("passwordTouched", true); }}
                    placeholder={initial ? "Leave blank to keep current" : ""}
                  />
                </div>
              </div>
              <div className="grid gap-2">
                <div className="flex items-center justify-between">
                  <Label>Database</Label>
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={handleDiscover}
                    disabled={s.discovering}
                    className="text-xs h-6"
                  >
                    {s.discovering ? "Discovering..." : "Discover databases"}
                  </Button>
                </div>
                {s.databases.length > 0 ? (
                  <SearchableSelect
                    items={s.databases}
                    value={s.database}
                    onChange={(v) => set("database", v)}
                    placeholder="Search databases..."
                  />
                ) : (
                  <Input
                    value={s.database}
                    onChange={(e) => set("database", e.target.value)}
                    placeholder="Enter database name or use Discover"
                  />
                )}
                {s.discoverError && (
                  <div className="text-xs text-destructive border border-destructive/30 rounded px-2 py-1.5 max-h-20 overflow-auto break-all">{s.discoverError}</div>
                )}
              </div>

              {/* Test connection */}
              <div className="flex items-center gap-2 pt-1">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={handleTest}
                  disabled={s.testing}
                  className="text-xs"
                >
                  {s.testing ? "Testing..." : "Test Connection"}
                </Button>
                {s.testResult && (
                  <span className={`text-xs ${s.testResult.status === "ok" ? "text-green-600" : "text-destructive"}`}>
                    {s.testResult.status === "ok" ? "Connected successfully" : s.testResult.message}
                  </span>
                )}
              </div>
            </div>
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
