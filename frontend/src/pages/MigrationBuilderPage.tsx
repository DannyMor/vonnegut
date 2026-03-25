import { useEffect, useState, useCallback, useRef } from "react";
import { useParams, useNavigate } from "react-router";
import { api } from "@/lib/api";
import { Canvas } from "@/components/migration-builder/Canvas";
import { EditorPanel } from "@/components/migration-builder/EditorPanel";
import { RunLog, type LogEntry } from "@/components/migration-builder/RunLog";
import { Button } from "@/components/ui/button";
import { Play, FlaskConical, Save, Loader2, PanelBottomOpen, PanelBottomClose, CircleCheck, CircleX, Pencil } from "lucide-react";
import type { Migration } from "@/types/migration";
import type { Connection } from "@/types/connection";
import type { StepType, PipelineStep, PipelineTestResult, ColumnDef, ValidationStatus } from "@/types/pipeline";

type BottomTab = "editor" | "run";

const EMPTY_MIGRATION: Migration = {
  id: "", name: "Untitled Migration",
  source_connection_id: "", target_connection_id: "",
  source_table: "", target_table: "",
  source_query: "", source_schema: [],
  status: "draft", truncate_target: false,
  rows_processed: null, total_rows: null, error_message: null,
  created_at: "", updated_at: "",
  transformations: [], pipeline_steps: [],
};


export function MigrationBuilderPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const isNew = id === "new";

  const [migration, setMigration] = useState<Migration | null>(isNew ? { ...EMPTY_MIGRATION } : null);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<PipelineTestResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [running, setRunning] = useState(false);
  const [panelOpen, setPanelOpen] = useState(false);
  const [bottomTab, setBottomTab] = useState<BottomTab>("editor");
  const [logEntries, setLogEntries] = useState<LogEntry[]>([]);
  const [testStartedAt, setTestStartedAt] = useState<number | null>(null);
  const [validationStatus, setValidationStatus] = useState<ValidationStatus>("DRAFT");
  const abortRef = useRef<{ abort: () => void } | null>(null);
  const lastNodeIdRef = useRef<string>("source");

  const loadValidation = useCallback(async () => {
    if (!id || isNew) return;
    try {
      const result = await api.migrations.validation(id);
      setValidationStatus(result.validation_status as ValidationStatus);
    } catch { /* ignore — endpoint may not exist for new migrations */ }
  }, [id, isNew]);

  const load = useCallback(async () => {
    if (!id || isNew) return;
    setMigration(await api.migrations.get(id));
    loadValidation();
  }, [id, isNew, loadValidation]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { api.connections.list().then(setConnections).catch(() => {}); }, []);
  useEffect(() => { return () => { abortRef.current?.abort(); }; }, []);

  // --- Save: persist migration + sync steps ---
  const handleSave = async () => {
    if (!migration) return;
    setError(null);
    setSaving(true);
    const steps = migration.pipeline_steps || [];

    try {
    if (isNew) {
      const created = await api.migrations.create({
        name: migration.name,
        source_connection_id: migration.source_connection_id,
        target_connection_id: migration.target_connection_id,
        source_table: migration.source_table,
        target_table: migration.target_table,
        source_query: migration.source_query,
        source_schema: migration.source_schema,
        truncate_target: migration.truncate_target,
      });
      // Create all local steps on the backend
      for (const step of steps) {
        await api.pipelineSteps.add(created.id, {
          step_type: step.step_type,
          name: step.name,
          config: step.config,
        });
      }
      navigate(`/migrations/${created.id}`, { replace: true });
    } else if (id) {
      await api.migrations.update(id, {
        name: migration.name,
        source_table: migration.source_table,
        target_table: migration.target_table,
        source_query: migration.source_query,
        source_schema: migration.source_schema,
        truncate_target: migration.truncate_target,
      });
      // Sync steps: delete removed, create new, update existing
      const serverMig = await api.migrations.get(id);
      const serverStepIds = new Set((serverMig.pipeline_steps || []).map((s) => s.id));
      const localStepIds = new Set(steps.map((s) => s.id));

      // Delete steps that were removed locally
      for (const sid of serverStepIds) {
        if (!localStepIds.has(sid)) await api.pipelineSteps.delete(id, sid);
      }
      // Delete all server steps and recreate in order to preserve ordering
      // (simpler than diffing positions)
      for (const sid of serverStepIds) {
        if (localStepIds.has(sid)) await api.pipelineSteps.delete(id, sid);
      }
      // Track which position the selected step is at, so we can remap after recreate
      const selectedStepIdx = steps.findIndex((s) => s.id === selectedNodeId);
      for (const step of steps) {
        await api.pipelineSteps.add(id, {
          step_type: step.step_type,
          name: step.name,
          config: step.config,
          description: step.description ?? undefined,
        });
      }
      await load();
      // Remap selectedNodeId and lastNodeIdRef to new server ID at the same position
      if (selectedStepIdx >= 0) {
        setMigration((cur) => {
          const newSteps = cur?.pipeline_steps || [];
          if (selectedStepIdx < newSteps.length) {
            const newId = newSteps[selectedStepIdx].id;
            setSelectedNodeId(newId);
            lastNodeIdRef.current = newId;
          }
          return cur;
        });
      }
    }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Save failed");
      throw e;
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async () => {
    if (!id || isNew || !migration) return;
    setError(null);
    setTesting(true);
    setLogEntries([]);
    setPanelOpen(true);
    setBottomTab("run");
    try {
      await handleSave();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Save failed before test");
      setTesting(false);
      return;
    }
    const now = new Date().toISOString();
    const startTime = Date.now();
    setTestStartedAt(startTime);
    setLogEntries([{ type: "info", message: `Starting test for \`${migration.name.trim()}\`...`, timestamp: now }]);
    setValidationStatus("VALIDATING");
    let hadErrors = false;
    abortRef.current = api.migrations.testStream(id, (event) => {
      const entry = event as unknown as LogEntry;
      if (entry.type === "step_error" || entry.type === "error") {
        hadErrors = true;
      }
      if (entry.type === "done") {
        setTesting(false);
        setTestStartedAt(null);
        const totalMs = Date.now() - startTime;
        const dur = totalMs < 1000 ? `${totalMs}ms` : `${(totalMs / 1000).toFixed(2)}s`;
        const message = hadErrors ? `Test failed in ${dur}` : `Test passed in ${dur}`;
        setLogEntries((prev) => [...prev, { type: "info", message, timestamp: new Date().toISOString() }]);
        loadValidation();
        return;
      }
      if (entry.type === "result") {
        setTestResults((event as { data: PipelineTestResult }).data);
        return;
      }
      setLogEntries((prev) => [...prev, entry]);
    });
  };

  const handleRun = async () => {
    if (!id || isNew || !migration) return;
    setError(null);
    setRunning(true);
    setLogEntries([]);
    setPanelOpen(true);
    setBottomTab("run");
    try {
      await handleSave();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Save failed before run");
      setRunning(false);
      return;
    }
    const now = new Date().toISOString();
    const startTime = Date.now();
    setTestStartedAt(startTime);
    setLogEntries([{ type: "info", message: `Starting run for \`${migration.name.trim()}\`...`, timestamp: now }]);
    let hadErrors = false;
    abortRef.current = api.migrations.runStream(id, (event) => {
      const entry = event as unknown as LogEntry;
      if (entry.type === "step_error" || entry.type === "error") {
        hadErrors = true;
      }
      if (entry.type === "done") {
        setRunning(false);
        setTestStartedAt(null);
        const totalMs = Date.now() - startTime;
        const dur = totalMs < 1000 ? `${totalMs}ms` : `${(totalMs / 1000).toFixed(2)}s`;
        const message = hadErrors ? `Run failed in ${dur}` : `Run completed in ${dur}`;
        setLogEntries((prev) => [...prev, { type: "info", message, timestamp: new Date().toISOString() }]);
        load();
        return;
      }
      if (entry.type === "result") {
        return;
      }
      setLogEntries((prev) => [...prev, entry]);
    });
  };

  // --- Local step management (no API calls) ---
  const handleAddStep = useCallback((type: StepType, afterNodeId: string) => {
    setMigration((prev) => {
      if (!prev) return prev;
      const steps = [...(prev.pipeline_steps || [])];
      const stepCount = steps.length;
      const defaultNames: Record<StepType, string> = {
        sql: `SQL Transform ${stepCount + 1}`,
        code: `Code Transform ${stepCount + 1}`,
        ai: `AI Assistant ${stepCount + 1}`,
      };
      const defaultConfigs: Record<StepType, Record<string, unknown>> = {
        sql: { expression: "SELECT * FROM {prev}" },
        code: { function_code: "def transform(df: pl.DataFrame) -> pl.DataFrame:\n    return df\n" },
        ai: { prompt: "", generated_type: null, generated_code: null, approved: false },
      };
      const newStep: PipelineStep = {
        id: crypto.randomUUID(),
        migration_id: prev.id,
        name: defaultNames[type],
        description: null,
        position: 0,
        step_type: type,
        config: defaultConfigs[type],
        created_at: "",
        updated_at: "",
      };

      // Insert after the specified node
      if (afterNodeId === "source") {
        steps.splice(0, 0, newStep);
      } else {
        const idx = steps.findIndex((s) => s.id === afterNodeId);
        steps.splice(idx + 1, 0, newStep);
      }
      // Recompute positions
      steps.forEach((s, i) => { s.position = i; });
      return { ...prev, pipeline_steps: steps };
    });
    setValidationStatus("DRAFT");
  }, []);

  const handleDeleteStep = useCallback((stepId: string) => {
    setMigration((prev) => {
      if (!prev) return prev;
      const steps = (prev.pipeline_steps || []).filter((s) => s.id !== stepId);
      steps.forEach((s, i) => { s.position = i; });
      return { ...prev, pipeline_steps: steps };
    });
    setValidationStatus("DRAFT");
    setSelectedNodeId((cur) => cur === stepId ? null : cur);
    if (lastNodeIdRef.current === stepId) lastNodeIdRef.current = "source";
  }, []);

  const handleUpdateMigration = useCallback((updates: Partial<Migration>) => {
    setMigration((prev) => {
      if (!prev) return prev;
      return { ...prev, ...updates };
    });
  }, []);

  const handleUpdateStep = useCallback((stepId: string, updates: Record<string, unknown>) => {
    setMigration((prev) => {
      if (!prev) return prev;
      const steps = (prev.pipeline_steps || []).map((s) => {
        if (s.id !== stepId) return s;
        // Merge updates — handle nested config updates
        const merged = { ...s };
        for (const [key, val] of Object.entries(updates)) {
          if (key === "config") {
            merged.config = val as Record<string, unknown>;
          } else {
            (merged as Record<string, unknown>)[key] = val;
          }
        }
        return merged;
      });
      return { ...prev, pipeline_steps: steps };
    });
    if ("config" in updates) setValidationStatus("DRAFT");
  }, []);

  // Derived state
  const steps = migration?.pipeline_steps || [];
  const isStepSelected = selectedNodeId && selectedNodeId !== "source" && selectedNodeId !== "target";
  const selectedStep: PipelineStep | null = isStepSelected
    ? (steps.find((s) => s.id === selectedNodeId) || null)
    : null;
  // If selectedNodeId points to a step that no longer exists, treat as no selection
  const effectiveSelectedNodeId = (isStepSelected && !selectedStep) ? null : selectedNodeId;

  // The node currently being edited in the bottom panel (last selected, or "source" as default)
  const editingNodeId = lastNodeIdRef.current;
  const isEditingStep = editingNodeId !== "source" && editingNodeId !== "target";
  const editingStep: PipelineStep | null = isEditingStep
    ? (steps.find((s) => s.id === editingNodeId) || null)
    : null;
  // If the editing node was a step that got deleted, fall back to source
  if (isEditingStep && !editingStep) lastNodeIdRef.current = "source";
  const safeEditingNodeId = (isEditingStep && !editingStep) ? "source" : editingNodeId;

  const getInputSchema = (nodeId: string): ColumnDef[] => {
    if (nodeId === "source" || !migration) return [];
    if (nodeId === "target") {
      if (steps.length === 0) return migration.source_schema || [];
      const lastResult = testResults?.steps?.find((r) => r.node_id === steps[steps.length - 1].id);
      return lastResult?.schema || migration.source_schema || [];
    }
    const stepIdx = steps.findIndex((s) => s.id === nodeId);
    if (stepIdx === 0) return migration.source_schema || [];
    const prevResult = testResults?.steps?.find((r) => r.node_id === steps[stepIdx - 1].id);
    return prevResult?.schema || migration.source_schema || [];
  };

  const getOutputSchema = (nodeId: string): ColumnDef[] => {
    if (nodeId === "target") return [];
    if (nodeId === "source") return migration?.source_schema || [];
    const stepResult = testResults?.steps?.find((r) => r.node_id === nodeId);
    return stepResult?.schema || [];
  };

  if (!migration) return <div className="p-6">Loading...</div>;

  return (
    <div className="flex flex-1 flex-col">
      {/* Toolbar */}
      <div className="flex items-center gap-2 border-b px-4 py-2">
        <input
          className="font-semibold mr-4 bg-transparent border-none outline-none"
          value={migration.name}
          onChange={(e) => handleUpdateMigration({ name: e.target.value })}
          onBlur={(e) => { const v = e.target.value.trim(); if (v !== e.target.value) handleUpdateMigration({ name: v || "Untitled Migration" }); }}
        />
        {!isNew && (
          <span className={`inline-flex items-center gap-1 text-xs font-medium px-2 py-0.5 rounded-full ${
            validationStatus === "VALID" ? "bg-green-500/15 text-green-600" :
            validationStatus === "INVALID" ? "bg-red-500/15 text-red-600" :
            validationStatus === "VALIDATING" ? "bg-blue-500/15 text-blue-600" :
            "bg-muted text-muted-foreground"
          }`}>
            {validationStatus === "VALID" && <CircleCheck className="h-3 w-3" />}
            {validationStatus === "INVALID" && <CircleX className="h-3 w-3" />}
            {validationStatus === "VALIDATING" && <Loader2 className="h-3 w-3 animate-spin" />}
            {validationStatus === "DRAFT" && <Pencil className="h-3 w-3" />}
            {validationStatus}
          </span>
        )}
        <div className="flex-1" />
        <Button variant="outline" size="sm" onClick={handleSave} disabled={saving || testing || running}>
          {saving ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Save className="h-3 w-3 mr-1" />}
          {saving ? "Saving..." : "Save"}
        </Button>
        {!isNew && (
          <>
            <Button variant="outline" size="sm" onClick={handleTest} disabled={saving || testing || running}>
              {testing ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <FlaskConical className="h-3 w-3 mr-1" />}
              {testing ? "Testing..." : "Test"}
            </Button>
            <Button size="sm" onClick={handleRun} disabled={saving || testing || running}>
              {running ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Play className="h-3 w-3 mr-1" />}
              {running ? "Running..." : "Run"}
            </Button>
          </>
        )}
      </div>

      {/* Error bar */}
      {error && (
        <div className="flex items-center gap-2 px-4 py-2 bg-destructive/10 text-destructive text-sm border-b">
          <span className="flex-1 overflow-auto max-h-12">{error}</span>
          <button onClick={() => setError(null)} className="text-xs hover:underline shrink-0">Dismiss</button>
        </div>
      )}

      {/* Graph */}
      <div className={panelOpen ? "h-[45%]" : "flex-1"}>
        <Canvas
          migration={migration}
          testResults={testResults?.steps || null}
          selectedNodeId={effectiveSelectedNodeId}
          panelOpen={panelOpen}
          onNodeClick={(nodeId) => {
            setSelectedNodeId(nodeId);
            lastNodeIdRef.current = nodeId;
            setPanelOpen(true);
            setBottomTab("editor");
          }}
          onCanvasClick={() => setSelectedNodeId(null)}
          onAddStep={handleAddStep}
          onDeleteStep={handleDeleteStep}
        />
      </div>

      {/* Bottom Panel with Tabs */}
      {panelOpen && (
        <div className="h-[55%] border-t flex flex-col overflow-hidden">
          {/* Tab bar */}
          <div className="flex items-center border-b px-2 shrink-0 cursor-pointer" onDoubleClick={() => setPanelOpen(false)}>
            <button
              className={`px-3 py-1.5 text-xs font-medium border-b-2 transition-colors ${
                bottomTab === "editor"
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              }`}
              onClick={() => setBottomTab("editor")}
            >
              Editor
            </button>
            <button
              className={`px-3 py-1.5 text-xs font-medium border-b-2 transition-colors ${
                bottomTab === "run"
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              }`}
              onClick={() => setBottomTab("run")}
            >
              Run
              {testing && <Loader2 className="h-3 w-3 ml-1 inline animate-spin" />}
            </button>
            <div className="flex-1" />
            <button
              onClick={() => setPanelOpen(false)}
              className="p-1 rounded hover:bg-muted text-muted-foreground hover:text-foreground transition-colors"
              title="Collapse console"
            >
              <PanelBottomClose className="h-4 w-4" />
            </button>
          </div>

          {/* Tab content */}
          <div className="flex-1 min-h-0">
            {bottomTab === "editor" && (
              <EditorPanel
                nodeId={safeEditingNodeId}
                migration={migration}
                connections={connections}
                step={editingStep}
                inputSchema={getInputSchema(safeEditingNodeId)}
                outputSchema={getOutputSchema(safeEditingNodeId)}
                onClose={() => setPanelOpen(false)}
                onUpdateMigration={handleUpdateMigration}
                onUpdateStep={handleUpdateStep}
              />
            )}
            {bottomTab === "run" && (
              <div className="h-full p-2">
                <RunLog entries={logEntries} running={testing} startedAt={testStartedAt} />
              </div>
            )}
          </div>
        </div>
      )}

      {/* Collapsed panel toggle */}
      {!panelOpen && (
        <div className="border-t px-2 py-1 flex items-center shrink-0 cursor-pointer" onDoubleClick={() => setPanelOpen(true)}>
          <span className="text-xs text-muted-foreground px-2">Console</span>
          <div className="flex-1" />
          <button
            onClick={() => setPanelOpen(true)}
            className="p-1 rounded hover:bg-muted text-muted-foreground hover:text-foreground transition-colors"
            title="Expand console"
          >
            <PanelBottomOpen className="h-4 w-4" />
          </button>
        </div>
      )}
    </div>
  );
}
