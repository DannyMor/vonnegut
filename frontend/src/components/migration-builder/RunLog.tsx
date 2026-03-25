import { useEffect, useMemo, useRef, useState } from "react";
import {
  Loader2, CheckCircle2, XCircle, Clock, Play, AlertTriangle, Timer,
} from "lucide-react";

export interface LogEntry {
  type: "step_start" | "step_complete" | "step_error" | "error" | "info" | "done" | "result";
  node_id?: string;
  name?: string;
  status?: string;
  duration_ms?: number;
  row_count?: number;
  col_count?: number;
  error?: string;
  timestamp?: string;
  message?: string;
  validation_errors?: Array<{ type: string; column?: string; message: string }>;
}

interface Props {
  entries: LogEntry[];
  running: boolean;
  startedAt: number | null;
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  return `${Math.floor(ms / 60000)}m ${((ms % 60000) / 1000).toFixed(1)}s`;
}

function formatTime(iso?: string): string {
  if (!iso) return "";
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString(undefined, { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return "";
  }
}

function ElapsedTimer({ startedAt }: { startedAt: number }) {
  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    const interval = setInterval(() => setElapsed(Date.now() - startedAt), 100);
    return () => clearInterval(interval);
  }, [startedAt]);
  return (
    <span className="text-xs text-muted-foreground flex items-center gap-1 ml-auto">
      <Timer className="h-3 w-3" /> {formatDuration(elapsed)}
    </span>
  );
}

/** Renders text with `backtick` segments as highlighted code spans. */
function FormattedMessage({ text }: { text: string }) {
  const parts = text.split(/(`[^`]+`)/g);
  return (
    <>
      {parts.map((part, i) =>
        part.startsWith("`") && part.endsWith("`") ? (
          <span key={i} className="text-blue-300 font-semibold bg-blue-500/10 px-1 rounded">
            {part.slice(1, -1)}
          </span>
        ) : (
          <span key={i}>{part}</span>
        )
      )}
    </>
  );
}

function EntryLine({ entry }: { entry: LogEntry }) {
  const time = formatTime(entry.timestamp);

  if (entry.type === "step_start") {
    return (
      <div className="flex items-center gap-2 py-1 text-sm text-blue-400">
        <Loader2 className="h-3.5 w-3.5 animate-spin shrink-0" />
        <span className="text-muted-foreground font-mono text-xs w-16 shrink-0">{time}</span>
        <span>Executing <span className="font-medium text-blue-300">{entry.name}</span>...</span>
      </div>
    );
  }

  if (entry.type === "step_complete") {
    return (
      <div className="flex items-center gap-2 py-1 text-sm text-emerald-400">
        <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
        <span className="text-muted-foreground font-mono text-xs w-16 shrink-0">{time}</span>
        <span>
          <span className="font-medium text-emerald-300">{entry.name}</span>
          {" "}completed
        </span>
        {entry.duration_ms !== undefined && (
          <span className="text-xs text-muted-foreground flex items-center gap-1">
            <Clock className="h-3 w-3" /> {formatDuration(entry.duration_ms)}
          </span>
        )}
        {entry.row_count !== undefined && (
          <span className="text-xs text-muted-foreground">
            {entry.row_count} rows, {entry.col_count} cols
          </span>
        )}
      </div>
    );
  }

  if (entry.type === "step_error") {
    return (
      <div className="flex flex-col gap-0.5 py-1">
        <div className="flex items-center gap-2 text-sm text-red-400">
          <XCircle className="h-3.5 w-3.5 shrink-0" />
          <span className="text-muted-foreground font-mono text-xs w-16 shrink-0">{time}</span>
          <span>
            <span className="font-medium text-red-300">{entry.name}</span>
            {" "}failed
          </span>
          {entry.duration_ms !== undefined && (
            <span className="text-xs text-muted-foreground flex items-center gap-1">
              <Clock className="h-3 w-3" /> {formatDuration(entry.duration_ms)}
            </span>
          )}
        </div>
        {entry.validation_errors && entry.validation_errors.length > 0 && (
          <div className="ml-8 text-xs text-red-400/80 bg-red-500/10 rounded px-2 py-1.5 space-y-0.5 max-h-32 overflow-auto">
            {entry.validation_errors.map((ve, i) => (
              <div key={i} className="flex items-start gap-1.5">
                <span className="text-red-500/60 shrink-0">•</span>
                <span>
                  {ve.column && <span className="font-mono text-red-300">{ve.column}</span>}
                  {ve.column && " — "}
                  {ve.message}
                </span>
              </div>
            ))}
          </div>
        )}
        {entry.error && !entry.validation_errors?.length && (
          <div className="ml-8 text-xs text-red-400/80 font-mono bg-red-500/10 rounded px-2 py-1 max-h-20 overflow-auto">
            {entry.error}
          </div>
        )}
      </div>
    );
  }

  if (entry.type === "error") {
    return (
      <div className="flex flex-col gap-0.5 py-1">
        <div className="flex items-center gap-2 text-sm text-red-400">
          <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
          <span className="text-muted-foreground font-mono text-xs w-16 shrink-0">{time}</span>
          <span>{entry.error || entry.message}</span>
        </div>
      </div>
    );
  }

  if (entry.type === "info") {
    return (
      <div className="flex items-center gap-2 py-1 text-sm text-muted-foreground">
        <Play className="h-3.5 w-3.5 shrink-0" />
        <span className="font-mono text-xs w-16 shrink-0">{time}</span>
        <span><FormattedMessage text={entry.message || ""} /></span>
      </div>
    );
  }

  return null;
}

export function RunLog({ entries, running, startedAt }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null);

  // Filter out step_start entries once resolved by a completion, step error, or global error/done
  const visibleEntries = useMemo(() => {
    const resolved = new Set<string>();
    // A global error or done resolves ALL pending step_starts
    const hasGlobalEnd = entries.some((e) => e.type === "error" || e.type === "done");
    for (const e of entries) {
      if ((e.type === "step_complete" || e.type === "step_error") && e.node_id) {
        resolved.add(e.node_id);
      }
    }
    return entries.filter((e) => {
      if (e.type !== "step_start") return true;
      if (!e.node_id) return true;
      if (resolved.has(e.node_id)) return false;
      if (hasGlobalEnd && !running) return false;
      return true;
    });
  }, [entries, running]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [visibleEntries.length]);

  if (entries.length === 0 && !running) {
    return (
      <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
        Run a test to see execution results here.
      </div>
    );
  }

  return (
    <div className="h-full overflow-auto p-3 bg-[#1e1e2e] rounded font-mono text-xs leading-relaxed">
      {running && startedAt && (
        <div className="flex items-center gap-2 py-1 text-sm text-muted-foreground border-b border-white/5 mb-1 pb-1">
          <ElapsedTimer startedAt={startedAt} />
        </div>
      )}
      {visibleEntries.map((entry, i) => (
        <EntryLine key={i} entry={entry} />
      ))}
      <div ref={bottomRef} />
    </div>
  );
}
