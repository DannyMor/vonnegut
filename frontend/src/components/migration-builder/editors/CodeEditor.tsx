import { CodeEditor as CodeMirrorEditor } from "@/components/ui/code-editor";
import type { PipelineStep } from "@/types/pipeline";

interface Props {
  step: PipelineStep;
  onUpdate: (config: Record<string, unknown>) => void;
}

const DEFAULT_CODE = `def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df
`;

export function CodeEditor({ step, onUpdate }: Props) {
  const code = (step.config.function_code as string) || DEFAULT_CODE;

  return (
    <div className="flex flex-col gap-3 h-full">
      <div className="flex flex-col flex-1 min-h-0">
        <label className="text-xs font-medium text-muted-foreground block mb-1 shrink-0">Python Transform</label>
        <p className="text-xs text-muted-foreground mb-2 shrink-0">
          Define a <code className="bg-muted px-1 rounded">transform(df)</code> function that takes and returns a polars DataFrame.
          Available: <code className="bg-muted px-1 rounded">pl</code> (polars), math, re, json, hashlib, datetime.
        </p>
        <CodeMirrorEditor
          value={code}
          onChange={(v) => onUpdate({ function_code: v })}
          language="python"
          flexGrow
        />
      </div>
    </div>
  );
}
