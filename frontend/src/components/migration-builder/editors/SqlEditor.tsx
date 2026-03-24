import { CodeEditor } from "@/components/ui/code-editor";
import type { PipelineStep } from "@/types/pipeline";

interface Props {
  step: PipelineStep;
  onUpdate: (config: Record<string, unknown>) => void;
}

export function SqlEditor({ step, onUpdate }: Props) {
  const expression = (step.config.expression as string) || "";

  return (
    <div className="flex flex-col gap-3 h-full">
      <div className="flex flex-col flex-1 min-h-0">
        <label className="text-xs font-medium text-muted-foreground block mb-1 shrink-0">SQL Expression</label>
        <p className="text-xs text-muted-foreground mb-2 shrink-0">
          Write a SELECT that transforms the input. Use <code className="bg-muted px-1 rounded">{"{prev}"}</code> to reference the previous step.
        </p>
        <CodeEditor
          value={expression}
          onChange={(v) => onUpdate({ expression: v })}
          language="sql"
          placeholder="SELECT col1, lower(col2) as col2 FROM {prev}"
          flexGrow
        />
      </div>
    </div>
  );
}
