import { useState } from "react";
import { Sparkles, Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api";
import type { PipelineStep, ColumnDef } from "@/types/pipeline";

interface Props {
  step: PipelineStep;
  inputSchema: ColumnDef[];
  onUpdate: (updates: Record<string, unknown>) => void;
}

export function AiEditor({ step, inputSchema, onUpdate }: Props) {
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const prompt = (step.config.prompt as string) || "";
  const generatedCode = (step.config.generated_code as string) || "";
  const generatedType = (step.config.generated_type as string) || null;
  const approved = (step.config.approved as boolean) || false;

  const handleGenerate = async () => {
    setGenerating(true);
    setError(null);
    try {
      const result = await api.ai.suggest({
        prompt,
        source_schema: inputSchema.map((c) => ({ column: c.name, type: c.type })),
        sample_data: [],
        target_schema: null,
      });
      onUpdate({
        config: {
          ...step.config,
          generated_type: "sql",
          generated_code: result.expression,
          approved: false,
        },
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Generation failed");
    } finally {
      setGenerating(false);
    }
  };

  const handleApprove = () => {
    onUpdate({
      step_type: generatedType === "code" ? "code" : "sql",
      config: generatedType === "code"
        ? { function_code: generatedCode }
        : { expression: generatedCode },
    });
  };

  return (
    <div className="flex flex-col gap-4">
      <div>
        <label className="text-xs font-medium text-muted-foreground block mb-1">Prompt</label>
        <textarea
          className="w-full border rounded px-3 py-2 text-sm min-h-[80px] resize-y"
          value={prompt}
          onChange={(e) => onUpdate({ config: { ...step.config, prompt: e.target.value } })}
          placeholder="Describe the transformation you want, e.g. 'Hash the email column with SHA256'"
        />
        <Button
          size="sm"
          className="mt-2"
          onClick={handleGenerate}
          disabled={!prompt || generating}
        >
          <Sparkles className="h-3 w-3 mr-1" />
          {generating ? "Generating..." : "Generate"}
        </Button>
        {error && (
          <div className="mt-2 text-xs text-destructive">{error}</div>
        )}
      </div>

      {generatedCode && (
        <div>
          <label className="text-xs font-medium text-muted-foreground block mb-1">
            Generated {generatedType === "code" ? "Python" : "SQL"}
          </label>
          <textarea
            className="w-full border rounded px-3 py-2 text-sm font-mono min-h-[150px] resize-y"
            value={generatedCode}
            onChange={(e) => onUpdate({
              config: { ...step.config, generated_code: e.target.value },
            })}
          />
          {!approved && (
            <Button size="sm" variant="outline" className="mt-2" onClick={handleApprove}>
              <Check className="h-3 w-3 mr-1" /> Approve & Convert
            </Button>
          )}
        </div>
      )}
    </div>
  );
}
