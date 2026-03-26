import { BaseEdge, EdgeLabelRenderer, getBezierPath, type EdgeProps, type Edge } from "@xyflow/react";
import { Plus } from "lucide-react";
import { useState, useRef, useEffect } from "react";
import type { StepType } from "@/types/pipeline";

interface AddStepEdgeData {
  onAddStep: (type: StepType, afterNodeId: string) => void;
  sourceNodeId: string;
  [key: string]: unknown;
}

type AddStepEdgeType = Edge<AddStepEdgeData>;

export function AddStepEdge({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data }: EdgeProps<AddStepEdgeType>) {
  const d = data!;
  const [showDropdown, setShowDropdown] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const [edgePath, labelX, labelY] = getBezierPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition });

  useEffect(() => {
    if (!showDropdown) return;
    const handleClick = (e: PointerEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("pointerdown", handleClick, true);
    return () => document.removeEventListener("pointerdown", handleClick, true);
  }, [showDropdown]);

  const handleAdd = (e: React.MouseEvent, type: StepType) => {
    e.stopPropagation();
    d.onAddStep(type, d.sourceNodeId);
    setShowDropdown(false);
  };

  return (
    <>
      <BaseEdge id={id} path={edgePath} />
      <EdgeLabelRenderer>
        <div
          ref={dropdownRef}
          className="absolute flex items-center justify-center"
          style={{ transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`, pointerEvents: "all" }}
        >
          <button
            onClick={(e) => { e.stopPropagation(); setShowDropdown(!showDropdown); }}
            className="h-6 w-6 rounded-full border bg-background flex items-center justify-center hover:bg-muted transition-colors"
          >
            <Plus className="h-3 w-3" />
          </button>
          {showDropdown && (
            <div className="absolute bottom-8 z-[100] rounded-md border bg-popover text-popover-foreground shadow-md py-1 min-w-[140px]">
              <button className="w-full text-left px-3 py-1.5 text-sm hover:bg-muted" onClick={(e) => handleAdd(e, "sql")}>SQL Transform</button>
              <button className="w-full text-left px-3 py-1.5 text-sm hover:bg-muted" onClick={(e) => handleAdd(e, "code")}>Code Transform</button>
              <button className="w-full text-left px-3 py-1.5 text-sm hover:bg-muted" onClick={(e) => handleAdd(e, "ai")}>AI Assistant</button>
            </div>
          )}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}
