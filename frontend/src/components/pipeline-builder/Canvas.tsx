import { useCallback, useMemo, useState, useEffect, useRef } from "react";
import {
  ReactFlow, Background, useReactFlow, ReactFlowProvider,
  useNodesInitialized, applyNodeChanges,
  type Node, type Edge, type NodeTypes, type EdgeTypes, type NodeChange,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { AlignHorizontalSpaceAround, Magnet } from "lucide-react";
import { SourceNode } from "./nodes/SourceNode";
import { TargetNode } from "./nodes/TargetNode";
import { PipelineNode } from "./nodes/PipelineNode";
import { AddStepEdge } from "./edges/AddStepEdge";
import type { Pipeline } from "@/types/pipeline-definition";
import type { StepType, StepResult } from "@/types/pipeline";

interface Props {
  pipeline: Pipeline;
  testResults: StepResult[] | null;
  selectedNodeId: string | null;
  panelOpen: boolean;
  onNodeClick: (nodeId: string) => void;
  onCanvasClick: () => void;
  onAddStep: (type: StepType, afterNodeId: string) => void;
  onDeleteStep: (stepId: string) => void;
}

const NODE_SPACING = 350;
const CENTER_Y = 150;
const FIT_OPTIONS = { padding: 0.5, maxZoom: 0.85, duration: 200 };

function buildNodeData(pipeline: Pipeline, testResults: StepResult[] | null, selectedNodeId: string | null, onDeleteStep: (id: string) => void): Node[] {
  const steps = pipeline.pipeline_steps || [];
  const result: Node[] = [];

  result.push({
    id: "source",
    type: "source",
    position: { x: 0, y: CENTER_Y },
    selected: selectedNodeId === "source",
    data: {
      connectionName: "",
      table: pipeline.source_table || "Not configured",
      schemaCount: pipeline.source_schema?.length || null,
      label: pipeline.source_label,
    },
  });

  steps.forEach((step, i) => {
    const stepResult = testResults?.find((r) => r.node_id === step.id);
    result.push({
      id: step.id,
      type: "pipeline",
      position: { x: (i + 1) * NODE_SPACING, y: CENTER_Y },
      selected: selectedNodeId === step.id,
      data: {
        name: step.name,
        stepType: step.step_type,
        description: step.description,
        schemaCount: stepResult?.schema?.length ?? null,
        validationStatus: stepResult?.status ?? null,
        onDelete: onDeleteStep,
      },
    });
  });

  const targetResult = testResults?.find((r) => r.node_id === "target");
  const targetValidation: "valid" | "invalid" | "unknown" = targetResult
    ? (targetResult.status === "ok" && targetResult.validation.valid ? "valid" : "invalid")
    : "unknown";

  result.push({
    id: "target",
    type: "target",
    position: { x: (steps.length + 1) * NODE_SPACING, y: CENTER_Y },
    selected: selectedNodeId === "target",
    data: {
      connectionName: "",
      table: pipeline.target_table || "Not configured",
      schemaCount: null,
      validationStatus: targetValidation,
      label: pipeline.target_label,
    },
  });

  return result;
}

/** Adjust Y positions so all node centers sit on the same horizontal line. */
function centerAlignNodes(nodes: Node[]): Node[] {
  const maxHeight = Math.max(...nodes.map(n => n.measured?.height ?? 0));
  if (maxHeight === 0) return nodes;
  return nodes.map(n => {
    const h = n.measured?.height ?? 0;
    const offset = (maxHeight - h) / 2;
    return { ...n, position: { ...n.position, y: CENTER_Y + offset } };
  });
}

function CanvasInner({
  pipeline, testResults, selectedNodeId, panelOpen,
  onNodeClick, onCanvasClick, onAddStep, onDeleteStep,
}: Props) {
  const nodeTypes: NodeTypes = useMemo(() => ({
    source: SourceNode,
    target: TargetNode,
    pipeline: PipelineNode,
  }), []);

  const edgeTypes: EdgeTypes = useMemo(() => ({
    addStep: AddStepEdge,
  }), []);

  const steps = pipeline.pipeline_steps || [];
  const { fitView, getNodes } = useReactFlow();
  const nodesInitialized = useNodesInitialized();

  const nodeIdKey = ["source", ...steps.map(s => s.id), "target"].join(",");
  const prevNodeIdKeyRef = useRef(nodeIdKey);
  const hasCenterAlignedRef = useRef(false);
  const [autoFit, setAutoFit] = useState(true);

  const [nodes, setNodes] = useState<Node[]>(() =>
    buildNodeData(pipeline, testResults, selectedNodeId, onDeleteStep)
  );

  const doFitView = useCallback(() => {
    setTimeout(() => fitView(FIT_OPTIONS), 50);
  }, [fitView]);

  // After nodes are measured, center-align them vertically
  useEffect(() => {
    if (nodesInitialized && !hasCenterAlignedRef.current) {
      hasCenterAlignedRef.current = true;
      const measured = getNodes();
      if (measured.some(n => n.measured?.height)) {
        setNodes(prev => {
          const withMeasures = prev.map(n => {
            const m = measured.find(mn => mn.id === n.id);
            return m?.measured ? { ...n, measured: m.measured } : n;
          });
          return centerAlignNodes(withMeasures);
        });
        doFitView();
      }
    }
  }, [nodesInitialized, getNodes, doFitView]);

  // When structure changes (nodes added/removed), rebuild layout
  useEffect(() => {
    if (nodeIdKey !== prevNodeIdKeyRef.current) {
      prevNodeIdKeyRef.current = nodeIdKey;
      hasCenterAlignedRef.current = false;
      setNodes(buildNodeData(pipeline, testResults, selectedNodeId, onDeleteStep));
    }
  }, [nodeIdKey, pipeline, testResults, selectedNodeId, onDeleteStep]);

  // When data changes (but not structure), update data + selected without resetting positions
  useEffect(() => {
    const freshNodes = buildNodeData(pipeline, testResults, selectedNodeId, onDeleteStep);
    setNodes(prev => prev.map(n => {
      const fresh = freshNodes.find(f => f.id === n.id);
      if (!fresh) return n;
      return { ...n, data: fresh.data, selected: fresh.selected };
    }));
  }, [pipeline, testResults, selectedNodeId, onDeleteStep]);

  // Re-fit when the bottom panel opens or closes (canvas size changes)
  const prevPanelOpenRef = useRef(panelOpen);
  useEffect(() => {
    if (prevPanelOpenRef.current !== panelOpen) {
      prevPanelOpenRef.current = panelOpen;
      if (autoFit) {
        doFitView();
      }
    }
  }, [panelOpen, autoFit, doFitView]);

  const onNodesChange = useCallback((changes: NodeChange[]) => {
    setNodes(prev => applyNodeChanges(changes, prev));
  }, []);

  const handleAutoAlign = useCallback(() => {
    const measured = getNodes();
    const fresh = buildNodeData(pipeline, testResults, selectedNodeId, onDeleteStep).map(n => {
      const m = measured.find(mn => mn.id === n.id);
      return m?.measured ? { ...n, measured: m.measured } : n;
    });
    setNodes(centerAlignNodes(fresh));
    doFitView();
  }, [pipeline, testResults, selectedNodeId, onDeleteStep, doFitView, getNodes]);

  const edges: Edge[] = useMemo(() => {
    const result: Edge[] = [];
    const nodeIds = ["source", ...steps.map(s => s.id), "target"];

    for (let i = 0; i < nodeIds.length - 1; i++) {
      result.push({
        id: `${nodeIds[i]}-${nodeIds[i + 1]}`,
        source: nodeIds[i],
        target: nodeIds[i + 1],
        type: "addStep",
        data: { onAddStep, sourceNodeId: nodeIds[i] },
      });
    }

    return result;
  }, [steps, onAddStep]);

  const handleNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    onNodeClick(node.id);
  }, [onNodeClick]);

  return (
    <div className="h-full w-full relative">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodesChange={onNodesChange}
        onNodeClick={handleNodeClick}
        onPaneClick={onCanvasClick}
        fitView
        fitViewOptions={FIT_OPTIONS}
      >
        <Background />
      </ReactFlow>
      <div className="absolute top-2 right-2 z-10 flex items-center gap-1">
        <button
          onClick={() => setAutoFit(f => !f)}
          title={autoFit ? "Auto-fit enabled (click to disable)" : "Auto-fit disabled (click to enable)"}
          className={`p-1.5 rounded border transition-colors ${
            autoFit
              ? "bg-primary/10 border-primary/30 text-primary hover:bg-primary/20"
              : "bg-background border-border text-muted-foreground hover:bg-muted hover:text-foreground"
          }`}
        >
          <Magnet className="h-4 w-4" />
        </button>
        <button
          onClick={handleAutoAlign}
          title="Auto-align and fit"
          className="p-1.5 rounded border bg-background hover:bg-muted text-muted-foreground hover:text-foreground transition-colors"
        >
          <AlignHorizontalSpaceAround className="h-4 w-4" />
        </button>
      </div>
    </div>
  );
}

export function Canvas(props: Props) {
  return (
    <ReactFlowProvider>
      <CanvasInner {...props} />
    </ReactFlowProvider>
  );
}
