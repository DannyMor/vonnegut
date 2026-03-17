import { useCallback, useMemo } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  type Node,
  type Edge,
  type NodeTypes,
  useNodesState,
  useEdgesState,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { SourceNode } from "./nodes/SourceNode";
import { TargetNode } from "./nodes/TargetNode";
import { TransformNode } from "./nodes/TransformNode";
import type { Migration } from "@/types/migration";

interface Props {
  migration: Migration;
  onNodeClick: (nodeId: string, nodeType: string) => void;
}

export function Canvas({ migration, onNodeClick }: Props) {
  const nodeTypes: NodeTypes = useMemo(
    () => ({
      source: SourceNode,
      target: TargetNode,
      transform: TransformNode,
    }),
    []
  );

  const initialNodes: Node[] = useMemo(() => {
    const nodes: Node[] = [
      {
        id: "source",
        type: "source",
        position: { x: 50, y: 200 },
        data: {
          label: "Source",
          table: migration.source_table || "Select table...",
        },
      },
      {
        id: "target",
        type: "target",
        position: { x: 700, y: 200 },
        data: {
          label: "Target",
          table: migration.target_table || "Select table...",
        },
      },
    ];

    migration.transformations.forEach((t, i) => {
      nodes.push({
        id: t.id,
        type: "transform",
        position: { x: 250 + i * 200, y: 200 },
        data: {
          label: t.type === "column_mapping" ? "Mapping" : t.type === "ai_generated" ? "AI Transform" : "Expression",
          transformType: t.type,
          config: t.config,
        },
      });
    });

    return nodes;
  }, [migration]);

  const initialEdges: Edge[] = useMemo(() => {
    const edges: Edge[] = [];
    const transformIds = migration.transformations.map((t) => t.id);

    if (transformIds.length === 0) {
      edges.push({ id: "source-target", source: "source", target: "target" });
    } else {
      edges.push({ id: `source-${transformIds[0]}`, source: "source", target: transformIds[0] });
      for (let i = 0; i < transformIds.length - 1; i++) {
        edges.push({ id: `${transformIds[i]}-${transformIds[i + 1]}`, source: transformIds[i], target: transformIds[i + 1] });
      }
      edges.push({ id: `${transformIds[transformIds.length - 1]}-target`, source: transformIds[transformIds.length - 1], target: "target" });
    }

    return edges;
  }, [migration]);

  const [nodes, , onNodesChange] = useNodesState(initialNodes);
  const [edges, , onEdgesChange] = useEdgesState(initialEdges);

  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      onNodeClick(node.id, node.type || "");
    },
    [onNodeClick]
  );

  return (
    <div className="h-full w-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={handleNodeClick}
        fitView
      >
        <Background />
        <Controls />
      </ReactFlow>
    </div>
  );
}
