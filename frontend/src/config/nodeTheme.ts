// frontend/src/config/nodeTheme.ts
export const nodeTheme = {
  source: {
    color: "bg-blue-50 border-blue-400",
    accent: "text-blue-600",
    badge: "bg-blue-100 text-blue-700",
  },
  target: {
    color: "bg-green-50 border-green-400",
    accent: "text-green-600",
    badge: "bg-green-100 text-green-700",
  },
  column_mapping: {
    color: "bg-amber-50 border-amber-400",
    accent: "text-amber-600",
    badge: "bg-amber-100 text-amber-700",
  },
  sql_expression: {
    color: "bg-purple-50 border-purple-400",
    accent: "text-purple-600",
    badge: "bg-purple-100 text-purple-700",
  },
  ai_generated: {
    color: "bg-teal-50 border-teal-400",
    accent: "text-teal-600",
    badge: "bg-teal-100 text-teal-700",
  },
} as const;

export type NodeType = keyof typeof nodeTheme;
