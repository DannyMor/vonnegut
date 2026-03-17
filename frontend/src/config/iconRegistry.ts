// frontend/src/config/iconRegistry.ts
import {
  Database,
  DatabaseZap,
  ArrowRightLeft,
  Code,
  Sparkles,
  CircleCheck,
  CircleX,
  Plug,
  Search,
  Workflow,
  type LucideIcon,
} from "lucide-react";

export const icons: Record<string, LucideIcon> = {
  source: Database,
  target: DatabaseZap,
  column_mapping: ArrowRightLeft,
  sql_expression: Code,
  ai_generated: Sparkles,
  connection_ok: CircleCheck,
  connection_error: CircleX,
  nav_connections: Plug,
  nav_explorer: Search,
  nav_migrations: Workflow,
};
