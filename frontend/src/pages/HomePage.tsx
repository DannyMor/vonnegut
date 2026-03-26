import { useEffect, useState } from "react";
import { useNavigate } from "react-router";
import { icons } from "@/config/iconRegistry";
import { api } from "@/lib/api";
import type { LucideIcon } from "lucide-react";

interface CardInfo {
  label: string;
  description: string;
  icon: LucideIcon;
  to: string;
  count: number | null;
  countLabel: string;
}

export function HomePage() {
  const navigate = useNavigate();
  const [connectionCount, setConnectionCount] = useState<number | null>(null);
  const [pipelineCount, setPipelineCount] = useState<number | null>(null);

  useEffect(() => {
    api.connections.list().then((list) => setConnectionCount(list.length)).catch(() => {});
    api.pipelines.list().then((list) => setPipelineCount(list.length)).catch(() => {});
  }, []);

  const cards: CardInfo[] = [
    {
      label: "Connections",
      description: "Manage database connections",
      icon: icons.nav_connections,
      to: "/connections",
      count: connectionCount,
      countLabel: "connections",
    },
    {
      label: "Explorer",
      description: "Browse tables and schemas",
      icon: icons.nav_explorer,
      to: "/explorer",
      count: null,
      countLabel: "",
    },
    {
      label: "Pipelines",
      description: "Build and run data pipelines",
      icon: icons.nav_pipelines,
      to: "/pipelines",
      count: pipelineCount,
      countLabel: "pipelines",
    },
  ];

  return (
    <div className="flex flex-1 items-center justify-center">
      <div className="flex gap-6">
        {cards.map((card) => {
          const Icon = card.icon;
          return (
            <button
              key={card.to}
              onClick={() => navigate(card.to)}
              className="flex w-52 cursor-pointer flex-col items-center gap-3 rounded-xl border bg-card p-6 text-card-foreground shadow-sm transition-all hover:shadow-md hover:border-primary/40 hover:bg-muted/30"
            >
              <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-primary/10">
                <Icon className="h-6 w-6 text-primary" />
              </div>
              <div className="text-center">
                <div className="font-semibold">{card.label}</div>
                <div className="text-xs text-muted-foreground mt-1">{card.description}</div>
              </div>
              {card.count !== null && (
                <div className="text-sm text-muted-foreground">
                  <span className="font-medium text-foreground">{card.count}</span> {card.countLabel}
                </div>
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
}
