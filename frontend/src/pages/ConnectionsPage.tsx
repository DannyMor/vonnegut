// frontend/src/pages/ConnectionsPage.tsx
import { PageHeader } from "@/components/layout/PageHeader";
import { icons } from "@/config/iconRegistry";

export function ConnectionsPage() {
  return (
    <div className="flex flex-1 flex-col">
      <PageHeader icon={icons.nav_connections} title="Connections" description="Manage database connections" />
      <div className="p-6">Coming soon</div>
    </div>
  );
}
