// frontend/src/pages/MigrationsListPage.tsx
import { PageHeader } from "@/components/layout/PageHeader";
import { icons } from "@/config/iconRegistry";

export function MigrationsListPage() {
  return (
    <div className="flex flex-1 flex-col">
      <PageHeader icon={icons.nav_migrations} title="Migrations" description="Build and run migration pipelines" />
      <div className="p-6">Coming soon</div>
    </div>
  );
}
