// frontend/src/pages/ExplorerPage.tsx
import { PageHeader } from "@/components/layout/PageHeader";
import { icons } from "@/config/iconRegistry";

export function ExplorerPage() {
  return (
    <div className="flex flex-1 flex-col">
      <PageHeader icon={icons.nav_explorer} title="Explorer" description="Browse schemas and preview data" />
      <div className="p-6">Coming soon</div>
    </div>
  );
}
