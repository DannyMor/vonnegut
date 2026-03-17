import { BrowserRouter, Routes, Route, Navigate } from "react-router";
import { Sidebar } from "@/components/layout/Sidebar";
import { ConnectionsPage } from "@/pages/ConnectionsPage";
import { ExplorerPage } from "@/pages/ExplorerPage";
import { MigrationsListPage } from "@/pages/MigrationsListPage";
import { MigrationBuilderPage } from "@/pages/MigrationBuilderPage";

export default function App() {
  return (
    <BrowserRouter>
      <div className="flex h-screen">
        <Sidebar />
        <main className="flex flex-1 flex-col overflow-auto">
          <Routes>
            <Route path="/" element={<Navigate to="/connections" replace />} />
            <Route path="/connections" element={<ConnectionsPage />} />
            <Route path="/explorer" element={<ExplorerPage />} />
            <Route path="/migrations" element={<MigrationsListPage />} />
            <Route path="/migrations/:id" element={<MigrationBuilderPage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
