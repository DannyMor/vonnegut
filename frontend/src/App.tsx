import { BrowserRouter, Routes, Route } from "react-router";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { Sidebar } from "@/components/layout/Sidebar";
import { HomePage } from "@/pages/HomePage";
import { ConnectionsPage } from "@/pages/ConnectionsPage";
import { ExplorerPage } from "@/pages/ExplorerPage";
import { MigrationsListPage } from "@/pages/MigrationsListPage";
import { MigrationBuilderPage } from "@/pages/MigrationBuilderPage";

export default function App() {
  return (
    <BrowserRouter>
      <ErrorBoundary>
        <div className="flex h-screen">
          <Sidebar />
          <main className="flex flex-1 flex-col overflow-auto">
            <Routes>
              <Route path="/" element={<HomePage />} />
              <Route path="/connections" element={<ConnectionsPage />} />
              <Route path="/explorer" element={<ExplorerPage />} />
              <Route path="/migrations" element={<MigrationsListPage />} />
              <Route path="/migrations/:id" element={<MigrationBuilderPage />} />
            </Routes>
          </main>
        </div>
      </ErrorBoundary>
    </BrowserRouter>
  );
}
