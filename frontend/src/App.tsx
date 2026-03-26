import { BrowserRouter, Routes, Route } from "react-router";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { Sidebar } from "@/components/layout/Sidebar";
import { HomePage } from "@/pages/HomePage";
import { ConnectionsPage } from "@/pages/ConnectionsPage";
import { ExplorerPage } from "@/pages/ExplorerPage";
import { PipelinesListPage } from "@/pages/PipelinesListPage";
import { PipelineBuilderPage } from "@/pages/PipelineBuilderPage";

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
              <Route path="/pipelines" element={<PipelinesListPage />} />
              <Route path="/pipelines/:id" element={<PipelineBuilderPage />} />
            </Routes>
          </main>
        </div>
      </ErrorBoundary>
    </BrowserRouter>
  );
}
