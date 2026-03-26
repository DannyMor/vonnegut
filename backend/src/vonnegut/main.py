# backend/src/vonnegut/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from vonnegut.config import Settings
from vonnegut.database import AppDatabase, SqliteDatabase
from vonnegut.encryption import get_or_create_key
from vonnegut.adapters.factory import DefaultAdapterFactory
from vonnegut.repositories import (
    ConnectionRepository,
    PipelineRepository,
    PipelineMetadataRepository,
    PipelineStepRepository,
    TransformationRepository,
)
from vonnegut.routers.ai import router as ai_router
from vonnegut.routers.connections import router as connections_router
from vonnegut.routers.explorer import router as explorer_router
from vonnegut.routers.pipelines import router as pipelines_router
from vonnegut.routers.pipeline_steps import router as pipeline_steps_router
from vonnegut.routers.transformations import router as transformations_router
from vonnegut.services.connection_manager import ConnectionManager


def _init_repositories(app: FastAPI, db: AppDatabase, encryption_key: str) -> None:
    """Initialize all repositories and services on app.state."""
    app.state.db = db
    conn_repo = ConnectionRepository(db)
    app.state.pipeline_repo = PipelineRepository(db)
    app.state.pipeline_metadata_repo = PipelineMetadataRepository(db)
    app.state.pipeline_step_repo = PipelineStepRepository(db)
    app.state.transformation_repo = TransformationRepository(db)
    app.state.connection_manager = ConnectionManager(
        repo=conn_repo, encryption_key=encryption_key,
    )


def create_app(
    db: AppDatabase | None = None,
    encryption_key: str | None = None,
    settings: Settings | None = None,
    adapter_factory=None,
) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.db is None:
            db_instance = SqliteDatabase(settings.database_url)
            await db_instance.initialize()
            _init_repositories(app, db_instance, app.state.encryption_key)
        yield
        if app.state.db is not None:
            await app.state.db.close()

    app = FastAPI(title="Vonnegut", version="0.1.0", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.db = None
    app.state.encryption_key = encryption_key or get_or_create_key()
    app.state.settings = settings
    app.state.adapter_factory = adapter_factory or DefaultAdapterFactory()
    app.state.pipeline_repo = None
    app.state.pipeline_metadata_repo = None
    app.state.pipeline_step_repo = None
    app.state.transformation_repo = None
    app.state.connection_manager = None

    # Pre-initialize if db was injected (e.g. tests)
    if db is not None:
        _init_repositories(app, db, app.state.encryption_key)

    @app.get("/api/health")
    def health():
        return {"status": "ok"}

    app.include_router(ai_router, prefix="/api/v1")
    app.include_router(connections_router, prefix="/api/v1")
    app.include_router(explorer_router, prefix="/api/v1")
    app.include_router(pipelines_router, prefix="/api/v1")
    app.include_router(pipeline_steps_router, prefix="/api/v1")
    app.include_router(transformations_router, prefix="/api/v1")

    return app


# uvicorn entry point: `uvicorn vonnegut.main:app --factory`
app = create_app
