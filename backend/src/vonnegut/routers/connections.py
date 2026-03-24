# backend/src/vonnegut/routers/connections.py
import json

from fastapi import APIRouter, HTTPException, Request, status

from vonnegut.models.connection import (
    ConnectionCreate,
    ConnectionResponse,
    ConnectionUpdate,
)

router = APIRouter(tags=["connections"])


def _get_manager(request: Request):
    return request.app.state.connection_manager


def _get_adapter_factory(request: Request):
    return request.app.state.adapter_factory


async def _create_adapter_from_config(config_dict: dict, request: Request):
    """Create an adapter from raw config dict (no saved connection needed)."""
    fake_conn = {"config": config_dict}
    return await _get_adapter_factory(request).create(fake_conn)


@router.post("/connections", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(body: ConnectionCreate, request: Request):
    manager = _get_manager(request)
    config_dict = body.config.model_dump()
    conn = await manager.create(name=body.name, config=config_dict)
    return ConnectionResponse(**conn)


@router.get("/connections", response_model=list[ConnectionResponse])
async def list_connections(request: Request):
    manager = _get_manager(request)
    rows = await manager.list_all()
    result = []
    for row in rows:
        config = row["config"] if isinstance(row["config"], dict) else json.loads(row["config"])
        result.append(ConnectionResponse(
            id=row["id"], name=row["name"],
            config=config, created_at=row["created_at"], updated_at=row["updated_at"],
        ))
    return result


@router.post("/connections/test-config")
async def test_config(body: ConnectionCreate, request: Request):
    """Test connectivity using raw config — no saved connection required."""
    try:
        adapter = await _create_adapter_from_config(body.config.model_dump(), request)
        await adapter.disconnect()
        return {"status": "ok", "message": "Connection successful"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/connections/discover-databases")
async def discover_databases(body: ConnectionCreate, request: Request):
    """Discover databases using raw config — no saved connection required."""
    try:
        adapter = await _create_adapter_from_config(body.config.model_dump(), request)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    try:
        databases = await adapter.fetch_databases()
        return databases
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        await adapter.disconnect()


@router.get("/connections/{conn_id}", response_model=ConnectionResponse)
async def get_connection(conn_id: str, request: Request):
    manager = _get_manager(request)
    conn = await manager.get(conn_id)
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    return ConnectionResponse(**conn)


@router.put("/connections/{conn_id}", response_model=ConnectionResponse)
async def update_connection(conn_id: str, body: ConnectionUpdate, request: Request):
    manager = _get_manager(request)
    config_dict = body.config.model_dump() if body.config is not None else None
    conn = await manager.update(conn_id, name=body.name, config=config_dict)
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    return ConnectionResponse(**conn)


@router.delete("/connections/{conn_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(conn_id: str, request: Request):
    manager = _get_manager(request)
    deleted = await manager.delete(conn_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Connection not found")


@router.post("/connections/{conn_id}/test")
async def test_connection(conn_id: str, request: Request):
    manager = _get_manager(request)
    conn = await manager.get(conn_id)
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        adapter = await _get_adapter_factory(request).create(conn)
        await adapter.disconnect()
        return {"status": "ok", "message": "Connection successful"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
