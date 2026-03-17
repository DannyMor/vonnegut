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


@router.post("/connections", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(body: ConnectionCreate, request: Request):
    manager = _get_manager(request)
    conn = await manager.create(name=body.name, type=body.type, config=body.config)
    return ConnectionResponse(**conn)


@router.get("/connections", response_model=list[ConnectionResponse])
async def list_connections(request: Request):
    manager = _get_manager(request)
    rows = await manager.list_all()
    result = []
    for row in rows:
        config = row["config"] if isinstance(row["config"], dict) else json.loads(row["config"])
        result.append(ConnectionResponse(
            id=row["id"], name=row["name"], type=row["type"],
            config=config, created_at=row["created_at"], updated_at=row["updated_at"],
        ))
    return result


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
    conn = await manager.update(conn_id, name=body.name, config=body.config)
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
