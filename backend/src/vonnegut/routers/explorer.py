# backend/src/vonnegut/routers/explorer.py
from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(tags=["explorer"])


def _get_manager(request: Request):
    return request.app.state.connection_manager


def _get_adapter_factory(request: Request):
    return request.app.state.adapter_factory


@router.get("/connections/{conn_id}/databases")
async def list_databases(conn_id: str, request: Request):
    manager = _get_manager(request)
    conn = await manager.get(conn_id)
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    adapter = await _get_adapter_factory(request).create(conn)
    try:
        return await adapter.fetch_databases()
    finally:
        await adapter.disconnect()


@router.get("/connections/{conn_id}/tables")
async def list_tables(conn_id: str, request: Request):
    manager = _get_manager(request)
    conn = await manager.get(conn_id)
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    adapter = await _get_adapter_factory(request).create(conn)
    try:
        return await adapter.fetch_tables()
    finally:
        await adapter.disconnect()


@router.get("/connections/{conn_id}/tables/{table}/schema")
async def get_table_schema(conn_id: str, table: str, request: Request):
    manager = _get_manager(request)
    conn = await manager.get(conn_id)
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    adapter = await _get_adapter_factory(request).create(conn)
    try:
        return await adapter.fetch_schema(table)
    finally:
        await adapter.disconnect()


@router.get("/connections/{conn_id}/tables/{table}/sample")
async def get_table_sample(conn_id: str, table: str, request: Request, rows: int = Query(default=10)):
    manager = _get_manager(request)
    conn = await manager.get(conn_id)
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    adapter = await _get_adapter_factory(request).create(conn)
    try:
        return await adapter.fetch_sample(table, rows=rows)
    finally:
        await adapter.disconnect()
