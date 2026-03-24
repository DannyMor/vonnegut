# backend/src/vonnegut/routers/explorer.py
from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(tags=["explorer"])


def _get_manager(request: Request):
    return request.app.state.connection_manager


def _get_adapter_factory(request: Request):
    return request.app.state.adapter_factory


async def _with_adapter(conn_id: str, request: Request, fn):
    """Helper: resolve connection, create adapter, run fn, handle errors."""
    manager = _get_manager(request)
    conn = await manager.get(conn_id)
    if conn is None:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        adapter = await _get_adapter_factory(request).create(conn)
    except ConnectionError as e:
        raise HTTPException(status_code=502, detail=str(e))
    try:
        return await fn(adapter)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        await adapter.disconnect()


@router.get("/connections/{conn_id}/databases")
async def list_databases(conn_id: str, request: Request):
    return await _with_adapter(conn_id, request, lambda a: a.fetch_databases())


@router.get("/connections/{conn_id}/tables")
async def list_tables(conn_id: str, request: Request):
    return await _with_adapter(conn_id, request, lambda a: a.fetch_tables())


@router.get("/connections/{conn_id}/tables/{table}/schema")
async def get_table_schema(conn_id: str, table: str, request: Request):
    return await _with_adapter(conn_id, request, lambda a: a.fetch_schema(table))


@router.get("/connections/{conn_id}/tables/{table}/sample")
async def get_table_sample(conn_id: str, table: str, request: Request, rows: int = Query(default=10, ge=1, le=1000)):
    return await _with_adapter(conn_id, request, lambda a: a.fetch_sample(table, rows=rows))
