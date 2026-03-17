# backend/tests/test_api_ai.py
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from httpx import AsyncClient, ASGITransport

from vonnegut.main import create_app
from vonnegut.database import Database


@pytest_asyncio.fixture
async def app(tmp_path, encryption_key):
    db = Database(f"sqlite+aiosqlite:///{tmp_path}/test.db")
    await db.initialize()
    application = create_app(db=db, encryption_key=encryption_key)
    yield application
    await db.close()


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_suggest_transformation(client):
    mock_response = MagicMock()
    mock_response.content = [MagicMock()]
    mock_response.content[0].text = '{"expression": "LOWER(email)", "output_column": "email_lower", "explanation": "Converts email to lowercase"}'

    with patch("vonnegut.services.ai_assistant.anthropic") as mock_anthropic:
        mock_client = MagicMock()
        mock_client.messages.create = MagicMock(return_value=mock_response)
        mock_anthropic.Anthropic.return_value = mock_client

        resp = await client.post("/api/v1/ai/suggest-transformation", json={
            "prompt": "lowercase the email",
            "source_schema": [{"column": "email", "type": "text"}],
            "sample_data": [{"email": "Alice@Example.COM"}],
            "target_schema": None,
        })

    assert resp.status_code == 200
    data = resp.json()
    assert data["expression"] == "LOWER(email)"
    assert data["output_column"] == "email_lower"
    assert "explanation" in data
