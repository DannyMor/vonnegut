# backend/src/vonnegut/services/ai_assistant.py
import json

import anthropic


class AIAssistant:
    def __init__(self, api_key: str | None = None):
        self._client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()

    def suggest_transformation(
        self,
        prompt: str,
        source_schema: list[dict],
        sample_data: list[dict],
        target_schema: list[dict] | None = None,
    ) -> dict:
        system_prompt = """You are a SQL transformation assistant. Given a source table schema, sample data, and a user request, suggest a SQL expression transformation.

Respond with ONLY a JSON object (no markdown, no explanation outside the JSON):
{
    "expression": "<SQL expression using supported functions: UPPER, LOWER, CONCAT, COALESCE, TRIM, LENGTH>",
    "output_column": "<suggested output column name>",
    "explanation": "<brief human-readable explanation>"
}"""

        user_message = f"""Source schema: {json.dumps(source_schema)}
Sample data: {json.dumps(sample_data[:10])}
Target schema: {json.dumps(target_schema) if target_schema else "Not specified"}

Request: {prompt}"""

        response = self._client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )

        result_text = response.content[0].text
        return json.loads(result_text)
