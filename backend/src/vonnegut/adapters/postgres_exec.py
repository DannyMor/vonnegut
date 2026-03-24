import asyncio
import csv
import io
import json
from typing import Any
from urllib.parse import quote

from vonnegut.adapters.base import ColumnSchema, DatabaseAdapter
from vonnegut.adapters.pg_types import pg_type_category

_DEFAULT_TIMEOUT = 30


class PostgresExecAdapter(DatabaseAdapter):
    def __init__(
        self,
        namespace: str,
        pod_selector: str,
        pick_strategy: str,
        pick_filter: str | None,
        container: str | None,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        timeout: int = _DEFAULT_TIMEOUT,
    ):
        self._namespace = namespace
        self._pod_selector = pod_selector
        self._pick_strategy = pick_strategy
        self._pick_filter = pick_filter
        self._container = container
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._timeout = timeout
        self._resolved_pod: str | None = None

    @classmethod
    def from_config(cls, config: dict) -> "PostgresExecAdapter":
        return cls(
            namespace=config["namespace"],
            pod_selector=config["pod_selector"],
            pick_strategy=config.get("pick_strategy", "first_ready"),
            pick_filter=config.get("pick_filter"),
            container=config.get("container"),
            host=config["host"],
            port=config.get("port", 5432),
            database=config.get("database", ""),
            user=config["user"],
            password=config["password"],
        )

    async def connect(self) -> None:
        """Resolve a pod using label selectors."""
        try:
            process = await asyncio.create_subprocess_exec(
                "kubectl", "get", "pods",
                "-n", self._namespace,
                "-l", self._pod_selector,
                "-o", "json",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            raise ConnectionError("kubectl not found on PATH")

        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise ConnectionError(f"kubectl failed: {stderr.decode().strip()}")

        data = json.loads(stdout.decode())
        pods = data.get("items", [])

        # Filter to Running + Ready
        ready_pods = []
        for pod in pods:
            phase = pod.get("status", {}).get("phase")
            conditions = pod.get("status", {}).get("conditions", [])
            is_ready = any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in conditions
            )
            if phase == "Running" and is_ready:
                ready_pods.append(pod)

        # Apply pick strategy
        if self._pick_strategy == "name_contains" and self._pick_filter:
            ready_pods = [
                p for p in ready_pods
                if self._pick_filter in p["metadata"]["name"]
            ]

        if not ready_pods:
            raise ConnectionError(
                f"No ready pods matching selector '{self._pod_selector}' "
                f"in namespace '{self._namespace}'"
            )

        self._resolved_pod = ready_pods[0]["metadata"]["name"]

    async def disconnect(self) -> None:
        self._resolved_pod = None

    def _psql_uri(self, database: str | None = None) -> str:
        db = database or self._database
        pw = quote(self._password, safe="")
        return f"postgresql://{self._user}:{pw}@{self._host}:{self._port}/{db}?sslmode=require"

    @staticmethod
    def _validate_identifier(name: str) -> str:
        """Validate a SQL identifier (table name) to prevent injection."""
        if not all(c.isalnum() or c == "_" for c in name) or not name:
            raise ValueError(f"Invalid identifier: {name}")
        return name

    async def _run_psql(
        self, query: str, database: str | None = None, include_headers: bool = False,
    ) -> str:
        if self._resolved_pod is None:
            raise RuntimeError("Not connected — call connect() first")

        cmd = [
            "kubectl", "exec",
            "-n", self._namespace,
            self._resolved_pod,
        ]
        if self._container:
            cmd.extend(["-c", self._container])

        psql_flags = ["--csv", "-c", query]
        if not include_headers:
            psql_flags.insert(1, "-t")

        uri = self._psql_uri(database)
        cmd.extend(["--", "psql", uri] + psql_flags)

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=self._timeout,
            )
        except asyncio.TimeoutError:
            process.kill()
            raise RuntimeError(f"psql timed out after {self._timeout}s")

        if process.returncode != 0:
            raise RuntimeError(f"psql error: {stderr.decode().strip()}")

        return stdout.decode()

    def _parse_csv_rows(self, output: str) -> list[dict[str, Any]]:
        """Parse psql --csv output (with header row) into list of dicts."""
        text = output.strip()
        if not text:
            return []
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader]

    async def execute(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        if params:
            raise NotImplementedError(
                "PostgresExecAdapter does not support parameterized queries. "
                "Pre-format the query string instead."
            )
        output = await self._run_psql(query, include_headers=True)
        return self._parse_csv_rows(output)

    async def fetch_tables(self) -> list[str]:
        output = await self._run_psql(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        return [line.strip() for line in output.strip().splitlines() if line.strip()]

    async def fetch_schema(self, table: str) -> list[ColumnSchema]:
        safe_table = self._validate_identifier(table)
        query = f"""
            SELECT
                c.column_name,
                c.udt_name,
                c.is_nullable,
                c.column_default,
                CASE WHEN pk.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END as is_pk,
                fk.fk_ref,
                CASE WHEN uq.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END as is_unique
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT DISTINCT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = '{safe_table}'
            ) pk ON c.column_name = pk.column_name
            LEFT JOIN (
                SELECT DISTINCT ON (ku.column_name) ku.column_name,
                       ccu.table_name || '.' || ccu.column_name AS fk_ref
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' AND ku.table_name = '{safe_table}'
            ) fk ON c.column_name = fk.column_name
            LEFT JOIN (
                SELECT DISTINCT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'UNIQUE' AND tc.table_name = '{safe_table}'
            ) uq ON c.column_name = uq.column_name
            WHERE c.table_schema = 'public' AND c.table_name = '{safe_table}'
            ORDER BY c.ordinal_position
        """
        output = await self._run_psql(query, include_headers=True)
        rows = self._parse_csv_rows(output)
        return [
            ColumnSchema(
                name=r["column_name"],
                type=r["udt_name"],
                category=pg_type_category(r["udt_name"]),
                nullable=r["is_nullable"] == "YES",
                default=r["column_default"] if r["column_default"] else None,
                is_primary_key=r["is_pk"] == "YES",
                foreign_key=r["fk_ref"] if r.get("fk_ref") else None,
                is_unique=r["is_unique"] == "YES",
            )
            for r in rows
        ]

    async def fetch_sample(self, table: str, rows: int = 10) -> list[dict[str, Any]]:
        safe_table = self._validate_identifier(table)
        safe_rows = max(1, int(rows))
        output = await self._run_psql(
            f'SELECT * FROM "{safe_table}" LIMIT {safe_rows}', include_headers=True,
        )
        return self._parse_csv_rows(output)

    async def fetch_databases(self) -> list[str]:
        output = await self._run_psql(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname",
        )
        return [line.strip() for line in output.strip().splitlines() if line.strip()]
