from __future__ import annotations

import ast
import asyncio
import builtins
import datetime
import functools
import hashlib
import json
import math
import re
from concurrent.futures import ThreadPoolExecutor

import polars as pl
import pyarrow as pa

from vonnegut.pipeline.dag.node import CodeNodeConfig
from vonnegut.pipeline.dag.plan import ExecutionContext
from vonnegut.pipeline.engine.executor.base import NodeExecutor

_SAFE_BUILTIN_NAMES = [
    "abs", "all", "any", "bool", "bytes", "chr", "dict", "divmod",
    "enumerate", "filter", "float", "format", "frozenset", "hasattr",
    "hash", "int", "isinstance", "issubclass", "iter", "len", "list",
    "map", "max", "min", "next", "ord", "pow", "print", "range",
    "repr", "reversed", "round", "set", "slice", "sorted", "str",
    "sum", "tuple", "type", "zip",
    "True", "False", "None",
    "ValueError", "TypeError", "KeyError", "IndexError", "RuntimeError",
    "Exception", "StopIteration", "AttributeError", "ZeroDivisionError",
]
_SAFE_BUILTINS = {name: getattr(builtins, name) for name in _SAFE_BUILTIN_NAMES if hasattr(builtins, name)}

_CODE_GLOBALS = {
    "pl": pl, "polars": pl,
    "math": math, "re": re, "json": json,
    "hashlib": hashlib, "datetime": datetime,
    "__builtins__": _SAFE_BUILTINS,
}

# Dunder attributes that enable sandbox escapes
_BLOCKED_DUNDERS = frozenset({
    "__import__", "__loader__", "__spec__",
    "__subclasses__", "__bases__", "__mro__",
    "__globals__", "__code__", "__closure__",
    "__builtins__", "__class__",
})

# Names that should never appear in user code
_BLOCKED_NAMES = frozenset({
    "exec", "eval", "compile", "open",
    "breakpoint", "exit", "quit",
    "getattr", "setattr", "delattr",
    "__import__", "__builtins__",
})

DEFAULT_TIMEOUT_SECONDS = 30

_executor_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="code-sandbox")


def check_code_safety(code: str) -> list[str]:
    """Scan code AST for dangerous patterns. Returns list of violation messages."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []  # syntax errors are caught later at compile time

    violations: list[str] = []

    for node in ast.walk(tree):
        # Block import statements
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            violations.append("Import statements are not allowed.")

        # Block dangerous attribute access (e.g. obj.__globals__)
        elif isinstance(node, ast.Attribute):
            if node.attr in _BLOCKED_DUNDERS:
                violations.append(
                    f"Access to '{node.attr}' is not allowed."
                )

        # Block dangerous function calls by name
        elif isinstance(node, ast.Name):
            if node.id in _BLOCKED_NAMES:
                violations.append(
                    f"Use of '{node.id}' is not allowed."
                )

        # Block string access to dunder attrs via getitem (e.g. x["__globals__"])
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value in _BLOCKED_DUNDERS:
                violations.append(
                    f"Reference to '{node.value}' is not allowed."
                )

    return violations


class CodeExecutor(NodeExecutor):
    def __init__(self, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS):
        self.timeout_seconds = timeout_seconds

    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table:
        config = context.config
        assert isinstance(config, CodeNodeConfig)

        input_table = inputs.get("default")
        df = pl.from_arrow(input_table) if input_table is not None else pl.DataFrame()

        code = config.function_code

        # AST safety check
        violations = check_code_safety(code)
        if violations:
            raise ValueError(
                "Code contains blocked patterns:\n" + "\n".join(f"  - {v}" for v in violations)
            )

        compiled = compile(code, f"<{context.node_id}>", "exec")
        local_ns: dict = {}

        exec(compiled, {**_CODE_GLOBALS}, local_ns)

        transform_fn = local_ns.get("transform")
        if transform_fn is None:
            raise ValueError("Code must define a 'transform(df)' function")

        # Run transform with timeout in a thread pool
        loop = asyncio.get_running_loop()
        try:
            result_df = await asyncio.wait_for(
                loop.run_in_executor(
                    _executor_pool,
                    functools.partial(transform_fn, df),
                ),
                timeout=self.timeout_seconds,
            )
        except asyncio.TimeoutError:
            raise ValueError(
                f"Code transform timed out after {self.timeout_seconds} seconds."
            ) from None

        if not isinstance(result_df, pl.DataFrame):
            raise ValueError(f"transform() must return a polars DataFrame, got {type(result_df).__name__}")

        return result_df.to_arrow()
