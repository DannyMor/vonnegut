from __future__ import annotations
import builtins
import datetime
import hashlib
import json
import math
import re

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


class CodeExecutor(NodeExecutor):
    async def execute(
        self, context: ExecutionContext, inputs: dict[str, pa.Table]
    ) -> pa.Table:
        config = context.config
        assert isinstance(config, CodeNodeConfig)

        input_table = inputs.get("default")
        df = pl.from_arrow(input_table) if input_table is not None else pl.DataFrame()

        code = config.function_code
        compiled = compile(code, f"<{context.node_id}>", "exec")
        local_ns: dict = {}

        try:
            exec(compiled, {**_CODE_GLOBALS}, local_ns)
        except ImportError as e:
            if "__import__" in str(e):
                raise ValueError(
                    "Import statements are not allowed in code transforms. "
                    "Available modules: pl (polars), math, re, json, hashlib, datetime."
                ) from None
            raise

        transform_fn = local_ns.get("transform")
        if transform_fn is None:
            raise ValueError("Code must define a 'transform(df)' function")

        result_df = transform_fn(df)
        if not isinstance(result_df, pl.DataFrame):
            raise ValueError(f"transform() must return a polars DataFrame, got {type(result_df).__name__}")

        return result_df.to_arrow()
