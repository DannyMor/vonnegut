# backend/src/vonnegut/services/transformation_engine.py
import re


class TransformationEngine:
    """Applies transformations to rows in Python.

    Supports a safe subset of SQL-like functions:
    UPPER, LOWER, CONCAT, COALESCE, TRIM, LENGTH.
    """

    _FUNCTIONS = {
        "UPPER": lambda args: str(args[0]).upper() if args[0] is not None else None,
        "LOWER": lambda args: str(args[0]).lower() if args[0] is not None else None,
        "TRIM": lambda args: str(args[0]).strip() if args[0] is not None else None,
        "LENGTH": lambda args: len(str(args[0])) if args[0] is not None else None,
        "CONCAT": lambda args: "".join(str(a) if a is not None else "" for a in args),
        "COALESCE": lambda args: next((a for a in args if a is not None), None),
    }

    def apply_column_mapping(self, rows: list[dict], config: dict) -> list[dict]:
        mappings = config["mappings"]
        result = []
        for row in rows:
            new_row = {}
            for m in mappings:
                if m["drop"]:
                    continue
                source = m["source_col"]
                target = m["target_col"]
                if source in row:
                    new_row[target] = row[source]
            result.append(new_row)
        return result

    def apply_sql_expression(self, rows: list[dict], config: dict) -> list[dict]:
        expression = config["expression"]
        output_col = config["output_column"]
        result = []
        for row in rows:
            value = self._evaluate_expression(expression, row)
            new_row = dict(row)
            new_row[output_col] = value
            result.append(new_row)
        return result

    def apply_pipeline(self, rows: list[dict], transformations: list[dict]) -> list[dict]:
        for t in transformations:
            t_type = t["type"]
            config = t["config"]
            if t_type == "column_mapping":
                rows = self.apply_column_mapping(rows, config)
            elif t_type in ("sql_expression", "ai_generated"):
                rows = self.apply_sql_expression(rows, config)
        return rows

    def _evaluate_expression(self, expr: str, row: dict):
        """Evaluate a simple SQL-like expression against a row."""
        match = re.match(r"(\w+)\((.+)\)$", expr.strip())
        if not match:
            return row.get(expr.strip())

        func_name = match.group(1).upper()
        args_str = match.group(2)

        if func_name not in self._FUNCTIONS:
            raise ValueError(f"Unsupported function: {func_name}")

        args = self._parse_args(args_str, row)
        return self._FUNCTIONS[func_name](args)

    def _parse_args(self, args_str: str, row: dict) -> list:
        """Parse function arguments — column references or string literals."""
        args = []
        for arg in self._split_args(args_str):
            arg = arg.strip()
            if (arg.startswith("'") and arg.endswith("'")) or (arg.startswith('"') and arg.endswith('"')):
                args.append(arg[1:-1])
            elif arg in row:
                args.append(row[arg])
            else:
                args.append(arg)
        return args

    def _split_args(self, args_str: str) -> list[str]:
        """Split arguments respecting quoted strings."""
        args = []
        current = ""
        in_quotes = False
        quote_char = None
        for ch in args_str:
            if ch in ("'", '"') and not in_quotes:
                in_quotes = True
                quote_char = ch
                current += ch
            elif ch == quote_char and in_quotes:
                in_quotes = False
                current += ch
                quote_char = None
            elif ch == "," and not in_quotes:
                args.append(current)
                current = ""
            else:
                current += ch
        if current:
            args.append(current)
        return args
