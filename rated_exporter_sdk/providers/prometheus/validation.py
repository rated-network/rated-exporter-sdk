import re
from typing import ClassVar, List

from rated_exporter_sdk.providers.prometheus.errors import PrometheusQueryError


class QueryValidator:
    METRIC_NAME_PATTERN: ClassVar[re.Pattern] = re.compile(
        r"^[a-zA-Z_:][a-zA-Z0-9_:]*$"
    )

    BINARY_OPERATORS: ClassVar[dict] = {
        "arithmetic": ["+", "-", "*", "/", "%", "^"],
        "comparison": ["==", "!=", ">", "<", ">=", "<="],
        "logical": ["and", "or", "unless"],
        "vector_matching": ["on", "ignoring", "group_left", "group_right"],
    }

    AGGREGATION_OPERATORS: ClassVar[dict] = {
        "sum": {"allow_by": True},
        "min": {"allow_by": True},
        "max": {"allow_by": True},
        "avg": {"allow_by": True},
        "count": {"allow_by": True},
        "group": {"allow_by": True},
        "stddev": {"allow_by": True},
        "stdvar": {"allow_by": True},
        "topk": {"numeric_first": True, "allow_by": True},
        "bottomk": {"numeric_first": True, "allow_by": True},
        "quantile": {"numeric_first": True, "allow_by": True},
        "count_values": {"string_first": True, "allow_by": True},
    }

    RANGE_FUNCTIONS: ClassVar[dict] = {
        "rate": {"needs_range": True},
        "irate": {"needs_range": True},
        "increase": {"needs_range": True},
        "resets": {"needs_range": True},
        "changes": {"needs_range": True},
        "deriv": {"needs_range": True},
        "predict_linear": {"needs_range": True, "numeric_second": True},
        "delta": {"needs_range": True},
        "idelta": {"needs_range": True},
        "avg_over_time": {"needs_range": True},
        "min_over_time": {"needs_range": True},
        "max_over_time": {"needs_range": True},
        "sum_over_time": {"needs_range": True},
        "count_over_time": {"needs_range": True},
        "quantile_over_time": {"numeric_first": True, "needs_range": True},
        "stddev_over_time": {"needs_range": True},
        "stdvar_over_time": {"needs_range": True},
        "last_over_time": {"needs_range": True},
        "present_over_time": {"needs_range": True},
        "absent_over_time": {"needs_range": True},
    }

    SPECIAL_FUNCTIONS: ClassVar[dict] = {
        "vector": {},
        "histogram_quantile": {"numeric_first": True},
        "label_replace": {"string_args": [1, 2, 3, 4]},
        "label_join": {"string_args": [1, 2]},
        "round": {"numeric_second": True},
        "scalar": {},
        "clamp_max": {"numeric_second": True},
        "clamp_min": {"numeric_second": True},
        "sort": {},
        "sort_desc": {},
        "timestamp": {},
        "absent": {},
        "floor": {},
        "ceil": {},
        "exp": {},
        "ln": {},
        "log2": {},
        "log10": {},
        "sqrt": {},
        "abs": {},
        "day_of_month": {},
        "day_of_week": {},
        "days_in_month": {},
        "hour": {},
        "minute": {},
        "month": {},
        "year": {},
    }

    ALL_FUNCTIONS: ClassVar[dict] = {
        **AGGREGATION_OPERATORS,
        **RANGE_FUNCTIONS,
        **SPECIAL_FUNCTIONS,
    }

    def validate_query(self, query: str) -> None:
        if not query or not isinstance(query, str):
            raise PrometheusQueryError("Query must be a non-empty string", query=query)

        try:
            metric_names = self.extract_metric_names(query)
            for metric_name in metric_names:
                self.validate_metric_name(metric_name)

            tokens = self.tokenize_query(query)

            if not all(
                [
                    self.check_balanced(query, "(", ")"),
                    self.check_balanced(query, "{", "}"),
                    self.check_balanced(query, "[", "]"),
                ]
            ):
                raise PrometheusQueryError(
                    "Unbalanced brackets or parentheses", query=query
                )

            subquery_pattern = re.compile(r"\[.+:\s*\]")
            for token in tokens:
                if subquery_pattern.search(token):
                    duration_parts = token[1:-1].split(":")
                    if len(duration_parts) != 2:
                        raise PrometheusQueryError(
                            f"Invalid subquery format in: {token}", query=query
                        )
                    for duration in duration_parts:
                        if not re.match(r"^\d+[smhdwy]$", duration.strip()):
                            raise PrometheusQueryError(
                                f"Invalid duration format in subquery: {duration}",
                                query=query,
                            )

            if query.count('"') % 2 != 0:
                raise PrometheusQueryError("Unmatched quotes", query=query)

            # Validate that the query does not end with a binary operator
            if any(query.strip().endswith(op) for op in ["or", "and", "unless"]):
                raise PrometheusQueryError(
                    "Query cannot end with a binary operator", query=query
                )

            # Check for invalid 'offset' usage
            offset_pattern = re.compile(r"offset\s+\d+[smhdwy]")
            for m in re.finditer(r"offset", query):
                offset_str = query[m.start() :]
                if not offset_pattern.match(offset_str):
                    raise PrometheusQueryError(
                        "Invalid offset duration format", query=query
                    )

        except PrometheusQueryError:
            raise

        except Exception as e:
            raise PrometheusQueryError(f"Invalid query: {e!s}", query=query)

    def tokenize_query(self, query: str) -> List[str]:
        tokens = []
        current_token = ""
        in_string = False
        paren_count = 0
        i = 0
        while i < len(query):
            char = query[i]
            if char == '"':
                in_string = not in_string
                current_token += char
                i += 1
            elif in_string:
                current_token += char
                i += 1
            elif char == "(":
                paren_count += 1
                if paren_count == 1 and current_token:
                    tokens.append(current_token)
                    current_token = ""
                current_token += char
                i += 1
            elif char == ")":
                paren_count -= 1
                current_token += char
                if paren_count == 0:
                    tokens.append(current_token)
                    current_token = ""
                i += 1
            elif char.isspace() and paren_count == 0:
                if current_token:
                    tokens.append(current_token)
                    current_token = ""
                i += 1
            elif not in_string and query.startswith("offset", i):
                if current_token:
                    tokens.append(current_token)
                    current_token = ""
                tokens.append("offset")
                i += len("offset")
            else:
                current_token += char
                i += 1
        if current_token:
            tokens.append(current_token)
        return tokens

    def validate_function_call(self, func_name: str, args: List[str]) -> None:
        if func_name not in self.ALL_FUNCTIONS:
            raise PrometheusQueryError(
                f"Unknown function: {func_name}", query=func_name
            )

        func_spec = self.ALL_FUNCTIONS[func_name]

        if func_spec.get("needs_range"):  # type: ignore
            if not any("[" in arg and "]" in arg for arg in args):
                raise PrometheusQueryError(
                    f"Function {func_name} requires a range vector selector",
                    query=func_name,
                )

        if func_spec.get("numeric_first") and args:  # type: ignore
            if not args[0].replace(".", "").isdigit():
                raise PrometheusQueryError(
                    f"Function {func_name} requires numeric first argument",
                    query=func_name,
                )

        if func_spec.get("string_args"):  # type: ignore
            for arg_index in func_spec["string_args"]:  # type: ignore
                if arg_index < len(args):
                    arg = args[arg_index]
                    if not (arg.startswith('"') and arg.endswith('"')):
                        raise PrometheusQueryError(
                            f"Function {func_name} requires string argument at position {arg_index + 1}",
                            query=func_name,
                        )

    def validate_metric_selector(self, selector: str) -> None:
        selector = selector.strip()
        if "{" in selector:
            if not selector.endswith("}"):
                raise PrometheusQueryError(
                    "Invalid metric selector: missing closing '}'", query=selector
                )
            metric_name_part, labels_part = selector.split("{", 1)
            metric_name_part = metric_name_part.strip()
            labels_part = "{" + labels_part

            if metric_name_part and not self.METRIC_NAME_PATTERN.match(
                metric_name_part
            ):
                raise PrometheusQueryError(
                    f"Invalid metric name: {metric_name_part}", query=selector
                )

            if selector.endswith(",}"):
                raise PrometheusQueryError(
                    "Invalid label matcher: trailing comma found", query=selector
                )

            label_matchers = labels_part[1:-1].split(",")
            for _matcher in label_matchers:
                matcher = _matcher.strip()
                if not re.match(
                    r'^[a-zA-Z_][a-zA-Z0-9_]*\s*(=~|!~|!=|=)\s*".+"$', matcher
                ):
                    raise PrometheusQueryError(
                        f"Invalid label matcher: {matcher}", query=selector
                    )

    def extract_metric_names(self, expr: str) -> List[str]:
        expr = expr.strip()
        metrics = []

        # Handle aggregation functions
        agg_func_match = re.match(
            r"^(?P<func>\w+)\s+(?:by|without)\s*\((?P<labels>[^\)]*)\)\s*\((?P<inner_expr>.*)\)$",
            expr,
        )
        if agg_func_match:
            func_name = agg_func_match.group("func")
            inner_expr = agg_func_match.group("inner_expr")
            if func_name in self.AGGREGATION_OPERATORS:
                self.validate_function_call(func_name, [inner_expr])
                metrics.extend(self.extract_metric_names(inner_expr))
                return metrics

        # Existing function parsing logic
        if "(" in expr and ")" in expr:
            func_name = expr[: expr.find("(")].strip()
            args_str = expr[expr.find("(") + 1 : expr.rfind(")")].strip()
            args = self.parse_function_args(args_str)
            if func_name in self.ALL_FUNCTIONS:
                self.validate_function_call(func_name, args)
                for arg in args:
                    if arg.replace(".", "").isdigit() or arg.startswith('"'):
                        continue
                    metrics.extend(self.extract_metric_names(arg))
                return metrics

        # Handle binary operators
        for op_type in self.BINARY_OPERATORS.values():
            for op in op_type:
                if f" {op} " in expr:
                    parts = expr.split(f" {op} ")
                    for part in parts:
                        metrics.extend(self.extract_metric_names(part))
                    return metrics

        # Handle range vector selectors
        if "[" in expr and "]" in expr:
            metric_selector = expr.split("[", 1)[0].strip()
            self.validate_metric_selector(metric_selector)
            metric_name = metric_selector.split("{", 1)[0].strip()
            if metric_name:
                metrics.append(metric_name)
            return metrics

        # Handle simple metric names or vector selectors
        metric_name = expr.strip()
        self.validate_metric_selector(metric_name)
        if "{" in metric_name:
            metric_name = metric_name.split("{", 1)[0].strip()
        if metric_name:
            metrics.append(metric_name)
        return metrics

    def validate_metric_name(self, metric_name: str) -> None:
        if not metric_name:
            return  # Allow empty metric name
        if "{" in metric_name:
            metric_name = metric_name.split("{", 1)[0].strip()
        if not self.METRIC_NAME_PATTERN.match(metric_name):
            raise PrometheusQueryError(
                f"Invalid metric name: {metric_name}", query=metric_name
            )

    def parse_function_args(self, args_str: str) -> List[str]:
        args = []
        current_arg = ""
        paren_count = 0
        in_quotes = False

        for char in args_str:
            if char == '"':
                in_quotes = not in_quotes
                current_arg += char
            elif not in_quotes:
                if char == "(":
                    paren_count += 1
                    current_arg += char
                elif char == ")":
                    paren_count -= 1
                    current_arg += char
                elif char == "," and paren_count == 0:
                    args.append(current_arg.strip())
                    current_arg = ""
                else:
                    current_arg += char
            else:
                current_arg += char

        if current_arg:
            args.append(current_arg.strip())

        return args

    def check_balanced(self, expr: str, open_char: str, close_char: str) -> bool:
        count = 0
        in_string = False
        for char in expr:
            if char == '"':
                in_string = not in_string
            elif not in_string:
                if char == open_char:
                    count += 1
                elif char == close_char:
                    count -= 1
                if count < 0:
                    return False
        return count == 0
