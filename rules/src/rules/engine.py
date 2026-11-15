"""Rule files and a small, safe condition language.

A condition is an expression over named values (features and transaction fields):

    txn_count_1h >= 5 and distinct_merchants_1h >= 4
    km_from_last_txn / (minutes_since_last_txn / 60) > 900

It is parsed with Python's `ast` and checked against a whitelist: comparisons,
and/or/not, + - * /, numbers, strings, booleans and known names. Anything else
(calls, attributes, subscripts, lambdas, comprehensions) is rejected when the file is
loaded. Conditions are compiled into closures; nothing is ever passed to eval().

Nulls: arithmetic with a null gives null, and a comparison involving a null is false.
A rule never fires because a feature is missing.
"""

import ast
import operator
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

ACTIONS = {"approve": 0, "review": 1, "decline": 2}  # decision severity order
RULE_ID = re.compile(r"^[a-z][a-z0-9_]*$")

Value = object
Fn = Callable[[Mapping[str, Value]], Value]

_COMPARE: dict[type[ast.cmpop], Callable[[Any, Any], bool]] = {
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
}
_ARITH: dict[type[ast.operator], Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}


class RuleError(ValueError):
    """A rule file or condition is invalid."""


def _number(value: Value) -> Value:
    return float(value) if isinstance(value, Decimal) else value


def compile_condition(text: str, names: frozenset[str]) -> Fn:
    try:
        tree = ast.parse(text, mode="eval")
    except SyntaxError as exc:
        raise RuleError(f"cannot parse {text!r}: {exc.msg}") from exc
    return _compile(tree.body, text, names)


def _compile(node: ast.AST, text: str, names: frozenset[str]) -> Fn:
    if isinstance(node, ast.BoolOp):
        parts = [_compile(v, text, names) for v in node.values]
        if isinstance(node.op, ast.And):
            return lambda ctx: all(p(ctx) is True for p in parts)
        return lambda ctx: any(p(ctx) is True for p in parts)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        inner = _compile(node.operand, text, names)
        return lambda ctx: inner(ctx) is not True
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        inner = _compile(node.operand, text, names)

        def negate(ctx: Mapping[str, Value]) -> Value:
            value = inner(ctx)
            return None if value is None else -value  # type: ignore[operator]

        return negate
    if isinstance(node, ast.Compare):
        left = _compile(node.left, text, names)
        ops = [_COMPARE.get(type(op)) for op in node.ops]
        if any(op is None for op in ops):
            raise RuleError(f"{text!r}: only > >= < <= == != comparisons are allowed")
        rights = [_compile(c, text, names) for c in node.comparators]

        def compare(ctx: Mapping[str, Value]) -> bool:
            a = left(ctx)
            for op, right in zip(ops, rights, strict=True):
                b = right(ctx)
                if a is None or b is None or not op(a, b):  # type: ignore[misc]
                    return False
                a = b
            return True

        return compare
    if isinstance(node, ast.BinOp):
        fn = _ARITH.get(type(node.op))
        if fn is None:
            raise RuleError(f"{text!r}: only + - * / are allowed")
        left, right = _compile(node.left, text, names), _compile(node.right, text, names)

        def arith(ctx: Mapping[str, Value]) -> Value:
            a, b = left(ctx), right(ctx)
            if a is None or b is None:
                return None
            try:
                return fn(a, b)
            except ZeroDivisionError:
                return None

        return arith
    if isinstance(node, ast.Name):
        if node.id not in names:
            raise RuleError(f"{text!r}: unknown name {node.id!r}")
        name = node.id
        return lambda ctx: _number(ctx.get(name))
    if isinstance(node, ast.Constant) and isinstance(node.value, int | float | str | bool):
        constant = node.value
        return lambda ctx: constant
    raise RuleError(f"{text!r}: {type(node).__name__} is not allowed in a condition")


@dataclass(frozen=True)
class Rule:
    id: str
    description: str
    condition: str
    action: str
    severity: int
    test: Fn


@dataclass(frozen=True)
class Evaluation:
    outcome: str  # approve | review | decline
    score: int  # highest severity among fired rules, 0 when none fired
    fired: tuple[str, ...]


@dataclass(frozen=True)
class RuleSet:
    version: int
    description: str
    rules: tuple[Rule, ...]

    @classmethod
    def load(cls, path: Path, names: frozenset[str]) -> "RuleSet":
        doc = yaml.safe_load(path.read_text())
        return cls.parse(doc, names, source=path.name)

    @classmethod
    def parse(cls, doc: dict[str, Any], names: frozenset[str], source: str = "") -> "RuleSet":
        if set(doc) != {"version", "description", "rules"}:
            raise RuleError(f"{source}: expected keys version, description, rules")
        version = doc["version"]
        if not isinstance(version, int) or version < 1:
            raise RuleError(f"{source}: version must be a positive integer")
        if source and source != f"rules_v{version}.yml":
            raise RuleError(f"{source}: file name must match version {version}")
        rules = []
        for raw in doc["rules"] or []:
            if set(raw) != {"id", "description", "condition", "action", "severity"}:
                raise RuleError(
                    f"{raw.get('id')}: needs id, description, condition, action, severity"
                )
            rid = raw["id"]
            if not isinstance(rid, str) or not RULE_ID.match(rid):
                raise RuleError(f"{rid!r}: rule ids are lower snake_case")
            if raw["action"] not in ("review", "decline"):
                raise RuleError(f"{rid}: action must be review or decline")
            severity = raw["severity"]
            if not isinstance(severity, int) or not 1 <= severity <= 100:
                raise RuleError(f"{rid}: severity must be an integer from 1 to 100")
            condition = str(raw["condition"])
            rules.append(
                Rule(
                    rid,
                    str(raw["description"]),
                    condition,
                    raw["action"],
                    severity,
                    compile_condition(condition, names),
                )
            )
        ids = [r.id for r in rules]
        if dupes := sorted({i for i in ids if ids.count(i) > 1}):
            raise RuleError(f"{source}: duplicate rule ids {dupes}")
        if not rules:
            raise RuleError(f"{source}: no rules")
        return cls(version, str(doc["description"]), tuple(rules))

    def evaluate(self, context: Mapping[str, Value]) -> Evaluation:
        fired = [r for r in self.rules if r.test(context) is True]
        if not fired:
            return Evaluation("approve", 0, ())
        outcome = max((r.action for r in fired), key=ACTIONS.__getitem__)
        return Evaluation(outcome, max(r.severity for r in fired), tuple(r.id for r in fired))
