"""Markdown reports of backtests (FR-BT2, FR-BT3)."""

from typing import Any

from backtester.metrics import Result


def pct(value: float | None) -> str:
    return "-" if value is None else f"{value:.2%}"


def markdown(result: Result, header: dict[str, str]) -> str:
    lines = [f"# Backtest: rules v{result.rules_version}", ""]
    lines += [f"- **{k}:** {v}" for k, v in header.items()]
    lines += [
        f"- **transactions:** {result.rows:,}, of which **{result.positives:,}** labelled fraud",
        "",
        "| rule | action | alerts | precision | recall | alert rate "
        "| $ fraud declined | $ legit declined |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for s in result.table():
        lines.append(
            f"| {s.rule_id} | {s.action} | {s.alerts:,} | {pct(result.precision(s))} | "
            f"{pct(result.recall(s))} | {pct(result.alert_rate(s))} | "
            f"{s.fraud_declined_usd:,.2f} | {s.legit_declined_usd:,.2f} |"
        )
    lines += [
        "",
        "Labels are the training set's: matured, as known at its cutoff (FR-PIT2). An alert is",
        "a review or a decline; recall is alerted fraud among all fraud. Same rules file and",
        "same snapshot give the same numbers.",
    ]
    return "\n".join(lines) + "\n"


def comparison(results: list[Result], header: dict[str, str]) -> str:
    """Side by side (FR-BT3): overall per version with the change from the first, then each
    rule across versions ("-" where a version does not have the rule)."""
    names = [f"v{r.rules_version}" for r in results]
    base = results[0]
    lines = [f"# Backtest comparison: {' vs '.join(names)}", ""]
    lines += [f"- **{k}:** {v}" for k, v in header.items()]
    lines += [
        f"- **transactions:** {base.rows:,}, of which **{base.positives:,}** labelled fraud",
        "",
    ]
    lines += [
        "| overall | " + " | ".join(names) + " | change |",
        "| --- |" + " ---: |" * (len(names) + 1),
    ]

    def metric(label: str, get: Any, fmt: Any) -> None:
        values = [get(r) for r in results]
        first, last = values[0], values[-1]
        change = "-" if first is None or last is None else fmt(last - first, signed=True)
        lines.append(f"| {label} | " + " | ".join(fmt(v) for v in values) + f" | {change} |")

    def p(v: Any, signed: bool = False) -> str:
        return "-" if v is None else (f"{v * 100:+.2f} pp" if signed else f"{v:.2%}")

    def n(v: Any, signed: bool = False) -> str:
        return "-" if v is None else (f"{v:+,}" if signed else f"{v:,}")

    def usd(v: Any, signed: bool = False) -> str:
        return "-" if v is None else (f"{v:+,.2f}" if signed else f"{v:,.2f}")

    metric("alerts", lambda r: r.overall.alerts, n)
    metric("precision", lambda r: r.precision(r.overall), p)
    metric("recall", lambda r: r.recall(r.overall), p)
    metric("alert rate", lambda r: r.alert_rate(r.overall), p)
    metric("$ fraud declined", lambda r: r.overall.fraud_declined_usd, usd)
    metric("$ legit declined", lambda r: r.overall.legit_declined_usd, usd)

    rules = list(dict.fromkeys(rid for r in results for rid in r.per_rule))
    lines += ["", "| rule (alerts / precision) | " + " | ".join(names) + " |",
              "| --- |" + " ---: |" * len(names)]  # fmt: skip
    for rid in rules:
        cells = []
        for r in results:
            s = r.per_rule.get(rid)
            cells.append("-" if s is None else f"{s.alerts:,} / {pct(r.precision(s))}")
        lines.append(f"| {rid} | " + " | ".join(cells) + " |")
    lines += ["", "Change: last version minus the first. Same training set for every version."]
    return "\n".join(lines) + "\n"
