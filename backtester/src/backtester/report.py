"""Markdown report of a backtest (FR-BT2)."""

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
