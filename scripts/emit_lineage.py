"""make lineage: send the streaming path's lineage and the last dbt build's to Marquez (OBS-2).

Dagster sends dbt and asset lineage on every run; this covers what runs outside Dagster:
Debezium, the Flink jobs, the feature sink and the decision service (described from the
platform's configuration), and a dbt build run by hand (make dbt-build).
"""

import sys

from cdc_reconcile import load_env
from orchestration import lineage


def main() -> int:
    database = load_env().get("POSTGRES_DB", "payments")
    sent = 0
    for job, inputs, outputs in lineage.streaming_jobs(database):
        ok = lineage.emit_run(job, inputs, outputs)
        sent += ok
        print(f"  {'OK  ' if ok else 'FAIL'} {job}: {len(inputs)} in, {len(outputs)} out")
    target = lineage.ROOT / "dbt/target"
    if (target / "run_results.json").exists():
        ok = lineage.send_dbt(target)
        print(f"  {'OK  ' if ok else 'FAIL'} dbt: the last build in dbt/target")
    print(f"lineage sent to {lineage.URL}: {sent} streaming jobs; open Marquez to browse it")
    return 0 if sent == len(lineage.streaming_jobs(database)) else 1


if __name__ == "__main__":
    sys.exit(main())
