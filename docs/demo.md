# Presenting the platform

`make demo` checks the stack is healthy, starts the demo console and opens
http://localhost:8090. Ten minutes is enough for the whole story.

## Before the meeting

1. Stop other projects in Docker (`docker compose -p <name> stop`): the memory budget
   assumes this stack has the Docker VM to itself. `make health` warns if one is running.
2. `make up`, then `make health` until everything is healthy (a fresh Redis takes about
   six minutes to rebuild).
3. Stop Dagster if it is running: its schedules would build dbt while you break things.
4. `make demo`. Keep Grafana (http://localhost:3030) and Marquez (http://localhost:3040)
   open in other tabs for questions.

## The story

Press **Play the story**. The banner narrates each step and waits for the real outcome
before moving on; each step's measured result stays listed under the banner.

| Step | What they see | What to say |
| --- | --- | --- |
| Live payments | the pipeline's lights go green, trains move, decisions stream in | Every payment is a row in Postgres; change data capture turns each change into an event, and a decision comes back in about a second, measured from the database commit. |
| Card testing | a burst of tiny payments on one card, declined | Fraudsters check stolen cards with small amounts. The card's rolling features count the burst within seconds. |
| Impossible travel | an in-person payment far from the owner's last one, declined | Features keep the distance and the minutes between in-person payments. |
| Account takeover | new device, changed email, then a big purchase, declined | Customer features join three different event streams. |
| Kill Flink | the Flink lights turn amber, then green; a recovery time | Flink restores from its checkpoint: no feature lost or counted twice. The chaos suite proves it (docs/chaos.md). |
| Cut the feature store | decisions turn to review; 0 approvals counted | If features cannot be trusted the platform never approves blind; it holds payments for review. |
| The lakehouse | parity at 100% | Everything also lands in Iceberg. dbt builds history and point-in-time training sets, and recomputes every feature offline: online and offline agree. |

Or drive it yourself: **Start traffic**, then the attack and failure buttons, in any order.

## Questions they ask

- **Why change data capture instead of having the app publish events?** The database is the
  source of truth. CDC captures every change, including ones the app never meant to
  publish, without touching application code, and it cannot drift from what was committed.
- **How do you know the real-time features are right?** Every decision logs the features it
  used. The lakehouse recomputes them from the raw history; parity is measured daily and
  alerted below 99.9% (`make parity-report`).
- **What about delayed fraud labels?** Chargebacks arrive weeks later. Training sets only use
  labels known at their cutoff and are pinned to an Iceberg snapshot, so the past cannot leak
  into training and every set can be reproduced exactly.
- **How is a rule change made safe?** Backtest on a published training set, then a promotion
  gate refuses any version that loses precision (`make backtest`, `make promote-rules`).
- **What happens when something breaks?** Six failure scenarios run automatically with their
  recovery measured (`make chaos`, docs/chaos.md); alerts are tested with promtool.
- **What would change at real scale?** Several Kafka brokers and Flink TaskManagers, a managed
  catalog and object store, a real feature store, and a trained model next to the rules.

## Deeper, if they want it

Grafana: the SLO dashboard. Marquez: lineage from Postgres to the training set. Dagster: the
asset graph and the gate that stops bad silver data. Flink: the jobs and their checkpoints.
