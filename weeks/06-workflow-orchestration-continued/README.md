# Week 6 — Workflow orchestration continued

[Module homepage](../../README.md) · [Preparation](../../preparation/week-06.md)

This week completes the orchestration activities remaining from Week 5. We continue using the same Kestra environment, Python scripts, and NYC Taxi data in [`weeks/05-workflow-orchestration`](../05-workflow-orchestration/README.md).

## What we will cover

- **Full ETL with three separate tasks:** Extract, Transform, and Load; follow their execution order and understand how they exchange data.
- **Retry:** automatically try a task again after a temporary failure and inspect the attempts in the logs.
- **Restart:** fix the cause of a failed Load task and resume using the completed transformation output.
- **Scheduling:** configure when the pipeline runs and select the correct year and month for each scheduled execution.

## Hands-on materials

Continue with [Part 5 — Build a complete ETL flow](../05-workflow-orchestration/part-5-full-etl.md), including the three-task challenge and the scheduling, retry, and restart exercises. Use the Week 5 folder for Docker commands; this Week 6 folder is an overview, not a separate environment.

By the end, you should be able to explain how your pipeline runs automatically, how its tasks share data, and how it recovers from a failure.
