# Week 6 — Preparation before class

[Module homepage](../README.md) · [Week 6 overview](../weeks/06-workflow-orchestration-continued/README.md)

Week 6 continues the orchestration work from Week 5: a full ETL flow with separate Extract, Transform, and Load tasks, retries, restart after a failure, and scheduling. Keep your Week 5 Kestra environment and taxi data. Follow the [Week 5 preparation checklist](week-05.md) if you have not completed it; no separate Week 6 environment is required.

Before the session, think about these questions: How does Transform receive the file produced by Extract? If Load fails, should we download and transform everything again? When should a task retry automatically? How does a scheduled run decide which month to process? You do not need to prepare written answers.
