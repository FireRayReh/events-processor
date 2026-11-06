# events-processor — audit pipeline

Captures every business action across .NET 10 services → Kafka → immutable MySQL store → React dashboard
(filtering + export). Target: 2M+ events/day, zero loss, no visible duplicates.

- Design, pitfalls and tradeoffs: [`docs/audit-pipeline-design.md`](docs/audit-pipeline-design.md)
- JSON contracts (JSON Schema 2020-12) + examples: [`contracts/`](contracts) — validate with `python3 contracts/validate.py`
- MySQL DDL: [`db/mysql/001_audit_schema.sql`](db/mysql/001_audit_schema.sql)
