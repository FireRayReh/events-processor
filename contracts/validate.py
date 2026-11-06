"""Validate every contracts/examples/*.json against its schema. Usage: python3 contracts/validate.py"""
import json, pathlib, sys
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

root = pathlib.Path(__file__).parent
schemas = {p.name: json.loads(p.read_text()) for p in root.glob("*.schema.json")}
registry = Registry().with_resources(
    (s["$id"], Resource.from_contents(s)) for s in schemas.values())

# example file prefix -> schema file
mapping = {
    "audit-event.": "audit-event.v1.schema.json",
    "audit-kafka-message": "audit-kafka-message.v1.schema.json",
    "audit-dlq-message": "audit-dlq-message.v1.schema.json",
    "audit-query-request": "audit-query-request.v1.schema.json",
    "audit-query-response": "audit-query-response.v1.schema.json",
    "audit-export-request": "audit-export-request.v1.schema.json",
    "audit-export-job": "audit-export-job.v1.schema.json",
    "audit-relay-heartbeat": "audit-relay-heartbeat.v1.schema.json",
}

failed = 0
for name, s in schemas.items():
    Draft202012Validator.check_schema(s)
for ex in sorted((root / "examples").glob("*.json")):
    schema_name = next(v for k, v in mapping.items() if ex.name.startswith(k))
    v = Draft202012Validator(schemas[schema_name], registry=registry, format_checker=FormatChecker())
    errors = list(v.iter_errors(json.loads(ex.read_text())))
    print(("FAIL " if errors else "ok   ") + f"{ex.name} -> {schema_name}")
    for e in errors:
        print(f"     {'/'.join(map(str, e.absolute_path))}: {e.message}")
    failed += bool(errors)
sys.exit(1 if failed else 0)
