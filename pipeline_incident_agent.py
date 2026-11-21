"""Investigate a pipeline incident from a local JSON snapshot or a built-in demo.

Python 3.10+. No dependencies. Run with --help for usage and --example
for a JSON snapshot you can adapt. The program only reads supplied evidence.
"""

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path


DEMO = {
    "run_id": "cargo-demo-042",
    "error_code": "CAST_INVALID_INPUT",
    "expected_schema": {"shipment_id": "string", "pieces": "int"},
    "incoming_schema": {"shipment_id": "string", "pieces": "string"},
    "checkpoint_status": "healthy",
    "baseline_rows": 8200,
    "input_rows": 8160,
}


@dataclass(frozen=True)
class Finding:
    cause: str
    evidence: str
    next_step: str


def check_schema(snapshot):
    expected = snapshot.get("expected_schema")
    incoming = snapshot.get("incoming_schema")
    if expected is None or incoming is None:
        return "Schema snapshots missing.", None

    missing = sorted(expected.keys() - incoming.keys())
    changed = [
        f"{column}: expected {expected[column]}, received {incoming[column]}"
        for column in sorted(expected.keys() & incoming.keys())
        if expected[column].strip().lower() != incoming[column].strip().lower()
    ]
    differences = [f"missing required column {column}" for column in missing] + changed
    if not differences:
        return "All required columns have their expected types.", None

    evidence = "; ".join(differences)
    return evidence, Finding(
        "Input schema mismatch",
        evidence,
        "Inspect rejected rows and compare the producer's schema with the ingestion contract.",
    )


def check_checkpoint(snapshot):
    status = snapshot.get("checkpoint_status", "unknown")
    if status in {"healthy", "unknown"}:
        return f"Checkpoint status: {status}.", None

    evidence = f"Checkpoint reported {status}."
    return evidence, Finding(
        "Streaming checkpoint problem",
        evidence,
        "Check checkpoint access, recent deployment changes, and the stored streaming progress.",
    )


def check_input_count(snapshot):
    actual = snapshot.get("input_rows")
    baseline = snapshot.get("baseline_rows")
    if actual is None or not baseline:
        return "A comparable positive row-count baseline is unavailable.", None

    evidence = f"{actual:,} input rows against a baseline of {baseline:,}."
    if actual * 2 >= baseline:
        return evidence, None

    return evidence, Finding(
        "Input volume below half of baseline",
        evidence,
        "Verify upstream delivery, input paths, and partition filters for this run.",
    )


CHECKS = {
    "schema": check_schema,
    "checkpoint": check_checkpoint,
    "input volume": check_input_count,
}

ERROR_HINTS = {
    "CAST_INVALID_INPUT": "schema",
    "SCHEMA_MISMATCH": "schema",
    "CHECKPOINT_UNAVAILABLE": "checkpoint",
    "CHECKPOINT_CORRUPT": "checkpoint",
    "MISSING_INPUT": "input volume",
}


def investigate(snapshot):
    code = snapshot.get("error_code", "UNKNOWN").upper()
    preferred = ERROR_HINTS.get(code)
    order = [preferred] if preferred else []
    order.extend(name for name in CHECKS if name != preferred)

    trace = []
    findings = []
    for name in order:
        observation, finding = CHECKS[name](snapshot)
        trace.append(f"{name}: {observation}")
        if finding:
            findings.append(finding)
            if name == preferred:
                break

    print(f"Run {snapshot.get('run_id', 'unknown')} | Error {code}")
    print("\nChecks performed")
    for index, entry in enumerate(trace, 1):
        print(f"{index}. {entry}")

    if not findings:
        print("\nDiagnosis: No supported cause found in the available evidence.")
        print("Next step: Collect the failure log and fill in any missing snapshots.")
        return

    heading = "Likely cause" if preferred and len(trace) == 1 else "Leads to investigate"
    print(f"\n{heading}")
    for finding in findings:
        print(f"- {finding.cause}")
        print(f"  Evidence: {finding.evidence}")
        print(f"  Next step: {finding.next_step}")


def load_snapshot(path):
    snapshot = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(snapshot, dict):
        raise ValueError("Snapshot must be a JSON object.")

    for field in ("run_id", "error_code"):
        if field in snapshot and not isinstance(snapshot[field], str):
            raise ValueError(f"{field} must be a string.")

    for field in ("expected_schema", "incoming_schema"):
        value = snapshot.get(field)
        if value is not None and (
            not isinstance(value, dict)
            or any(not isinstance(item, str) or not item.strip() for item in value.values())
        ):
            raise ValueError(f"{field} must map column names to non-empty type strings.")

    for field in ("input_rows", "baseline_rows"):
        value = snapshot.get(field)
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError(f"{field} must be a non-negative integer.")

    status = snapshot.get("checkpoint_status", "unknown")
    if status not in ("healthy", "unknown", "missing", "inaccessible", "corrupt"):
        raise ValueError("checkpoint_status must be healthy, unknown, missing, inaccessible, or corrupt.")

    return snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--demo", action="store_true", help="investigate a simulated incident")
    inputs.add_argument("--example", action="store_true", help="print a sample JSON snapshot")
    inputs.add_argument("--input", type=Path, help="investigate a local JSON snapshot")
    args = parser.parse_args()

    if args.example:
        print(json.dumps(DEMO, indent=2))
        return 0

    try:
        snapshot = DEMO if args.demo else load_snapshot(args.input)
    except (OSError, ValueError) as error:
        print(f"Could not load snapshot: {error}", file=sys.stderr)
        return 2

    investigate(snapshot)
    return 0


if __name__ == "__main__":
    sys.exit(main())
