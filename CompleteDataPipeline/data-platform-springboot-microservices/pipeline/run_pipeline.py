#!/usr/bin/env python3
"""Orchestrate the Spring Boot microservices into an end-to-end data pipeline."""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Sequence

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - handled at runtime
    yaml = None

try:
    import requests  # type: ignore
    from requests import Response
except ImportError as exc:  # pragma: no cover - handled at runtime
    raise SystemExit(
        "Missing dependency 'requests'. Install it with `pip install requests`."
    ) from exc


LOGGER = logging.getLogger("pipeline")
DEFAULT_CONFIG_PATH = Path(__file__).with_name("pipeline_config.yaml")
DEFAULT_DATA_PATH = Path(__file__).with_name("sample_data.json")


class PipelineError(RuntimeError):
    """Raised when a pipeline stage fails irrecoverably."""


@dataclass
class StageResult:
    """Container for service call metadata."""

    stage: str
    records: List[Dict[str, Any]] = field(default_factory=list)

    def add(self, payload: Dict[str, Any], response: Dict[str, Any]) -> None:
        self.records.append({"payload": payload, "response": response})


class PipelineRunner:
    """Coordinates data through all Spring Boot services."""

    def __init__(
        self,
        config: Dict[str, Any],
        raw_records: Sequence[Dict[str, Any]],
        simulate: bool = False,
    ) -> None:
        self._config = config
        self._raw_records = list(raw_records)
        self._simulate = simulate
        self._session = requests.Session()

        http_config = config.get("http", {})
        self._timeout = http_config.get("timeout_seconds", 10)
        self._retries = max(0, int(http_config.get("retry_attempts", 0)))
        self._backoff = float(http_config.get("retry_backoff_seconds", 1.0))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self) -> Dict[str, StageResult]:
        LOGGER.info("Starting pipeline with %s raw records", len(self._raw_records))
        ingestion_result = self._run_ingestion()
        dedup_result = self._run_deduplication(ingestion_result)
        quality_result = self._run_quality(dedup_result)

        return {
            "ingestion": ingestion_result,
            "deduplication": dedup_result,
            "quality": quality_result,
        }

    # ------------------------------------------------------------------
    # Stage implementations
    # ------------------------------------------------------------------
    def _run_ingestion(self) -> StageResult:
        stage_name = "dataingestion"
        result = StageResult(stage=stage_name)

        for idx, record in enumerate(self._raw_records, start=1):
            pipeline_record_id = f"ing-{record['source_record_id']}-{idx:03d}"
            payload = {
                "recordId": pipeline_record_id,
                "status": "INGESTED",
                "dataPayload": json.dumps(record),
            }
            LOGGER.debug("Ingestion payload %s: %s", idx, payload)
            response = self._post(stage_name, payload)
            result.add(payload, response)

        LOGGER.info("Ingestion stage completed for %s records", len(result.records))
        return result

    def _run_deduplication(self, ingestion_result: StageResult) -> StageResult:
        stage_name = "datadeduplication"
        result = StageResult(stage=stage_name)

        # Deduplicate records by the original source identifier
        deduped: Dict[str, Dict[str, Any]] = {}
        for record_entry in ingestion_result.records:
            raw_payload = json.loads(record_entry["payload"]["dataPayload"])
            deduped.setdefault(raw_payload["source_record_id"], raw_payload)

        LOGGER.info("Deduplication reduced %s ingested records to %s unique records", len(ingestion_result.records), len(deduped))

        for idx, raw_record in enumerate(deduped.values(), start=1):
            payload = {
                "recordId": f"dedup-{raw_record['source_record_id']}-{idx:03d}",
                "status": "DEDUPLICATED",
                "dataPayload": json.dumps({
                    **raw_record,
                    "deduplication_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }),
            }
            LOGGER.debug("Deduplication payload %s: %s", idx, payload)
            response = self._post(stage_name, payload)
            result.add(payload, response)

        return result

    def _run_quality(self, dedup_result: StageResult) -> StageResult:
        stage_name = "dataquality"
        result = StageResult(stage=stage_name)

        for idx, record_entry in enumerate(dedup_result.records, start=1):
            raw_payload = json.loads(record_entry["payload"]["dataPayload"])
            quality_status, quality_notes = self._evaluate_quality(raw_payload)

            payload = {
                "recordId": f"quality-{raw_payload['source_record_id']}-{idx:03d}",
                "status": quality_status,
                "dataPayload": json.dumps({
                    **raw_payload,
                    "quality_notes": quality_notes,
                    "quality_checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }),
            }
            LOGGER.debug("Quality payload %s: %s", idx, payload)
            response = self._post(stage_name, payload)
            result.add(payload, response)

        LOGGER.info("Quality stage evaluated %s records", len(result.records))
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _evaluate_quality(self, payload: Dict[str, Any]) -> (str, str):
        """Apply lightweight business rules to the deduplicated payload."""
        notes: List[str] = []
        status = "VALID"

        email = payload.get("customer_email", "")
        if "@" not in email:
            status = "INVALID"
            notes.append("customer_email is missing '@'")

        amount = payload.get("purchase_amount", 0)
        if not isinstance(amount, (int, float)) or amount <= 0:
            status = "INVALID"
            notes.append("purchase_amount must be positive")

        if not notes:
            notes.append("Record passed default validation rules")

        return status, "; ".join(notes)

    def _post(self, service_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self._simulate:
            LOGGER.info("Simulating POST to %s with payload: %s", service_key, payload)
            return {"simulated": True, "echo": payload}

        service_cfg = self._config["services"].get(service_key)
        if not service_cfg:
            raise PipelineError(f"Missing configuration for service '{service_key}'")

        url = service_cfg["base_url"].rstrip("/") + service_cfg["endpoint"]
        LOGGER.info("POST %s", url)

        last_exception: Exception | None = None
        for attempt in range(1, self._retries + 2):
            try:
                response: Response = self._session.post(url, json=payload, timeout=self._timeout)
                response.raise_for_status()
                if response.content:
                    return response.json()
                return {"status_code": response.status_code}
            except requests.RequestException as exc:  # type: ignore[attr-defined]
                last_exception = exc
                LOGGER.warning("Attempt %s/%s to call %s failed: %s", attempt, self._retries + 1, url, exc)
                if attempt <= self._retries:
                    time.sleep(self._backoff * attempt)
                else:
                    break

        raise PipelineError(f"Failed to call {service_key} after {self._retries + 1} attempts") from last_exception


# ----------------------------------------------------------------------
# CLI Entrypoint
# ----------------------------------------------------------------------

def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the data platform pipeline")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Path to the pipeline configuration YAML file")
    parser.add_argument("--input", type=Path, default=DEFAULT_DATA_PATH, help="Path to the JSON file with raw records")
    parser.add_argument("--simulate", action="store_true", help="Skip HTTP calls and log the generated payloads")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    return parser.parse_args(argv)


def load_config(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if yaml is not None:  # pragma: no branch - optional dependency
        return yaml.safe_load(text)
    LOGGER.warning("pyyaml not installed; using lightweight YAML parser")
    return _parse_simple_yaml(text)


def load_records(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _parse_simple_yaml(text: str) -> Dict[str, Any]:
    """Parse a minimal subset of YAML (mappings only).

    The helper supports the indentation-based dictionaries used in
    `pipeline_config.yaml`. It is *not* a full YAML parser, but it keeps the
    orchestrator runnable in restricted environments where `pyyaml` cannot be
    installed.
    """

    def _coerce(value: str) -> Any:
        value = value.strip()
        if value.lower() in {"true", "false"}:
            return value.lower() == "true"
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value

    root: Dict[str, Any] = {}
    stack: List[tuple[int, Dict[str, Any]]] = [(-1, root)]

    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line:
            continue

        indent = len(line) - len(line.lstrip(" "))
        key, _, value = line.strip().partition(":")
        if not key:
            continue

        while stack and indent <= stack[-1][0]:
            stack.pop()

        parent = stack[-1][1]
        if value.strip():
            parent[key] = _coerce(value)
        else:
            child: Dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))

    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s | %(levelname)8s | %(message)s")

    if not args.config.exists():
        LOGGER.error("Configuration file not found: %s", args.config)
        return 1

    if not args.input.exists():
        LOGGER.error("Input file not found: %s", args.input)
        return 1

    config = load_config(args.config)
    raw_records = load_records(args.input)

    runner = PipelineRunner(config=config, raw_records=raw_records, simulate=args.simulate)

    try:
        results = runner.run()
    except PipelineError as exc:
        LOGGER.error("Pipeline execution failed: %s", exc)
        return 2

    LOGGER.info("Pipeline finished successfully")
    for stage, stage_result in results.items():
        LOGGER.info("Stage '%s' produced %s records", stage, len(stage_result.records))

    return 0


if __name__ == "__main__":
    sys.exit(main())
