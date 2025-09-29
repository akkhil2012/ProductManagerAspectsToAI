from typing import Any, Dict
import json
import pytest
from fastapi.testclient import TestClient

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import ingestion_llm_app as appmod

client = TestClient(appmod.app)


def test_classify_email(monkeypatch):
    def fake_classify(text: str, metadata: Dict[str, Any]):
        return {
            "category": "email",
            "confidence": 0.91,
            "routing": {"priority": "normal", "destinations": ["warehouse_bronze"], "notes": "ok"},
            "tags": ["header:from", "header:subject"],
        }

    monkeypatch.setattr(appmod.engine, "classify", fake_classify)

    payload = {"text": "From: a@b.com\nSubject: Hi", "metadata": {"source_hint": "email_ingestion"}}
    r = client.post("/classify", json=payload)
    assert r.status_code == 200, (
        f"/classify status check failed. Request: {json.dumps(payload)} | "
        f"Status: {r.status_code} | Body: {r.text}"
    )
    body = r.json()
    expected_category = "email"
    assert body["category"] == expected_category, (
        f"/classify category mismatch. Request: {json.dumps(payload)} | "
        f"Expected: {expected_category} | Got: {json.dumps(body)}"
    )
    assert 0 <= body["confidence"] <= 1, (
        f"/classify confidence out of bounds. Request: {json.dumps(payload)} | "
        f"Got: {json.dumps(body)}"
    )
    assert body["routing"]["priority"] in {"low", "normal", "high", "urgent"}, (
        f"/classify invalid priority. Request: {json.dumps(payload)} | "
        f"Got: {json.dumps(body)}"
    )


def test_classify_chat_message(monkeypatch):
    def fake_classify(text: str, metadata: Dict[str, Any]):
        return {
            "category": "chat_message",
            "confidence": 0.74,
            "routing": {"priority": "low", "destinations": ["staging_kafka_topic"], "notes": "chat"},
            "tags": ["chat:user_label"],
        }

    monkeypatch.setattr(appmod.engine, "classify", fake_classify)

    payload = {"text": "[10:04] You: hey there\nAssistant: hello!", "metadata": {}}
    r = client.post("/classify", json=payload)
    assert r.status_code == 200, (
        f"/classify status check failed. Request: {json.dumps(payload)} | "
        f"Status: {r.status_code} | Body: {r.text}"
    )
    body = r.json()
    expected_category = "chat_message"
    expected_dest = ["staging_kafka_topic"]
    assert body["category"] == expected_category, (
        f"/classify category mismatch. Request: {json.dumps(payload)} | "
        f"Expected: {expected_category} | Got: {json.dumps(body)}"
    )
    assert body["routing"]["destinations"] == expected_dest, (
        f"/classify destinations mismatch. Request: {json.dumps(payload)} | "
        f"Expected: {json.dumps(expected_dest)} | Got: {json.dumps(body)}"
    )


def test_classify_local_file(monkeypatch):
    def fake_classify(text: str, metadata: Dict[str, Any]):
        return {
            "category": "local_file",
            "confidence": 0.6,
            "routing": {"priority": "low", "destinations": ["raw_s3_bucket"], "notes": "file"},
        }

    monkeypatch.setattr(appmod.engine, "classify", fake_classify)

    payload = {"text": "# Notes\n- item 1\n- item 2", "metadata": {"filename": "notes.md"}}
    r = client.post("/classify", json=payload)
    assert r.status_code == 200, (
        f"/classify status check failed. Request: {json.dumps(payload)} | "
        f"Status: {r.status_code} | Body: {r.text}"
    )
    body = r.json()
    expected_category = "local_file"
    assert body["category"] == expected_category, (
        f"/classify category mismatch. Request: {json.dumps(payload)} | "
        f"Expected: {expected_category} | Got: {json.dumps(body)}"
    )
    assert "raw_s3_bucket" in body["routing"]["destinations"], (
        f"/classify destination missing 'raw_s3_bucket'. Request: {json.dumps(payload)} | "
        f"Got: {json.dumps(body)}"
    )


def test_classify_invoice_with_routing(monkeypatch):
    def fake_classify(text: str, metadata: Dict[str, Any]):
        return {
            "category": "invoice",
            "confidence": 0.83,
            "routing": {"priority": "high", "destinations": ["warehouse_silver", "vector_store"], "notes": "invoice"},
            "tags": ["finance"],
        }

    monkeypatch.setattr(appmod.engine, "classify", fake_classify)

    payload = {"text": "Invoice #123 Total: $5400", "metadata": {"source_hint": "vendor_upload"}}
    r = client.post("/classify", json=payload)
    assert r.status_code == 200, (
        f"/classify status check failed. Request: {json.dumps(payload)} | "
        f"Status: {r.status_code} | Body: {r.text}"
    )
    body = r.json()
    expected_category = "invoice"
    expected_dests = {"warehouse_silver", "vector_store"}
    expected_tags = ["finance"]
    assert body["category"] == expected_category, (
        f"/classify category mismatch. Request: {json.dumps(payload)} | "
        f"Expected: {expected_category} | Got: {json.dumps(body)}"
    )
    assert set(body["routing"]["destinations"]) == expected_dests, (
        f"/classify destinations mismatch. Request: {json.dumps(payload)} | "
        f"Expected: {json.dumps(sorted(list(expected_dests)))} | Got: {json.dumps(body)}"
    )
    assert body.get("tags") == expected_tags, (
        f"/classify tags mismatch. Request: {json.dumps(payload)} | "
        f"Expected: {json.dumps(expected_tags)} | Got: {json.dumps(body)}"
    )


def test_classify_ambiguous_defaults(monkeypatch):
    def fake_classify(text: str, metadata: Dict[str, Any]):
        return {
            "category": "other",
            "confidence": 0.5,
            "routing": {"priority": "normal", "destinations": ["quarantine_dlq"], "notes": "ambiguous"},
        }

    monkeypatch.setattr(appmod.engine, "classify", fake_classify)

    payload = {"text": "misc content", "metadata": None}
    r = client.post("/classify", json=payload)
    assert r.status_code == 200, (
        f"/classify status check failed. Request: {json.dumps(payload)} | "
        f"Status: {r.status_code} | Body: {r.text}"
    )
    body = r.json()
    expected_category = "other"
    expected_dest = ["quarantine_dlq"]
    assert body["category"] == expected_category, (
        f"/classify category mismatch. Request: {json.dumps(payload)} | "
        f"Expected: {expected_category} | Got: {json.dumps(body)}"
    )
    assert body["routing"]["destinations"] == expected_dest, (
        f"/classify destinations mismatch. Request: {json.dumps(payload)} | "
        f"Expected: {json.dumps(expected_dest)} | Got: {json.dumps(body)}"
    )


def test_classify_handles_missing_metadata(monkeypatch):
    def fake_classify(text: str, metadata: Dict[str, Any]):
        # Ensure the API passes an empty dict if metadata is None
        assert isinstance(metadata, dict)
        return {
            "category": "code",
            "confidence": 0.7,
            "routing": {"priority": "normal", "destinations": ["warehouse_bronze"], "notes": "code"},
        }

    monkeypatch.setattr(appmod.engine, "classify", fake_classify)

    payload = {"text": "def foo(): pass"}
    r = client.post("/classify", json=payload)
    assert r.status_code == 200, (
        f"/classify status check failed. Request: {json.dumps(payload)} | "
        f"Status: {r.status_code} | Body: {r.text}"
    )
    body = r.json()
    expected_category = "code"
    assert body["category"] == expected_category, (
        f"/classify category mismatch. Request: {json.dumps(payload)} | "
        f"Expected: {expected_category} | Got: {json.dumps(body)}"
    )
