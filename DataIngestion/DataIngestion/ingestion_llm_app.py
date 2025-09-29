import os
import re
import json
import argparse
from typing import Dict, List, Any, Optional
from copy import deepcopy

from pydantic import BaseModel, Field
from openai import OpenAI

# ---------------------------
# Config
# ---------------------------
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

"""OpenAI SDK compatibility shim
Supports both:
- v1.x: `from openai import OpenAI`; client = OpenAI()
- v0.x: legacy `import openai`; use openai.ChatCompletion
"""
try:
    from openai import OpenAI  # SDK v1+
    client = OpenAI()  # Reads OPENAI_API_KEY from environment
    _OPENAI_SDK_FLAVOR = "v1"
except Exception:
    import openai  # SDK v0.x fallback
    openai.api_key = os.getenv("OPENAI_API_KEY", "")
    client = None  # not used in legacy mode
    _OPENAI_SDK_FLAVOR = "v0"

# ---------------------------
# JSON Schemas for Structured Outputs
# ---------------------------

CLASSIFY_SCHEMA = {
    "name": "IngestionClassification",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "category": {
                "type": "string",
                "description": "High-level type of the ingested artifact",
                "enum": [
                    "email", "local_file", "chat_message",
                    "contract", "invoice", "resume", "id_document",
                    "log", "code", "spreadsheet", "presentation", "image",
                    "other"
                ]
            },
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "routing": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"]},
                    "destinations": {
                        "type": "array",
                        "items": {"type": "string",
                                  "enum": [
                                      "raw_s3_bucket",
                                      "staging_kafka_topic",
                                      "pii_safe_bucket",
                                      "vector_store",
                                      "warehouse_bronze",
                                      "warehouse_silver",
                                      "quarantine_dlq"
                                  ]}
                    },
                    "notes": {"type": "string"}
                },
                "required": ["priority", "destinations"]
            },
            "tags": {"type": "array", "items": {"type": "string"}}
        },
        "required": ["category", "confidence", "routing"]
    },
    "strict": True
}

PII_SCHEMA = {
    "name": "PIIDetection",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "pii": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": [
                                "email", "phone", "credit_card", "aadhaar", "pan",
                                "ssn", "passport", "ip_address", "dob", "name",
                                "address", "bank_account", "ifsc", "other"
                            ]
                        },
                        "text": {"type": "string"},
                        "start": {"type": "integer", "minimum": 0},
                        "end": {"type": "integer", "minimum": 0},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1}
                    },
                    "required": ["type", "text", "start", "end", "confidence"]
                }
            }
        },
        "required": ["pii"]
    },
    "strict": True
}

# ---------------------------
# Simple Regex Heuristics (fast first pass)
# ---------------------------

REGEX_PATTERNS = {
    "email": re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"),
    "phone": re.compile(r"(?:(?:\+?\d{1,3}[\s\-]?)?(?:\(?\d{2,4}\)?[\s\-]?)?\d[\d\-\s]{6,12}\d)"),
    "credit_card": re.compile(r"\b(?:\d[ -]*?){13,19}\b"),
    "aadhaar": re.compile(r"\b(?:\d\s*){12}\b"),
    "pan": re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b", re.IGNORECASE),
    "ip_address": re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b"),
    "dob": re.compile(r"\b(?:\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2})\b"),
}

def regex_find_all(text: str) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    for pii_type, pattern in REGEX_PATTERNS.items():
        for m in pattern.finditer(text):
            findings.append({
                "type": pii_type, "text": m.group(0),
                "start": m.start(), "end": m.end(), "confidence": 0.65
            })
    return findings

# ---------------------------
# LLM helpers
# ---------------------------

def llm_structured_response(system_prompt: str, user_content: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    """Return structured JSON using OpenAI SDK without relying on response_format.

    Strategy:
    1) Try Responses API (newer SDKs) with strong JSON-only instructions.
    2) Fallback to Chat Completions with strong JSON-only instructions.
    3) Parse first JSON object from the model output.
    """
    sys_msg = (
        system_prompt
        + "\nReturn ONLY valid JSON that conforms exactly to this JSON Schema. "
        + "No extra keys. No markdown. No prose."
    )
    user_msg = (
        user_content
        + "\nReturn only JSON, no explanation."
    )

    if _OPENAI_SDK_FLAVOR == "v1":
        # Attempt 1: Responses API (no response_format)
        try:
            resp = client.responses.create(
                model=OPENAI_MODEL,
                input=[
                    {"role": "system", "content": sys_msg},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.2,
            )

            # Try direct .output_text first
            try:
                parsed = json.loads(resp.output_text)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass

            # Walk message content for text blocks
            for item in getattr(resp, "output", []):
                if getattr(item, "type", None) == "message":
                    for c in getattr(item, "content", []):
                        if getattr(c, "type", None) in ("output_text", "input_text"):
                            text = getattr(c, "text", None)
                            if not text:
                                continue
                            try:
                                return json.loads(text)
                            except Exception:
                                # best-effort JSON slice
                                first_brace = text.find("{")
                                last_brace = text.rfind("}")
                                if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
                                    return json.loads(text[first_brace:last_brace + 1])
        except Exception:
            # Responses API may not exist or may behave differently on some installations
            pass

        # Attempt 2: Chat Completions (no response_format)
        messages = [
            {"role": "system", "content": sys_msg},
            {"role": "user", "content": user_msg},
        ]
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=0.2,
        )
        content = resp.choices[0].message.content
        # Best-effort JSON extraction
        first_brace = content.find("{")
        last_brace = content.rfind("}")
        if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
            return json.loads(content[first_brace:last_brace + 1])
        # Final attempt: direct load
        return json.loads(content)

    else:  # legacy SDK v0.x
        # Use openai.ChatCompletion.create
        messages = [
            {"role": "system", "content": sys_msg},
            {"role": "user", "content": user_msg},
        ]
        resp = openai.ChatCompletion.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=0.2,
        )
        content = resp["choices"][0]["message"]["content"]
        first_brace = content.find("{")
        last_brace = content.rfind("}")
        if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
            return json.loads(content[first_brace:last_brace + 1])
        return json.loads(content)

def classify_and_route(text: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    meta_str = json.dumps(metadata or {}, ensure_ascii=False)
    system = (
        "You are a data-ingestion gatekeeper. "
        "Classify the input artifact and propose routing inside a modern data pipeline. "
        "Be conservative if uncertain."
    )
    user = (
        "Artifact content:\n---\n"
        f"{text}\n---\n"
        f"Metadata JSON (may help with source hints): {meta_str}\n\n"
        "Return the structured object per schema."
    )
    return llm_structured_response(system, user, CLASSIFY_SCHEMA)

def llm_validate_and_augment_pii(text: str, seed_findings: List[Dict[str, Any]]) -> Dict[str, Any]:
    system = (
        "You are a PII detection assistant for enterprise data ingestion. "
        "Validate, deduplicate, and add any missing PII spans. "
        "Return spans using exact character indices with respect to the original text."
    )
    user = (
        f"Text to scan (length={len(text)}):\n---\n{text}\n---\n\n"
        f"Seed findings from regex (may be noisy; deduplicate and fix spans):\n"
        f"{json.dumps(seed_findings, ensure_ascii=False)}\n\n"
        "Only return fields defined in the schema."
    )
    return llm_structured_response(system, user, PII_SCHEMA)

def redact_text(text: str, spans: List[Dict[str, Any]], placeholder_fmt: str = "[PII:{type}]") -> str:
    cleaned = []
    for s in spans:
        start, end = int(s["start"]), int(s["end"])
        if 0 <= start < end <= len(text):
            cleaned.append({"start": start, "end": end, "type": s["type"]})
    cleaned.sort(key=lambda x: x["start"])

    merged: List[Dict[str, Any]] = []
    for s in cleaned:
        if not merged or s["start"] > merged[-1]["end"]:
            merged.append(deepcopy(s))
        else:
            merged[-1]["end"] = max(merged[-1]["end"], s["end"])

    out_parts = []
    cursor = 0
    for s in merged:
        out_parts.append(text[cursor:s["start"]])
        out_parts.append(placeholder_fmt.format(type=s["type"]))
        cursor = s["end"]
    out_parts.append(text[cursor:])
    return "".join(out_parts)

# ---------------------------
# Public Engine Facade
# ---------------------------

class IngestionLLM:
    def __init__(self, model: str = OPENAI_MODEL):
        self.model = model

    def classify(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return classify_and_route(text, metadata or {})

    def detect_pii(self, text: str) -> Dict[str, Any]:
        seed = regex_find_all(text)
        llm_result = llm_validate_and_augment_pii(text, seed)
        aligned: List[Dict[str, Any]] = []
        for f in llm_result.get("pii", []):
            start, end = int(f["start"]), int(f["end"])
            if 0 <= start < end <= len(text):
                aligned.append(f)
        return {"pii": aligned}

    def redact(self, text: str, pii_spans: List[Dict[str, Any]], placeholder_fmt: str = "[PII:{type}]") -> str:
        return redact_text(text, pii_spans, placeholder_fmt)

engine = IngestionLLM()

# ----------------------------------------------------
# FastAPI service
# ----------------------------------------------------
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Ingestion LLM Service", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

class ClassifyRequest(BaseModel):
    text: str = Field(..., description="Raw artifact text")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Optional metadata")

class ClassifyResponse(BaseModel):
    category: str
    confidence: float
    routing: Dict[str, Any]
    tags: Optional[List[str]] = None

class DetectPIIRequest(BaseModel):
    text: str

class PiiSpan(BaseModel):
    type: str
    text: str
    start: int
    end: int
    confidence: float

class DetectPIIResponse(BaseModel):
    pii: List[PiiSpan]

class RedactRequest(BaseModel):
    text: str
    pii: List[PiiSpan]
    placeholder_fmt: Optional[str] = "[PII:{type}]"

class RedactResponse(BaseModel):
    redacted_text: str

class ProcessRequest(BaseModel):
    text: str
    metadata: Optional[Dict[str, Any]] = None
    redact: bool = True

class ProcessResponse(BaseModel):
    classification: ClassifyResponse
    pii: DetectPIIResponse
    redacted_text: Optional[str] = None

@app.post("/classify", response_model=ClassifyResponse)
def route_classify(req: ClassifyRequest):
    result = engine.classify(req.text, req.metadata or {})
    return result

@app.post("/detect_pii", response_model=DetectPIIResponse)
def route_detect_pii(req: DetectPIIRequest):
    return engine.detect_pii(req.text)

@app.post("/redact", response_model=RedactResponse)
def route_redact(req: RedactRequest):
    red = engine.redact(req.text, [s.model_dump() for s in req.pii], req.placeholder_fmt)
    return {"redacted_text": red}

@app.post("/process", response_model=ProcessResponse)
def route_process(req: ProcessRequest):
    classification = engine.classify(req.text, req.metadata or {})
    pii = engine.detect_pii(req.text)
    redacted = engine.redact(req.text, pii["pii"]) if req.redact else None
    return {
        "classification": classification,
        "pii": pii,
        "redacted_text": redacted
    }

# ----------------------------------------------------
# Streamlit UI
# ----------------------------------------------------
def run_streamlit():
    global OPENAI_MODEL
    import streamlit as st

    st.set_page_config(page_title="Ingestion LLM Console", layout="wide")
    st.title("🧩 Ingestion LLM — Classification, PII Detection & Redaction")

    with st.sidebar:
        st.header("Settings")
        model = st.text_input("OpenAI Model", value=OPENAI_MODEL)
        if model != OPENAI_MODEL:
            OPENAI_MODEL = model
            engine.model = model

        st.caption("Set OPENAI_API_KEY in your environment before running.")

        st.markdown("---")
        placeholder = st.text_input("Redaction placeholder format", value="[PII:{type}]")
        show_seed = st.checkbox("Show regex seed PII", value=False)

    st.subheader("Input")
    uploaded = st.file_uploader("Upload a text file (optional)", type=["txt", "log", "csv", "md"])
    text = ""
    if uploaded is not None:
        text = uploaded.read().decode("utf-8", errors="ignore")
    text = st.text_area("Paste or edit text", value=text, height=220)

    meta_default = {"source_hint": "unknown", "filename": getattr(uploaded, "name", None)}
    metadata_str = st.text_area("Metadata (JSON, optional)", value=json.dumps(meta_default, indent=2), height=140)

    col_a, col_b, col_c, col_d = st.columns([1,1,1,1])
    go_classify = col_a.button("Classify & Route")
    go_pii = col_b.button("Detect PII")
    go_both = col_c.button("Full Pipeline")
    clear = col_d.button("Clear")

    if clear:
        st.experimental_rerun()

    if not text.strip():
        st.info("Provide some text to get started.")
        return

    # Parse metadata JSON
    metadata: Dict[str, Any] = {}
    try:
        if metadata_str.strip():
            metadata = json.loads(metadata_str)
    except Exception as e:
        st.error(f"Metadata JSON parse error: {e}")
        metadata = {}

    # Panels
    with st.expander("🔎 Classification & Routing", expanded=(go_classify or go_both)):
        if go_classify or go_both:
            with st.spinner("Calling LLM for classification..."):
                result = classify_and_route(text, metadata)
            st.json(result)

    with st.expander("🛡️ PII Detection", expanded=(go_pii or go_both)):
        if go_pii or go_both:
            seed = regex_find_all(text)
            if show_seed:
                st.caption("Regex seed findings")
                st.json(seed)
            with st.spinner("Validating/augmenting PII with LLM..."):
                pii = llm_validate_and_augment_pii(text, seed)
            st.json(pii)

            redacted = redact_text(text, pii.get("pii", []), placeholder)
            st.subheader("Redacted Output")
            st.code(redacted)

            st.download_button(
                "Download Redacted Text",
                data=redacted,
                file_name="redacted.txt",
                mime="text/plain"
            )

    st.markdown("---")
    st.caption("Tip: Use the full pipeline to classify, detect PII, and produce a redacted twin in one go.")

# ----------------------------------------------------
# CLI (for Streamlit arg passthrough)
# ----------------------------------------------------
def _parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--mode", choices=["ui", "none"], default="none")
    return parser.parse_known_args()[0]

if __name__ == "__main__":
    # When running via `streamlit run ingestion_llm_app.py -- --mode ui`
    args = _parse_args()
    if args.mode == "ui":
        run_streamlit()
    else:
        # Simple console demo
        sample = (
            "From: rhea.sharma@acmebank.com\n"
            "Phone: +91 98765-43210\n"
            "PAN: ABCDE1234F\n"
            "Aadhaar: 1234 5678 9012\n"
            "Hi team, please route this invoice to Finance. Card: 4111 1111 1111 1111.\n"
            "Ship to: 221B Baker Street, London\n"
        )
        demo_meta = {"source_hint": "email_ingestion", "filename": "rhea_invoice.txt"}
        print("=== Classification ===")
        print(json.dumps(engine.classify(sample, demo_meta), indent=2))
        print("\n=== PII ===")
        pii = engine.detect_pii(sample)
        print(json.dumps(pii, indent=2))
        print("\n=== Redacted ===")
        print(engine.redact(sample, pii["pii"]))
