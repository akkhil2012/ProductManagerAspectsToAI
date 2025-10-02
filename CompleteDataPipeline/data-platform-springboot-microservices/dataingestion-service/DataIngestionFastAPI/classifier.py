
import os
import re
import json
from typing import Dict, Any, List
from dataclasses import dataclass

# ----------------- Optional OpenAI -----------------
USE_OPENAI = bool(os.getenv("OPENAI_API_KEY"))
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ----------------- Simple PII Heuristics -----------------
PAN_REGEX = re.compile(r"\b([A-Z]{5}[0-9]{4}[A-Z])\b")  # e.g., ABCDE1234F
AADHAAR_REGEX = re.compile(r"(?<!\d)(\d{4}\s?\d{4}\s?\d{4})(?!\d)")
EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_REGEX = re.compile(r"(?:(?:\+?91[\s\-]?)?[6-9]\d{9})|\b\d{3}[-\s.]?\d{3}[-\s.]?\d{4}\b")
CREDIT_CARD_REGEX = re.compile(r"\b(?:\d[ -]*?){13,19}\b")
IP_REGEX = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
DOB_REGEX = re.compile(r"\b(?:\d{1,2}[-/]){2}\d{2,4}\b")  # simplistic

def luhn_check(number: str) -> bool:
    digits = [int(d) for d in re.sub(r"\D", "", number)]
    if len(digits) < 13:
        return False
    checksum = 0
    parity = len(digits) % 2
    for i, d in enumerate(digits):
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0

def pii_fallback(text: str) -> Dict[str, List[str]]:
    candidates = {
        "emails": EMAIL_REGEX.findall(text),
        "phones": PHONE_REGEX.findall(text),
        "pan": PAN_REGEX.findall(text),
        "aadhaar": AADHAAR_REGEX.findall(text),
        "ip": IP_REGEX.findall(text),
        "dob_like": DOB_REGEX.findall(text),
        "credit_cards_raw": CREDIT_CARD_REGEX.findall(text),
    }
    cards_valid = []
    for c in candidates["credit_cards_raw"]:
        if luhn_check(c):
            cards_valid.append(c)
    candidates["credit_cards"] = cards_valid
    candidates.pop("credit_cards_raw", None)
    return candidates

def source_heuristics(text: str, had_file: bool) -> str:
    if had_file:
        return "local_file"
    if re.search(r"(?im)^(from|to|subject|cc|bcc)\s*:", text):
        return "email"
    if re.search(r"\b(you:|me:|agent:|bot:|whatsapp|slack|teams|chat)\b", text, re.I):
        return "chat"
    return "unknown"

def classify_with_llm(text: str) -> Dict[str, Any]:
    if not USE_OPENAI:
        return {}
    try:
        from openai import OpenAI
        client = OpenAI()
        system = {
            "role": "system",
            "content": (
                "You are a precise data classifier. "
                "Return strict JSON with keys: data_origin (email|chat|local_file|unknown), "
                "pii_present (true|false), pii_types (array of strings among: email, phone, pan, aadhaar, ip, dob, credit_card, name, address), "
                "summary (string <= 40 words). No extra commentary."
            )
        }
        user = {
            "role": "user",
            "content": f"Classify the following text for data origin and PII:\n\n{text[:8000]}"
        }
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[system, user],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content
        return json.loads(content)
    except Exception as e:
        return {"_llm_error": str(e)}

def combine_results(text: str, had_file: bool) -> Dict[str, Any]:
    pii = pii_fallback(text)
    pii_present_fallback = any(len(v) > 0 for v in pii.values())
    source_guess = source_heuristics(text, had_file)

    llm = classify_with_llm(text)
    result = {}

    if llm and "data_origin" in llm:
        result["data_origin"] = llm.get("data_origin", source_guess)
        result["pii_present"] = bool(llm.get("pii_present", pii_present_fallback))
        result["pii_types"] = llm.get("pii_types", [])
        result["summary"] = llm.get("summary", "")
        result["_llm_used"] = True
        if "_llm_error" in llm:
            result["_llm_error"] = llm["_llm_error"]
    else:
        mapped_types = []
        mapping = {
            "emails": "email",
            "phones": "phone",
            "pan": "pan",
            "aadhaar": "aadhaar",
            "ip": "ip",
            "dob_like": "dob",
            "credit_cards": "credit_card",
        }
        for k, v in pii.items():
            if v:
                mapped_types.append(mapping.get(k, k))
        result.update({
            "data_origin": source_guess,
            "pii_present": pii_present_fallback,
            "pii_types": sorted(set(mapped_types)),
            "summary": "Heuristic classification (no LLM).",
            "_llm_used": False,
        })
    result["pii_matches"] = pii
    return result

# -------------- File Readers (txt/csv/json/pdf/docx) --------------
def read_text_from_bytes(name: str, data: bytes) -> str:
    name_lower = (name or "").lower()
    if name_lower.endswith((".txt", ".md", ".csv", ".json")):
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("latin-1", errors="ignore")
    elif name_lower.endswith(".pdf"):
        try:
            from PyPDF2 import PdfReader
            import io
            reader = PdfReader(io.BytesIO(data))
            parts = []
            for page in reader.pages:
                parts.append(page.extract_text() or "")
            return "\\n".join(parts)
        except Exception as e:
            return f"[Error reading PDF: {e}]"
    elif name_lower.endswith(".docx"):
        try:
            import tempfile
            from docx import Document
            with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as tmp:
                tmp.write(data)
                tmp.flush()
                path = tmp.name
            doc = Document(path)
            text = "\\n".join(p.text for p in doc.paragraphs)
            try:
                os.unlink(path)
            except Exception:
                pass
            return text
        except Exception as e:
            return f"[Error reading DOCX: {e}]"
    else:
        # best-effort text decode
        try:
            return data.decode("utf-8")
        except Exception:
            return data.decode("latin-1", errors="ignore")
