"""Parse LLM responses for question generation."""

from __future__ import annotations

import json
import re
from typing import Any, Mapping

from src.synthesis.database.utils import stable_jsonify, to_text

from .models import QuestionGenerationCandidate


def _extract_json_payload(text: str) -> str:
    stripped = text.strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", stripped, re.I | re.S)
    if fenced:
        return fenced.group(1).strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped
    first_brace = stripped.find("{")
    last_brace = stripped.rfind("}")
    if first_brace >= 0 and last_brace > first_brace:
        return stripped[first_brace : last_brace + 1].strip()
    return stripped


def _extract_keyed_field(text: str, field_name: str) -> str:
    pattern = rf"(?im)^\s*{re.escape(field_name)}\s*[:：-]\s*(.+?)\s*$"
    match = re.search(pattern, text)
    return match.group(1).strip() if match else ""


def _extract_jsonish_string_field(text: str, field_name: str) -> str:
    pattern = rf'(?is)"{re.escape(field_name)}"\s*:\s*"((?:\\.|[^"\\])*)"'
    match = re.search(pattern, text)
    if not match:
        return ""
    try:
        return json.loads(f'"{match.group(1)}"')
    except json.JSONDecodeError:
        return match.group(1).strip()


def _extract_jsonish_partial_string_field(text: str, field_name: str) -> str:
    pattern = rf'(?is)"{re.escape(field_name)}"\s*:\s*"(.+)'
    match = re.search(pattern, text)
    if not match:
        return ""
    value = match.group(1)
    for delimiter in ('",\n', '"\n', '",', '"}'):
        if delimiter in value:
            value = value.split(delimiter, 1)[0]
            break
    return value.strip().strip("\"'")


def _extract_jsonish_list_field(text: str, field_name: str) -> list[str]:
    pattern = rf'(?is)"{re.escape(field_name)}"\s*:\s*(\[[^\]]*\])'
    match = re.search(pattern, text)
    if not match:
        return []
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [to_text(item) for item in payload if to_text(item)]


def _clean_recovered_question_text(text: str) -> str:
    cleaned = text.strip()
    if not cleaned:
        return ""
    cleaned = re.sub(r"(?is)^```(?:json)?\s*", "", cleaned).strip()
    cleaned = re.sub(r"(?is)```$", "", cleaned).strip()
    cleaned = re.sub(r'(?is)^\{\s*"question"\s*:\s*"', "", cleaned).strip()
    cleaned = re.sub(r'(?is)^"?question"?\s*[:：]\s*', "", cleaned).strip()
    cleaned = re.sub(r"(?i)^\*+\s*draft\s*\d+\s*\([^)]*\)\s*:\*+\s*", "", cleaned).strip()
    cleaned = re.sub(r"^[*\-\s`]+", "", cleaned).strip()
    for starter in (
        "Which ",
        "What ",
        "Who ",
        "Where ",
        "When ",
        "Why ",
        "How ",
        "Could you",
        "Can you",
        "Would you",
        "Please ",
        "List ",
        "Identify ",
        "Find ",
        "Show ",
    ):
        index = cleaned.find(starter)
        if index >= 0:
            cleaned = cleaned[index:].strip()
            break
    cleaned = cleaned.strip().strip("\"'").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _parse_spatial_phrases_from_text(value: str) -> list[str]:
    stripped = value.strip()
    if not stripped:
        return []
    if stripped.startswith("[") and stripped.endswith("]"):
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, list):
            return [to_text(item) for item in payload if to_text(item)]
    stripped = stripped.strip("[]")
    if not stripped:
        return []
    parts = re.split(r"\s*,\s*", stripped)
    return [part.strip().strip("\"'") for part in parts if part.strip().strip("\"'")]


def _extract_question_like_text(text: str) -> str:
    stripped = text.strip()
    if not stripped:
        return ""

    jsonish_question = _extract_jsonish_string_field(stripped, "question")
    if jsonish_question:
        return _clean_recovered_question_text(jsonish_question)

    jsonish_partial_question = _extract_jsonish_partial_string_field(stripped, "question")
    if jsonish_partial_question:
        return _clean_recovered_question_text(jsonish_partial_question)

    keyed_question = _extract_keyed_field(stripped, "question")
    if keyed_question:
        return _clean_recovered_question_text(keyed_question)

    lines = [line.strip() for line in stripped.splitlines() if line.strip()]
    for line in lines:
        if line.lower().startswith(("style:", "reasoning_summary:", "reasoning summary:", "spatial_phrases:", "spatial phrases:")):
            continue
        cleaned_line = _clean_recovered_question_text(line)
        if cleaned_line and "?" in cleaned_line:
            prefix, _, _ = cleaned_line.partition("?")
            question = f"{prefix.strip()}?"
            if question and len(question) > 1:
                return question
        if "?" in line:
            prefix, _, _ = line.partition("?")
            question = f"{prefix.strip()}?"
            if question and len(question) > 1:
                return _clean_recovered_question_text(question)

    candidate_text = " ".join(lines).strip()
    if not candidate_text:
        return ""
    candidate_text = re.sub(
        r"(?is)^\s*(here(?:'s| is)\s+(?:the\s+)?(?:json\s+)?(?:response|output)|output|response)\s*[:：-]\s*",
        "",
        candidate_text,
    ).strip()
    if candidate_text.startswith("{") or candidate_text.startswith("["):
        return ""
    return _clean_recovered_question_text(candidate_text)


def _recover_candidate_from_non_json(
    raw_text: str,
    *,
    raw_response: Any = None,
    parse_error: str,
) -> QuestionGenerationCandidate:
    question = _extract_question_like_text(raw_text)
    style = _extract_jsonish_string_field(raw_text, "style") or _extract_keyed_field(raw_text, "style")
    reasoning_summary = (
        _extract_jsonish_string_field(raw_text, "reasoning_summary")
        or _extract_keyed_field(raw_text, "reasoning_summary")
        or _extract_keyed_field(raw_text, "reasoning summary")
    )
    spatial_phrases = _extract_jsonish_list_field(raw_text, "spatial_phrases")
    spatial_field = (
        _extract_keyed_field(raw_text, "spatial_phrases")
        or _extract_keyed_field(raw_text, "spatial phrases")
    )
    return QuestionGenerationCandidate(
        question=question,
        style=style,
        reasoning_summary=reasoning_summary,
        spatial_phrases=spatial_phrases or _parse_spatial_phrases_from_text(spatial_field),
        raw_response_text=raw_text,
        raw_response=stable_jsonify(raw_response),
        parse_error=parse_error,
    )


def parse_question_generation_response(
    response_text: str,
    *,
    raw_response: Any = None,
) -> QuestionGenerationCandidate:
    raw_text = to_text(response_text)
    payload_text = _extract_json_payload(raw_text)
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        return _recover_candidate_from_non_json(
            raw_text,
            raw_response=raw_response,
            parse_error=f"Failed to parse question-generation JSON response: {exc}",
        )
    if not isinstance(payload, Mapping):
        return _recover_candidate_from_non_json(
            raw_text,
            raw_response=raw_response or payload,
            parse_error="Question-generation response must be a JSON object.",
        )
    question = to_text(payload.get("question"))
    if not question:
        return _recover_candidate_from_non_json(
            raw_text,
            raw_response=raw_response or payload,
            parse_error="Question-generation response is missing the 'question' field.",
        )
    spatial_phrases = payload.get("spatial_phrases", [])
    if isinstance(spatial_phrases, str):
        spatial_phrase_list = [spatial_phrases] if spatial_phrases else []
    elif isinstance(spatial_phrases, list):
        spatial_phrase_list = [to_text(item) for item in spatial_phrases if to_text(item)]
    else:
        spatial_phrase_list = []
    return QuestionGenerationCandidate(
        question=question,
        style=to_text(payload.get("style")),
        reasoning_summary=to_text(payload.get("reasoning_summary")),
        spatial_phrases=spatial_phrase_list,
        raw_response_text=raw_text,
        raw_response=stable_jsonify(raw_response or payload),
    )
