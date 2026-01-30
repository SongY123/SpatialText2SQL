import json
import os
import re
import urllib.error
import urllib.request
from typing import Dict, List, Optional


DEFAULT_MODEL = "qwen3-coder:30b-a3b-fp16"
DEFAULT_API_BASE = "http://10.132.80.118:11434/v1"
DEFAULT_API_KEY = "EMPTY"

def load_dotenv(dotenv_path: str) -> None:
    if not dotenv_path or not os.path.exists(dotenv_path):
        return

    with open(dotenv_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("export "):
                line = line[7:].lstrip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key or key in os.environ:
                continue
            if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            value = value.strip().strip("`")
            os.environ[key] = value


def format_schema(schema: Dict) -> str:
    schema_str = "Database Schema:\n"
    for table_name, table_info in schema.items():
        schema_str += f"\nTable: {table_name}\n"
        cols = table_info.get("columns") or []
        if cols:
            schema_str += "Columns:\n"
            for col in cols:
                schema_str += f"  - {col.get('name','')} ({col.get('type','')})\n"
    return schema_str


def format_sample_data(sample_data: Dict) -> str:
    sample_str = "\nSample Data:\n"
    for table_name, rows in (sample_data or {}).items():
        if not rows:
            continue
        sample_str += f"\nTable: {table_name}\n"
        for i, row in enumerate(rows[:3]):
            sample_str += f"Row {i+1}: {row}\n"
        if len(rows) > 3:
            sample_str += f"... (showing 3 of {len(rows)} rows)\n"
    return sample_str


def build_prompt(question: str, schema: Dict, sample_data: Optional[Dict] = None) -> str:
    prompt = (
        "You are a SQL expert for PostgreSQL.\n"
        "Given a question, database schema, and sample data, write ONE SQL query.\n"
        "Return only the SQL query. Do not include explanations.\n\n"
    )
    prompt += format_schema(schema)
    if sample_data:
        prompt += format_sample_data(sample_data)
    prompt += f"\nQuestion: {question.strip()}\nSQL:"
    return prompt


def clean_sql(text: str) -> str:
    s = text.strip()
    fenced = re.search(r"```(?:sql)?\s*([\s\S]*?)\s*```", s, flags=re.IGNORECASE)
    if fenced:
        s = fenced.group(1).strip()
    s = re.sub(r"^\s*(SQL\s*:)\s*", "", s, flags=re.IGNORECASE).strip()
    lines: List[str] = []
    for line in s.splitlines():
        if re.match(r"^\s*(Explanation|Reasoning|Notes?)\s*:", line, flags=re.IGNORECASE):
            break
        lines.append(line)
    s = "\n".join(lines).strip()
    if not s:
        return ""
    if ";" in s:
        first = s.split(";", 1)[0].strip()
        return first + ";" if first else ""
    return s


def chat_completions(
    messages: List[Dict[str, str]],
    model: str,
    api_base: str,
    api_key: str,
    temperature: float,
    max_tokens: int,
    timeout_seconds: int,
) -> str:
    url = api_base.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {api_key}")

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if e.fp else str(e)
        raise RuntimeError(f"LLM API HTTPError: {e.code} {raw}") from e
    except Exception as e:
        raise RuntimeError(f"LLM API request failed: {e}") from e

    obj = json.loads(raw)
    return obj["choices"][0]["message"]["content"]

def normalize_api_base(api_base: str) -> str:
    base = api_base.strip().strip("`").strip("'").strip('"').rstrip("/")
    if base.endswith("/v1"):
        return base
    return base + "/v1"


def generate_sql(
    question: str,
    schema: Dict,
    sample_data: Optional[Dict] = None,
    model: str = DEFAULT_MODEL,
    api_base: str = DEFAULT_API_BASE,
    api_key: str = DEFAULT_API_KEY,
    temperature: float = 0.0,
    max_tokens: int = 512,
    timeout_seconds: int = 120,
) -> str:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    load_dotenv(os.path.join(repo_root, ".env"))

    api_base = normalize_api_base(os.environ.get("OPENAI_API_BASE", api_base))
    api_key = os.environ.get("OPENAI_API_KEY", api_key).strip().strip("'").strip('"')
    model = os.environ.get("MODEL", model).strip().strip("'").strip('"')

    prompt = build_prompt(question=question, schema=schema, sample_data=sample_data)
    content = chat_completions(
        messages=[{"role": "user", "content": prompt}],
        model=model,
        api_base=api_base,
        api_key=api_key,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout_seconds=timeout_seconds,
    )
    return clean_sql(content)
