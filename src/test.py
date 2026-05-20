import requests


def _to_gemini_contents(messages: list[dict]) -> list[dict]:
    contents = []
    for message in messages:
        role = str(message.get("role") or "user").strip().lower()
        text = str(message.get("content") or "").strip()
        if not text:
            continue
        if role not in {"user", "model"}:
            role = "user" if role == "assistant" else role
        if role == "assistant":
            role = "model"
        contents.append(
            {
                "role": role,
                "parts": [{"text": text}],
            }
        )
    return contents


def chat_completion(
    api_key: str,
    model: str,
    messages: list[dict],
    base_url: str = "https://api.sisyphusx.com",
) -> str:
    url = f"{base_url.rstrip('/')}/v1beta/models/{model}:generateContent"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": "curl/8.7.1",
    }

    payload = {
        "contents": _to_gemini_contents(messages),
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    resp.raise_for_status()

    data = resp.json()
    candidates = data.get("candidates") or []
    if not candidates:
        raise ValueError(f"No candidates returned: {data}")
    content = candidates[0].get("content") or {}
    parts = content.get("parts") or []
    text_parts = [str(part.get("text") or "") for part in parts if isinstance(part, dict)]
    return "".join(text_parts).strip()


if __name__ == "__main__":
    result = chat_completion(
        api_key="sk-gUci3TOIX6LBoE5B0fdlHVTgDUdLr53BO9OhqRUHKyuY1EsU",
        model="gemini-3.1-pro-preview",
        messages=[
            {"role": "user", "content": "你好"},
        ],
    )

    print(result)
