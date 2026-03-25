"""Trim and format prior Q&A turns so prompts stay within a practical context budget."""

from __future__ import annotations

from typing import Any

# Hard cap per message to avoid abuse / runaway prompts
_PER_MESSAGE_CAP = 8_000


def normalize_context_turns(raw: Any) -> list[dict[str, str]]:
    """Parse API/JSON into validated ``[{role, content}, ...]`` (user | assistant only)."""
    if not isinstance(raw, list):
        return []
    out: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        role = item.get("role")
        content = item.get("content")
        if not isinstance(role, str) or not isinstance(content, str):
            continue
        r = role.strip().lower()
        if r not in ("user", "assistant"):
            continue
        c = content.strip()
        if not c:
            continue
        if len(c) > _PER_MESSAGE_CAP:
            c = c[: _PER_MESSAGE_CAP - 1] + "…"
        out.append({"role": r, "content": c})
    return out


def build_context_prefix(
    turns: list[dict[str, str]],
    *,
    max_chars: int,
    max_messages: int,
) -> str:
    """
    Oldest messages are dropped first until under ``max_chars`` (after ``max_messages`` tail slice).

    The block is clearly delimited so the model treats it as **dialogue only**, not warehouse facts.
    """
    if not turns or max_chars <= 0:
        return ""
    tail = turns[-max(0, max_messages) :]
    blocks: list[str] = []
    for t in tail:
        label = "User" if t["role"] == "user" else "Assistant"
        blocks.append(f"{label}: {t['content']}")

    while blocks:
        body = "\n\n".join(blocks)
        full = (
            "--- PRIOR CONVERSATION (follow-up context only; figures here may be outdated) ---\n"
            f"{body}\n"
            "--- END PRIOR CONVERSATION ---\n\n"
        )
        if len(full) <= max_chars:
            return full
        blocks.pop(0)
    return ""
