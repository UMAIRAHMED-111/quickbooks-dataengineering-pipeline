"""Turn raw LLM Q&A text into structured JSON for nicer API / UI rendering."""

from __future__ import annotations

import re
from typing import Any

_BULLET_LINE = re.compile(r"^\s*(?:[-*•]|\d+\.)\s+(.+)$")


def _split_blocks(text: str) -> tuple[list[str], list[str]]:
    """Split answer into bullet items and paragraph blocks (order not preserved across types)."""
    bullets: list[str] = []
    paragraphs: list[str] = []
    for block in re.split(r"\n\s*\n+", text.strip()):
        lines = [ln.rstrip() for ln in block.split("\n") if ln.strip()]
        if not lines:
            continue
        if all(_BULLET_LINE.match(ln) for ln in lines):
            for ln in lines:
                m = _BULLET_LINE.match(ln)
                if m:
                    bullets.append(m.group(1).strip())
        else:
            paragraphs.append("\n".join(lines))
    return bullets, paragraphs


def _first_sentence(text: str) -> str:
    plain = re.sub(r"\*\*([^*]+)\*\*", r"\1", text).strip()
    if not plain:
        return ""
    # One sentence if reasonably short; avoid cutting mid-number
    m = re.match(r"^(.{1,280}?[.!?])(?:\s|$)", plain, re.DOTALL)
    if m:
        return m.group(1).strip()
    line = plain.split("\n", 1)[0].strip()
    if len(line) <= 220:
        return line
    return line[:217].rsplit(" ", 1)[0] + "…"


def _compose_markdown(
    raw: str,
    headline: str | None,
    bullets: list[str],
    paragraphs: list[str],
) -> str:
    """Canonical markdown when there are bullets; otherwise preserve model spacing."""
    raw_clean = re.sub(r"\n{3,}", "\n\n", raw.strip())
    if not bullets:
        return raw_clean

    parts: list[str] = []
    if headline:
        parts.append(f"**{headline}**")
    parts.append("")
    parts.extend(f"- {b}" for b in bullets)
    rest = paragraphs[1:] if len(paragraphs) > 1 else []
    if rest:
        parts.append("")
        parts.append("\n\n".join(rest))
    return "\n".join(parts).strip()


def structure_qa_response(*, question: str, answer: str) -> dict[str, Any]:
    """
    Build API payload: original ``answer`` plus ``display`` for headlines, lists, markdown.

    CLI / backwards compatibility: consumers can keep using top-level ``answer`` only.
    """
    raw = (answer or "").strip()
    if not raw:
        return {
            "question": question,
            "answer": "",
            "display": {
                "format": "markdown",
                "markdown": "",
                "headline": None,
                "bullets": [],
                "paragraphs": [],
            },
        }

    bullets, paragraphs = _split_blocks(raw)
    headline: str | None = None
    if paragraphs:
        headline = _first_sentence(paragraphs[0]) or None
        if headline and len(headline) > 240:
            headline = headline[:237] + "…"

    markdown = _compose_markdown(raw, headline, bullets, paragraphs)

    return {
        "question": question,
        "answer": raw,
        "display": {
            "format": "markdown",
            "markdown": markdown,
            "headline": headline,
            "bullets": bullets,
            "paragraphs": paragraphs,
        },
    }
