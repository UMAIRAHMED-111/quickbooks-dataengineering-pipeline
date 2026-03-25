"""Detect conversation starters / chit-chat so Q&A skips warehouse + LLM for obvious non-queries."""

from __future__ import annotations

import re

# If the user is likely asking for numbers or entities from the warehouse, do not treat as small talk.
_DATA_INTENT = re.compile(
    r"\d|[$€£]|"
    r"\b(invoice|invoices|payment|payments|pay|paid|unpaid|owe|owing|debt|"
    r"customer|customers|client|clients|balance|balances|total|totals|sum|amount|amounts|"
    r"how\s+many|how\s+much|revenue|ar|ap|quickbooks|qbo|sync|snapshot|warehouse|"
    r"this\s+month|last\s+month|mtd|ytd|quarter|week|days?|year|overdue|due(\s+date)?|"
    r"e-?mail|sent|unsent|deliver|open\s+invoice|aging|statement)\b",
    re.I,
)

_REPLY_INTRO = (
    "Hi! I focus on your **synced QuickBooks data**—invoices, payments, customers, and balances. "
    "Try something like **how much we received this month** or **who has the largest unpaid balance**."
)


def try_small_talk_reply(question: str) -> str | None:
    """
    If the message is clearly not a warehouse question, return a short canned reply.

    Returns ``None`` when the message should go through normal Q&A (snapshot / dynamic SQL + LLM).
    """
    q = (question or "").strip()
    if not q:
        return None
    if len(q) > 160:
        return None
    if _DATA_INTENT.search(q):
        return None

    low = q.lower()
    core = low.rstrip("!.?…").strip()

    if re.fullmatch(r"(hi|hello|hey|howdy|yo|hiya)(\s+there)?", core):
        return _REPLY_INTRO

    if re.fullmatch(r"good\s+(morning|afternoon|evening|day|night)", core):
        return _REPLY_INTRO

    if re.fullmatch(r"(thanks?|thank\s+you|thx|ty|much\s+appreciated)(\s+(!+|you|so\s+much))?", core):
        return (
            "You’re welcome! If you need another angle on invoices, payments, or customers, just ask."
        )

    if re.fullmatch(r"(bye|goodbye|see\s+you|cya|ttyl)(\s+(!+)?)?", core):
        return "Take care! Open this chat anytime you want to explore your QuickBooks numbers."

    if re.fullmatch(r"(ok|okay|k|cool|nice|great|sounds\s+good)(\s+!+)?", core):
        return (
            "Glad that helps. When you’re ready, ask a question about your **invoices**, **payments**, "
            "or **customers** and I’ll use the latest sync."
        )

    if re.fullmatch(
        r"(help|\?|what\s+can\s+you(\s+do)?|what\s+do\s+you\s+do|capabilities|who\s+are\s+you)",
        core,
    ):
        return _REPLY_INTRO

    return None
