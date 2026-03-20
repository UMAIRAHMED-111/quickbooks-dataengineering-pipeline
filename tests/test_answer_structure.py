"""Tests for POST /api/v1/qa structured display payload."""

from qbo_pipeline.qa.answer_structure import structure_qa_response


def test_structure_bullets_and_headline():
    raw = (
        "There are **3** open invoices totaling **$500**.\n\n"
        "- Acme Corp: **$200** due.\n"
        "- Beta LLC: **$300** due."
    )
    out = structure_qa_response(question="Who owes?", answer=raw)
    assert out["question"] == "Who owes?"
    assert out["answer"] == raw
    d = out["display"]
    assert d["format"] == "markdown"
    assert d["headline"] is not None
    assert "3" in d["headline"] or "open invoices" in d["headline"].lower()
    assert len(d["bullets"]) == 2
    assert "Acme" in d["bullets"][0]
    assert "**" in d["markdown"]
    assert "- Acme" in d["markdown"]


def test_structure_plain_paragraph_only():
    raw = "Only one paragraph with no list."
    out = structure_qa_response(question="Q", answer=raw)
    assert out["display"]["bullets"] == []
    assert out["display"]["markdown"] == raw
    assert out["display"]["headline"]


def test_structure_empty():
    out = structure_qa_response(question="Q", answer="   ")
    assert out["answer"] == ""
    assert out["display"]["markdown"] == ""
