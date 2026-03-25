from qbo_pipeline.qa.warehouse_qa import _sanitize_qa_answer_text


def test_sanitize_replaces_query_no_rows():
    raw = "The query returned no rows."
    out = _sanitize_qa_answer_text(raw)
    assert "query" not in out.lower()
    assert "no rows" not in out.lower()
    assert "this month" in out.lower() or "synced" in out.lower()


def test_sanitize_passes_through_normal_answer():
    raw = "We have received a total of **$4752.62** in payments."
    assert _sanitize_qa_answer_text(raw) == raw
