from qbo_pipeline.qa.context_window import (
    build_context_prefix,
    normalize_context_turns,
)


def test_normalize_filters_roles_and_caps():
    raw = [
        {"role": "user", "content": "hello"},
        {"role": "system", "content": "ignore"},
        {"role": "assistant", "content": "hi"},
        {"role": "user", "content": ""},
    ]
    out = normalize_context_turns(raw)
    assert len(out) == 2
    assert out[0]["role"] == "user"
    assert out[1]["role"] == "assistant"


def test_build_context_prefix_truncates_from_oldest():
    turns = [
        {"role": "user", "content": "AAA"},
        {"role": "assistant", "content": "BBB"},
        {"role": "user", "content": "CCC"},
        {"role": "assistant", "content": "DDD"},
    ]
    # Budget tighter than full four turns + header/footer → oldest pairs dropped first.
    out = build_context_prefix(turns, max_chars=150, max_messages=10)
    assert "PRIOR CONVERSATION" in out
    assert "DDD" in out
    assert "AAA" not in out


def test_build_context_respects_max_messages():
    turns = [{"role": "user", "content": str(i)} for i in range(10)]
    block = build_context_prefix(turns, max_chars=50_000, max_messages=3)
    assert "0" not in block
    assert "9" in block
