from unittest.mock import MagicMock, patch

import pytest
from google.genai.errors import APIError

from qbo_pipeline.qa.gemini_retry import (
    generate_content_with_retry,
    retry_delay_hint_seconds,
)


def test_retry_delay_hint_from_message():
    exc = APIError(
        429,
        {
            "error": {
                "message": "Quota. Please retry in 18.2380612s.",
                "code": 429,
                "status": "RESOURCE_EXHAUSTED",
            }
        },
        None,
    )
    assert retry_delay_hint_seconds(exc) == pytest.approx(18.2380612)


def test_retry_delay_none_for_other_codes():
    exc = APIError(400, {"error": {"message": "bad", "code": 400}}, None)
    assert retry_delay_hint_seconds(exc) is None


def test_generate_content_retries_then_succeeds():
    client = MagicMock()
    e429 = APIError(
        429,
        {"error": {"message": "Please retry in 0.01s.", "code": 429}},
        None,
    )
    ok = MagicMock()
    ok.text = "done"
    client.models.generate_content.side_effect = [e429, ok]

    with patch("qbo_pipeline.qa.gemini_retry.time.sleep"):
        out = generate_content_with_retry(
            client, model="gemini-2.5-flash-lite", contents="hi", config=None, max_retries=3
        )
    assert out.text == "done"
    assert client.models.generate_content.call_count == 2


def test_generate_content_exhausts_retries():
    client = MagicMock()
    e429 = APIError(
        429,
        {"error": {"message": "Please retry in 0.01s.", "code": 429}},
        None,
    )
    client.models.generate_content.side_effect = [e429, e429, e429, e429]

    with patch("qbo_pipeline.qa.gemini_retry.time.sleep"):
        with pytest.raises(APIError):
            generate_content_with_retry(
                client,
                model="m",
                contents="x",
                config=None,
                max_retries=3,
            )
    assert client.models.generate_content.call_count == 4
