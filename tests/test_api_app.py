from pathlib import Path

import pytest

from repo_bootstrap import configure_for_checkout

configure_for_checkout(Path(__file__))

from qbo_pipeline.web.app import create_app


@pytest.fixture
def client(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.get_json()["status"] == "ok"


def test_catalog_no_db(client):
    r = client.get("/api/v1/metrics/catalog")
    assert r.status_code == 200
    data = r.get_json()
    assert "endpoints" in data
    assert any("paid-vs-unpaid" in p for p in data["endpoints"])
    assert any("POST /api/v1/qa" in p for p in data["endpoints"])


def test_overview_503_without_database_url(client):
    r = client.get("/api/v1/metrics/overview")
    assert r.status_code == 503
    assert "error" in r.get_json()


def test_sync_401_when_secret_configured(client, monkeypatch):
    monkeypatch.setenv("SYNC_API_SECRET", "expected-token")
    r = client.post("/api/v1/sync")
    assert r.status_code == 401


def test_sync_invokes_background_sync_when_authed(client, monkeypatch):
    monkeypatch.setenv("SYNC_API_SECRET", "expected-token")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost/db")
    monkeypatch.setenv("N8N_WEBHOOK_URL", "http://example.invalid/webhook")
    called: dict = {}

    def fake_start_sync_run(settings):
        called["started"] = True
        return "sync-uuid-123"

    def fake_continue_sync_run(settings, sync_id, *, local_path=None):
        called["continued_sync_id"] = sync_id
        called["local_path"] = local_path

    def fake_schedule(job, *, sync_id):
        called["scheduled_sync_id"] = sync_id
        job()

    monkeypatch.setattr("qbo_pipeline.web.app.start_sync_run", fake_start_sync_run)
    monkeypatch.setattr(
        "qbo_pipeline.web.app.continue_sync_run",
        fake_continue_sync_run,
    )
    monkeypatch.setattr("qbo_pipeline.web.app._schedule_background_sync", fake_schedule)
    r = client.post(
        "/api/v1/sync",
        headers={"Authorization": "Bearer expected-token"},
    )
    assert r.status_code == 202
    assert r.get_json()["sync_run_id"] == "sync-uuid-123"
    assert called["started"] is True
    assert called["scheduled_sync_id"] == "sync-uuid-123"
    assert called["continued_sync_id"] == "sync-uuid-123"
    assert called["local_path"] is None


def test_qa_400_empty_question(client):
    r = client.post("/api/v1/qa", json={"question": "   "})
    assert r.status_code == 400
    assert r.get_json()["error"] == "invalid_request"


def test_qa_400_missing_json(client):
    r = client.post(
        "/api/v1/qa",
        data="not-json",
        content_type="text/plain",
    )
    assert r.status_code == 400


def test_qa_503_without_config(client):
    r = client.post("/api/v1/qa", json={"question": "How many customers?"})
    assert r.status_code == 503
    body = r.get_json()
    assert body["error"] == "service_unavailable"


def test_qa_forwards_context_to_answer_question(client, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost/db")
    monkeypatch.setenv("OPENAI_API_KEY_1", "fake-key")
    captured: dict = {}

    def fake_answer(cfg, question, *, context=None):
        captured["question"] = question
        captured["context"] = context
        return "Ack."

    monkeypatch.setattr("qbo_pipeline.web.app.answer_question", fake_answer)
    r = client.post(
        "/api/v1/qa",
        json={
            "question": "And last month?",
            "context": [
                {"role": "user", "content": "Payments this month?"},
                {"role": "assistant", "content": "$100"},
            ],
        },
    )
    assert r.status_code == 200
    assert captured["question"] == "And last month?"
    assert captured["context"] is not None
    assert len(captured["context"]) == 2


def test_qa_200_mocked(client, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost/db")
    monkeypatch.setenv("OPENAI_API_KEY_1", "fake-openai-key-for-test")

    def fake_answer(cfg, question, *, context=None):
        assert "unpaid" in question
        return "There are 3 unpaid invoices."

    monkeypatch.setattr(
        "qbo_pipeline.web.app.answer_question",
        fake_answer,
    )
    r = client.post("/api/v1/qa", json={"question": "List unpaid stuff"})
    assert r.status_code == 200
    data = r.get_json()
    assert data["answer"] == "There are 3 unpaid invoices."
    assert data["question"] == "List unpaid stuff"
    assert "display" in data
    assert data["display"]["format"] == "markdown"
    assert "markdown" in data["display"]


def test_sync_local_file_json_body(client, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost/db")
    monkeypatch.setenv("N8N_WEBHOOK_URL", "http://example.invalid/webhook")
    called: dict = {}

    def fake_start_sync_run(settings):
        return "sync-uuid-456"

    def fake_continue_sync_run(settings, sync_id, *, local_path=None):
        called["sync_id"] = sync_id
        called["local_path"] = local_path

    def fake_schedule(job, *, sync_id):
        called["scheduled_sync_id"] = sync_id
        job()

    monkeypatch.setattr("qbo_pipeline.web.app.start_sync_run", fake_start_sync_run)
    monkeypatch.setattr(
        "qbo_pipeline.web.app.continue_sync_run",
        fake_continue_sync_run,
    )
    monkeypatch.setattr("qbo_pipeline.web.app._schedule_background_sync", fake_schedule)
    r = client.post(
        "/api/v1/sync",
        json={"local_file": "data/sample.json"},
    )
    assert r.status_code == 202
    assert r.get_json()["sync_run_id"] == "sync-uuid-456"
    assert called["scheduled_sync_id"] == "sync-uuid-456"
    assert called["sync_id"] == "sync-uuid-456"
    assert called["local_path"] == "data/sample.json"
