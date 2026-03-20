"""Warehouse Q&A LLM calls: OpenAI (two keys) first, then Gemini fallback."""

from __future__ import annotations

from typing import Literal

from google import genai
from google.genai import types
from openai import OpenAI

from qbo_pipeline.config import WarehouseQaConfig
from qbo_pipeline.qa.gemini_retry import generate_content_with_retry

Task = Literal["planner", "answer_snapshot", "sql_generate", "answer_from_sql"]


def _openai_model(cfg: WarehouseQaConfig, task: Task) -> str:
    if task == "planner":
        return cfg.openai_planner_model
    if task == "sql_generate":
        return cfg.openai_sql_model
    return cfg.openai_model


def _gemini_model(cfg: WarehouseQaConfig, task: Task) -> str:
    if task == "planner":
        return cfg.gemini_planner_model
    if task == "sql_generate":
        return cfg.gemini_sql_model
    return cfg.gemini_model


def _openai_chat(
    api_key: str,
    model: str,
    *,
    system_instruction: str,
    user_content: str,
    temperature: float,
    max_output_tokens: int,
) -> str:
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": user_content},
        ],
        temperature=temperature,
        max_tokens=max_output_tokens,
    )
    text = resp.choices[0].message.content
    return (text or "").strip()


def _gemini_chat(
    api_key: str,
    model: str,
    *,
    system_instruction: str,
    user_content: str,
    temperature: float,
    max_output_tokens: int,
) -> str:
    client = genai.Client(api_key=api_key)
    response = generate_content_with_retry(
        client,
        model=model,
        contents=user_content,
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
        ),
    )
    return (response.text or "").strip()


def complete_qa_llm(
    cfg: WarehouseQaConfig,
    *,
    task: Task,
    system_instruction: str,
    user_content: str,
    temperature: float,
    max_output_tokens: int,
) -> str:
    """
    Try OPENAI_API_KEY_1, then OPENAI_API_KEY_2 (gpt-4 by default), then Gemini.
    """
    o_model = _openai_model(cfg, task)
    g_model = _gemini_model(cfg, task)
    last_exc: BaseException | None = None

    for key in (cfg.openai_api_key_1, cfg.openai_api_key_2):
        if not key:
            continue
        try:
            return _openai_chat(
                key,
                o_model,
                system_instruction=system_instruction,
                user_content=user_content,
                temperature=temperature,
                max_output_tokens=max_output_tokens,
            )
        except Exception as exc:
            last_exc = exc
            continue

    if cfg.gemini_api_key:
        return _gemini_chat(
            cfg.gemini_api_key,
            g_model,
            system_instruction=system_instruction,
            user_content=user_content,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
        )

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("No LLM API keys configured (set OPENAI_API_KEY_1/2 and/or GEMINI_API_KEY)")
