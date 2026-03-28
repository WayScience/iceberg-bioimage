"""Shared pytest fixtures and helpers."""

import pytest


@pytest.fixture
def greeting_message() -> str:
    return "Hello from iceberg-bioimage."
