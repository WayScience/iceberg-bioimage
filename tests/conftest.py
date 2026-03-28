"""
conftest.py for pytest configuration.
"""

import pytest


@pytest.fixture
def my_data() -> str:
    return "Hello, differently!"
