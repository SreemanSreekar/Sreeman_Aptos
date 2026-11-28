"""Pytest configuration for the standalone verification harness.

Ensures the 100K chaos replay suite (`test_100k_chaos_replay.py`) always runs last
when invoking `pytest harness`, preventing alphabetical discovery from running the
100K workload ahead of the smaller isolated suites.
"""

from __future__ import annotations

from typing import Any


def pytest_collection_modifyitems(items: list[Any]) -> None:
    """Place test_100k_chaos_replay.py items at the end of any harness collection."""
    regular = []
    chaos = []
    for item in items:
        nodeid = getattr(item, "nodeid", "")
        if "test_100k_chaos_replay.py" in nodeid:
            chaos.append(item)
        else:
            regular.append(item)
    items[:] = regular + chaos
