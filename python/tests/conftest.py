// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
"""Shared fixtures for revoq Python binding tests."""
import tempfile
import os
import pytest
import revoq


@pytest.fixture
def tmp_journal(tmp_path):
    """Return a (path, location, dest_id) tuple on a fresh temp directory."""
    dest_id = 1
    location = revoq.make_location(
        path=str(tmp_path),
        mode="live",
        category="executiondata",
        group="test",
        name="pytest",
    )
    return str(tmp_path), location, dest_id
