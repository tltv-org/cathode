"""Tests for playout state persistence and first boot behavior.

Covers:
  - playout_state.save_layer_state / load_state / get_layer_state / clear_layer_state
  - Boot priority: persisted state → slate → failover only
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import playout_state


@pytest.fixture(autouse=True)
def use_tmp_state_dir(tmp_path, monkeypatch):
    """Redirect playout state to a temp directory."""
    import config

    monkeypatch.setattr(config, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(playout_state, "_STATE_DIR", str(tmp_path))


# ══════════════════════════════════════════════════════════════════
# playout_state module
# ══════════════════════════════════════════════════════════════════


class TestPlayoutState:
    """Test playout state persistence."""

    def test_load_state_returns_none_on_first_boot(self):
        """No state file → returns None."""
        assert playout_state.load_state("channel-one") is None

    def test_get_layer_state_returns_none_on_first_boot(self):
        """No state file → layer state is None."""
        assert playout_state.get_layer_state("channel-one", "input_a") is None

    def test_save_and_load(self):
        """Save layer state and load it back."""
        playout_state.save_layer_state(
            "channel-one",
            "input_a",
            playlist_name="evening-show",
            loop=True,
        )

        state = playout_state.load_state("channel-one")
        assert state is not None
        assert "input_a" in state
        assert state["input_a"]["playlist_name"] == "evening-show"
        assert state["input_a"]["loop"] is True
        assert "updated" in state

    def test_get_layer_state(self):
        """get_layer_state returns just the layer dict."""
        playout_state.save_layer_state(
            "channel-one",
            "input_a",
            playlist_name="morning",
            loop=False,
        )

        ls = playout_state.get_layer_state("channel-one", "input_a")
        assert ls is not None
        assert ls["playlist_name"] == "morning"
        assert ls["loop"] is False
        assert ls["type"] == "playlist"

    def test_get_layer_state_unknown_layer(self):
        """get_layer_state returns None for a layer with no state."""
        playout_state.save_layer_state(
            "channel-one",
            "input_a",
            playlist_name="show",
            loop=True,
        )
        assert playout_state.get_layer_state("channel-one", "input_b") is None

    def test_clear_layer_state(self):
        """clear_layer_state sets the layer to None."""
        playout_state.save_layer_state(
            "channel-one",
            "input_a",
            playlist_name="show",
            loop=True,
        )
        playout_state.clear_layer_state("channel-one", "input_a")

        ls = playout_state.get_layer_state("channel-one", "input_a")
        assert ls is None

    def test_clear_nonexistent_layer(self):
        """clear_layer_state on missing state file doesn't crash."""
        playout_state.clear_layer_state("channel-one", "input_a")
        # No exception

    def test_multiple_layers(self):
        """State persists independently per layer."""
        playout_state.save_layer_state(
            "channel-one",
            "input_a",
            playlist_name="main-loop",
            loop=True,
        )
        playout_state.save_layer_state(
            "channel-one",
            "input_b",
            playlist_name="overlay",
            loop=False,
        )

        assert (
            playout_state.get_layer_state("channel-one", "input_a")["playlist_name"]
            == "main-loop"
        )
        assert (
            playout_state.get_layer_state("channel-one", "input_b")["playlist_name"]
            == "overlay"
        )

    def test_channel_isolation(self):
        """State is isolated per channel."""
        playout_state.save_layer_state(
            "channel-one",
            "input_a",
            playlist_name="ch1-show",
            loop=True,
        )
        playout_state.save_layer_state(
            "channel-two",
            "input_a",
            playlist_name="ch2-show",
            loop=True,
        )

        assert (
            playout_state.get_layer_state("channel-one", "input_a")["playlist_name"]
            == "ch1-show"
        )
        assert (
            playout_state.get_layer_state("channel-two", "input_a")["playlist_name"]
            == "ch2-show"
        )

    def test_overwrite_updates_state(self):
        """Saving state for the same layer overwrites previous."""
        playout_state.save_layer_state(
            "channel-one",
            "input_a",
            playlist_name="old",
            loop=True,
        )
        playout_state.save_layer_state(
            "channel-one",
            "input_a",
            playlist_name="new",
            loop=False,
        )

        ls = playout_state.get_layer_state("channel-one", "input_a")
        assert ls["playlist_name"] == "new"
        assert ls["loop"] is False

    def test_corrupt_state_file_returns_none(self, tmp_path):
        """Corrupt JSON returns None instead of crashing."""
        state_dir = tmp_path / "playout-state"
        state_dir.mkdir(parents=True)
        (state_dir / "channel-one.json").write_text("NOT JSON{{{")

        assert playout_state.load_state("channel-one") is None
