"""Tests for program.py — schedule validation, save/load, block finding."""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, time
from pathlib import Path

import pytest

# Ensure app dir is on path
APP_DIR = Path(__file__).resolve().parent.parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import program


@pytest.fixture(autouse=True)
def use_tmp_program_dir(tmp_path, monkeypatch):
    """Redirect program storage to a temp directory for every test."""
    import config

    monkeypatch.setattr(config, "PROGRAM_DIR", str(tmp_path / "programs"))
    monkeypatch.setattr(program, "PROGRAM_DIR", str(tmp_path / "programs"))


# ── save_program validation ──


class TestSaveProgramValidation:
    """Test block validation in save_program."""

    def test_valid_canvas_block(self, tmp_path):
        blocks = [
            {
                "start": "19:00:00",
                "end": "19:30:00",
                "type": "canvas",
                "title": "Test Canvas",
                "html": "<div>Hello</div>",
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert result["date"] == "2026-03-15"
        assert len(result["blocks"]) == 1
        assert result["blocks"][0]["type"] == "canvas"

    def test_valid_playlist_block(self, tmp_path):
        blocks = [
            {
                "start": "20:00:00",
                "end": "21:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert len(result["blocks"]) == 1

    def test_valid_generator_block(self, tmp_path):
        blocks = [
            {
                "start": "20:00:00",
                "end": "20:30:00",
                "type": "generator",
                "title": "Test Generator",
                "name": "seance",
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert result["blocks"][0]["name"] == "seance"

    def test_valid_playlist_with_files(self, tmp_path):
        blocks = [
            {
                "start": "20:00:00",
                "end": "21:00:00",
                "type": "playlist",
                "title": "Test Playlist",
                "files": ["clip_01.mp4", "clip_02.mp4"],
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert result["blocks"][0]["files"] == ["clip_01.mp4", "clip_02.mp4"]

    def test_missing_start(self):
        blocks = [{"end": "19:30:00", "type": "canvas", "html": "<div>Hi</div>"}]
        with pytest.raises(ValueError, match="missing required field 'start'"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_missing_end(self):
        blocks = [{"start": "19:00:00", "type": "canvas", "html": "<div>Hi</div>"}]
        with pytest.raises(ValueError, match="missing required field 'end'"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_missing_type(self):
        blocks = [{"start": "19:00:00", "end": "19:30:00"}]
        with pytest.raises(ValueError, match="missing required field 'type'"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_invalid_type(self):
        blocks = [
            {"start": "19:00:00", "end": "19:30:00", "type": "invalid", "title": "Test"}
        ]
        with pytest.raises(ValueError, match="invalid type 'invalid'"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_canvas_missing_html_and_preset(self):
        blocks = [
            {
                "start": "19:00:00",
                "end": "19:30:00",
                "type": "canvas",
                "title": "Test Canvas",
            }
        ]
        with pytest.raises(ValueError, match="missing 'html' or 'preset' field"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_canvas_with_preset(self):
        blocks = [
            {
                "start": "19:00:00",
                "end": "19:30:00",
                "type": "canvas",
                "title": "Test Canvas",
                "preset": "channel-one-intro",
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert result["blocks"][0]["preset"] == "channel-one-intro"

    def test_canvas_both_html_and_preset(self):
        blocks = [
            {
                "start": "19:00:00",
                "end": "19:30:00",
                "type": "canvas",
                "title": "Test Canvas",
                "html": "<div>Hi</div>",
                "preset": "channel-one-intro",
            },
        ]
        with pytest.raises(ValueError, match="both 'html' and 'preset'"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_canvas_empty_preset_requires_html(self):
        """An empty string preset should still require html."""
        blocks = [
            {
                "start": "19:00:00",
                "end": "19:30:00",
                "type": "canvas",
                "title": "Test Canvas",
                "preset": "",
            },
        ]
        with pytest.raises(ValueError, match="missing 'html' or 'preset' field"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_generator_missing_name(self):
        blocks = [
            {
                "start": "19:00:00",
                "end": "19:30:00",
                "type": "generator",
                "title": "Test Generator",
            }
        ]
        with pytest.raises(ValueError, match="missing 'name' field"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_end_before_start(self):
        blocks = [
            {
                "start": "20:00:00",
                "end": "19:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            }
        ]
        with pytest.raises(ValueError, match="end time.*must be after start"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_end_equals_start(self):
        blocks = [
            {
                "start": "19:00:00",
                "end": "19:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            }
        ]
        with pytest.raises(ValueError, match="end time.*must be after start"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_overlapping_blocks(self):
        blocks = [
            {
                "start": "19:00:00",
                "end": "20:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            },
            {
                "start": "19:30:00",
                "end": "20:30:00",
                "type": "playlist",
                "title": "Test Playlist",
            },
        ]
        with pytest.raises(ValueError, match="Blocks overlap"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_adjacent_blocks_no_overlap(self):
        """Blocks that meet exactly at a boundary should be valid."""
        blocks = [
            {
                "start": "19:00:00",
                "end": "20:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            },
            {
                "start": "20:00:00",
                "end": "21:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert len(result["blocks"]) == 2

    def test_invalid_time_format(self):
        blocks = [
            {
                "start": "19:00",
                "end": "20:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            }
        ]
        with pytest.raises(ValueError, match="invalid time format"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_files_on_non_playlist_block(self):
        blocks = [
            {
                "start": "19:00:00",
                "end": "19:30:00",
                "type": "canvas",
                "title": "Test Canvas",
                "html": "<div>Hi</div>",
                "files": ["clip_01.mp4"],
            },
        ]
        with pytest.raises(ValueError, match="'files' is only valid on 'playlist'"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_empty_files_list(self):
        blocks = [
            {
                "start": "19:00:00",
                "end": "20:00:00",
                "type": "playlist",
                "title": "Test Playlist",
                "files": [],
            },
        ]
        with pytest.raises(ValueError, match="must not be empty"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_files_must_be_list(self):
        blocks = [
            {
                "start": "19:00:00",
                "end": "20:00:00",
                "type": "playlist",
                "title": "Test Playlist",
                "files": "clip_01.mp4",
            },
        ]
        with pytest.raises(ValueError, match="must be a list"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_blocks_sorted_by_start(self):
        """Blocks should be sorted by start time regardless of input order."""
        blocks = [
            {
                "start": "21:00:00",
                "end": "22:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            },
            {
                "start": "19:00:00",
                "end": "20:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert result["blocks"][0]["start"] == "19:00:00"
        assert result["blocks"][1]["start"] == "21:00:00"

    def test_multiple_block_types(self):
        """A program with all three block types should be valid."""
        blocks = [
            {
                "start": "19:00:00",
                "end": "19:15:00",
                "type": "canvas",
                "title": "Test Canvas",
                "html": "<div>Intro</div>",
            },
            {
                "start": "19:15:00",
                "end": "19:45:00",
                "type": "playlist",
                "title": "Test Playlist",
                "files": ["clip_01.mp4"],
            },
            {
                "start": "19:45:00",
                "end": "20:00:00",
                "type": "generator",
                "title": "Test Generator",
                "name": "seance",
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert len(result["blocks"]) == 3


# ── save/load/delete persistence ──


class TestProgramPersistence:
    """Test file I/O for programs."""

    def test_save_creates_file(self, tmp_path):
        blocks = [
            {
                "start": "19:00:00",
                "end": "20:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            }
        ]
        program.save_program(date(2026, 3, 15), blocks)
        path = (
            Path(program.PROGRAM_DIR)
            / "channel-one"
            / "2026"
            / "03"
            / "2026-03-15.json"
        )
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["date"] == "2026-03-15"

    def test_load_returns_saved_program(self, tmp_path):
        blocks = [
            {
                "start": "19:00:00",
                "end": "20:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            }
        ]
        program.save_program(date(2026, 3, 15), blocks)
        loaded = program.load_program(date(2026, 3, 15))
        assert loaded is not None
        assert loaded["date"] == "2026-03-15"
        assert len(loaded["blocks"]) == 1

    def test_load_nonexistent_returns_none(self):
        assert program.load_program(date(2099, 1, 1)) is None

    def test_delete_removes_file(self, tmp_path):
        blocks = [
            {
                "start": "19:00:00",
                "end": "20:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            }
        ]
        program.save_program(date(2026, 3, 15), blocks)
        assert program.delete_program(date(2026, 3, 15)) is True
        assert program.load_program(date(2026, 3, 15)) is None

    def test_delete_nonexistent_returns_false(self):
        assert program.delete_program(date(2099, 1, 1)) is False

    def test_list_programs(self, tmp_path):
        blocks = [
            {
                "start": "19:00:00",
                "end": "20:00:00",
                "type": "playlist",
                "title": "Test Playlist",
            }
        ]
        today = date.today()
        program.save_program(today, blocks)
        result = program.list_programs(days_ahead=3)
        assert result[today.isoformat()] is True


# ── find_current_block ──


class TestFindCurrentBlock:
    """Test block lookup by time."""

    def _make_program(self, blocks):
        return {"date": "2026-03-15", "blocks": blocks}

    def test_finds_matching_block(self):
        prog = self._make_program(
            [
                {
                    "start": "19:00:00",
                    "end": "20:00:00",
                    "type": "playlist",
                    "title": "Test Playlist",
                },
            ]
        )
        block = program.find_current_block(prog, time(19, 30))
        assert block is not None
        assert block["type"] == "playlist"

    def test_returns_none_outside_blocks(self):
        prog = self._make_program(
            [
                {
                    "start": "19:00:00",
                    "end": "20:00:00",
                    "type": "playlist",
                    "title": "Test Playlist",
                },
            ]
        )
        assert program.find_current_block(prog, time(18, 0)) is None
        assert program.find_current_block(prog, time(20, 0)) is None

    def test_start_inclusive_end_exclusive(self):
        prog = self._make_program(
            [
                {
                    "start": "19:00:00",
                    "end": "20:00:00",
                    "type": "playlist",
                    "title": "Test Playlist",
                },
            ]
        )
        assert program.find_current_block(prog, time(19, 0)) is not None
        assert program.find_current_block(prog, time(20, 0)) is None

    def test_multiple_blocks(self):
        prog = self._make_program(
            [
                {
                    "start": "19:00:00",
                    "end": "20:00:00",
                    "type": "playlist",
                    "title": "Test Playlist",
                },
                {
                    "start": "21:00:00",
                    "end": "22:00:00",
                    "type": "canvas",
                    "title": "Test Canvas",
                    "html": "<div>Hi</div>",
                },
            ]
        )
        assert program.find_current_block(prog, time(19, 30))["type"] == "playlist"
        assert program.find_current_block(prog, time(20, 30)) is None
        assert program.find_current_block(prog, time(21, 30))["type"] == "canvas"

    def test_empty_program(self):
        prog = self._make_program([])
        assert program.find_current_block(prog, time(12, 0)) is None


# ── get_block_remaining_seconds ──


class TestBlockRemainingSeconds:
    def test_remaining_calculation(self):
        block = {
            "start": "19:00:00",
            "end": "20:00:00",
            "type": "playlist",
            "title": "Test Playlist",
        }
        now = datetime(2026, 3, 15, 19, 30, 0)
        assert program.get_block_remaining_seconds(block, now) == 1800

    def test_at_block_end_returns_zero(self):
        block = {
            "start": "19:00:00",
            "end": "20:00:00",
            "type": "playlist",
            "title": "Test Playlist",
        }
        now = datetime(2026, 3, 15, 20, 0, 0)
        assert program.get_block_remaining_seconds(block, now) == 0

    def test_past_block_end_returns_zero(self):
        block = {
            "start": "19:00:00",
            "end": "20:00:00",
            "type": "playlist",
            "title": "Test Playlist",
        }
        now = datetime(2026, 3, 15, 20, 30, 0)
        assert program.get_block_remaining_seconds(block, now) == 0


# ── summarize_program ──


class TestSummarizeProgram:
    def test_summary_counts(self):
        prog = {
            "date": "2026-03-15",
            "blocks": [
                {
                    "start": "19:00:00",
                    "end": "19:15:00",
                    "type": "canvas",
                    "title": "Test Canvas",
                    "html": "<div>Hi</div>",
                },
                {
                    "start": "19:15:00",
                    "end": "19:45:00",
                    "type": "playlist",
                    "title": "Test Playlist",
                },
                {
                    "start": "19:45:00",
                    "end": "20:00:00",
                    "type": "generator",
                    "title": "Test Generator",
                    "name": "seance",
                },
            ],
        }
        summary = program.summarize_program(prog)
        assert summary["block_count"] == 3
        assert summary["total_scheduled"] == 3600.0  # 1 hour
        by_type = summary["by_type"]
        assert by_type["canvas"]["count"] == 1
        assert by_type["canvas"]["duration"] == 900.0  # 15 min
        assert by_type["playlist"]["count"] == 1
        assert by_type["playlist"]["duration"] == 1800.0  # 30 min
        assert by_type["generator"]["count"] == 1
        assert by_type["generator"]["duration"] == 900.0  # 15 min


# ══════════════════════════════════════════════════════════════════
# Multi-layer scheduling
# ══════════════════════════════════════════════════════════════════


class TestMultiLayerValidation:
    """Test per-layer overlap validation and layer field handling."""

    def test_different_layers_can_overlap(self):
        """Blocks on different layers CAN overlap in time."""
        blocks = [
            {
                "start": "10:00:00",
                "end": "12:00:00",
                "type": "playlist",
                "title": "Main Content",
                "layer": "input_a",
                "files": ["clip1.mp4"],
            },
            {
                "start": "11:00:00",
                "end": "11:15:00",
                "type": "playlist",
                "title": "Ad Break",
                "layer": "blinder",
                "files": ["ad.mp4"],
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert len(result["blocks"]) == 2

    def test_same_layer_cannot_overlap(self):
        """Blocks on the same layer CANNOT overlap."""
        blocks = [
            {
                "start": "10:00:00",
                "end": "12:00:00",
                "type": "playlist",
                "title": "Show A",
                "layer": "input_b",
                "files": ["clip1.mp4"],
            },
            {
                "start": "11:00:00",
                "end": "13:00:00",
                "type": "playlist",
                "title": "Show B",
                "layer": "input_b",
                "files": ["clip2.mp4"],
            },
        ]
        with pytest.raises(ValueError, match="overlap on layer 'input_b'"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_default_layer_overlap_backward_compat(self):
        """Blocks without layer field default to input_a and check overlaps."""
        blocks = [
            {
                "start": "10:00:00",
                "end": "12:00:00",
                "type": "playlist",
                "title": "Show A",
            },
            {
                "start": "11:00:00",
                "end": "13:00:00",
                "type": "playlist",
                "title": "Show B",
            },
        ]
        with pytest.raises(ValueError, match="overlap on layer 'input_a'"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_invalid_layer(self):
        """Invalid layer name raises ValueError."""
        blocks = [
            {
                "start": "10:00:00",
                "end": "12:00:00",
                "type": "playlist",
                "title": "Bad Layer",
                "layer": "nonexistent",
            },
        ]
        with pytest.raises(ValueError, match="invalid layer"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_playlist_name_validation(self):
        """playlist_name on non-playlist block raises error."""
        blocks = [
            {
                "start": "10:00:00",
                "end": "12:00:00",
                "type": "canvas",
                "title": "Bad",
                "html": "<div>Test</div>",
                "playlist_name": "morning",
            },
        ]
        with pytest.raises(ValueError, match="playlist_name"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_valid_playlist_name_block(self, tmp_path, monkeypatch):
        """Playlist block with playlist_name is valid when playlist exists."""
        import config
        import named_playlist_store

        monkeypatch.setattr(config, "PLAYLIST_DIR", str(tmp_path / "playlists"))

        # Create the named playlist first
        named_playlist_store.save(
            "morning-show",
            [{"source": "/media/clip.mp4", "duration": 60}],
            channel_id="channel-one",
        )

        blocks = [
            {
                "start": "10:00:00",
                "end": "12:00:00",
                "type": "playlist",
                "title": "Named PL",
                "playlist_name": "morning-show",
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert result["blocks"][0]["playlist_name"] == "morning-show"

    def test_playlist_name_not_found(self):
        """Playlist block referencing non-existent playlist raises error."""
        blocks = [
            {
                "start": "10:00:00",
                "end": "12:00:00",
                "type": "playlist",
                "title": "Missing PL",
                "playlist_name": "does-not-exist",
            },
        ]
        with pytest.raises(ValueError, match="does not exist"):
            program.save_program(date(2026, 3, 15), blocks)

    def test_block_loop_field(self):
        """Block with loop=False is saved correctly."""
        blocks = [
            {
                "start": "15:00:00",
                "end": "15:15:00",
                "type": "playlist",
                "title": "Ad Break",
                "layer": "blinder",
                "loop": False,
                "files": ["ad.mp4"],
            },
        ]
        result = program.save_program(date(2026, 3, 15), blocks)
        assert result["blocks"][0]["loop"] is False
        assert result["blocks"][0]["layer"] == "blinder"


class TestFindActiveBlocks:
    """Test find_active_blocks for multi-layer scheduling."""

    def test_single_block(self):
        """Single active block returns a list of one."""
        prog = {
            "blocks": [
                {
                    "start": "10:00:00",
                    "end": "12:00:00",
                    "type": "playlist",
                    "title": "Show",
                },
            ]
        }
        result = program.find_active_blocks(prog, time(11, 0))
        assert len(result) == 1
        assert result[0]["title"] == "Show"

    def test_multiple_blocks_different_layers(self):
        """Multiple blocks on different layers all returned."""
        prog = {
            "blocks": [
                {
                    "start": "10:00:00",
                    "end": "12:00:00",
                    "type": "playlist",
                    "title": "Main",
                    "layer": "input_a",
                },
                {
                    "start": "11:00:00",
                    "end": "11:15:00",
                    "type": "playlist",
                    "title": "Overlay",
                    "layer": "blinder",
                },
            ]
        }
        result = program.find_active_blocks(prog, time(11, 5))
        assert len(result) == 2

    def test_no_active_blocks(self):
        """Outside all block windows returns empty list."""
        prog = {
            "blocks": [
                {
                    "start": "10:00:00",
                    "end": "12:00:00",
                    "type": "playlist",
                    "title": "Show",
                },
            ]
        }
        result = program.find_active_blocks(prog, time(13, 0))
        assert len(result) == 0

    def test_block_boundary_start_inclusive(self):
        """Block start time is inclusive."""
        prog = {
            "blocks": [
                {
                    "start": "10:00:00",
                    "end": "12:00:00",
                    "type": "playlist",
                    "title": "Show",
                },
            ]
        }
        result = program.find_active_blocks(prog, time(10, 0))
        assert len(result) == 1

    def test_block_boundary_end_exclusive(self):
        """Block end time is exclusive."""
        prog = {
            "blocks": [
                {
                    "start": "10:00:00",
                    "end": "12:00:00",
                    "type": "playlist",
                    "title": "Show",
                },
            ]
        }
        result = program.find_active_blocks(prog, time(12, 0))
        assert len(result) == 0
