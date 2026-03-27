"""Direct playbin3 gapless playback tests — no engine, no mixer, no RTMP.

Tests playbin3 with custom sink bins (fakesink sync=true) to verify
about-to-finish fires at the correct time and gapless transitions work.

This isolates the playbin3 gapless mechanism from the interpipeline
architecture, mixer, and RTMP output — pure GStreamer testing.
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from pathlib import Path

import pytest

APP_DIR = Path("/app")
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import gi

gi.require_version("Gst", "1.0")
from gi.repository import Gst

if not Gst.is_initialized():
    Gst.init(None)

logger = logging.getLogger(__name__)


def _make(factory: str, name: str) -> Gst.Element:
    elem = Gst.ElementFactory.make(factory, name)
    if elem is None:
        raise RuntimeError(f"GStreamer element '{factory}' not found")
    return elem


# ── Helpers ──


class PlaybackTracker:
    """Track playbin3 about-to-finish signals and stream transitions."""

    def __init__(self):
        self.about_to_finish_times: list[float] = []
        self.stream_start_times: list[float] = []
        self.uris_set: list[str] = []
        self.errors: list[str] = []
        self.start_time: float = 0.0

    def elapsed(self) -> float:
        return time.monotonic() - self.start_time if self.start_time > 0 else 0.0


def _build_playbin3_with_fakesink(
    uri: str,
    tracker: PlaybackTracker,
    entries: list[str],
    entry_index: list[int],
    loop: bool,
    use_clocksync: bool = False,
) -> Gst.Element:
    """Build a playbin3 with fakesink (sync=true) as video/audio sinks.

    Args:
        uri: Initial file URI
        tracker: PlaybackTracker for recording events
        entries: List of file URIs for the playlist
        entry_index: Mutable list [current_index] for the callback
        loop: Whether to loop the playlist
        use_clocksync: If True, add clocksync before fakesink
    """
    pb3 = _make("playbin3", "test_pb3")
    pb3.set_property("uri", uri)
    pb3.set_property("flags", 0x03)  # video + audio only

    # Video sink
    if use_clocksync:
        vsink_bin = Gst.Bin.new("vsinkbin")
        vcs = _make("clocksync", "vcs")
        vfake = _make("fakesink", "vfake")
        vfake.set_property("sync", True)
        vsink_bin.add(vcs)
        vsink_bin.add(vfake)
        vcs.link(vfake)
        vsink_bin.add_pad(Gst.GhostPad.new("sink", vcs.get_static_pad("sink")))
        pb3.set_property("video-sink", vsink_bin)
    else:
        vfake = _make("fakesink", "vfake")
        vfake.set_property("sync", True)
        pb3.set_property("video-sink", vfake)

    # Audio sink
    if use_clocksync:
        asink_bin = Gst.Bin.new("asinkbin")
        acs = _make("clocksync", "acs")
        afake = _make("fakesink", "afake")
        afake.set_property("sync", True)
        asink_bin.add(acs)
        asink_bin.add(afake)
        acs.link(afake)
        asink_bin.add_pad(Gst.GhostPad.new("sink", acs.get_static_pad("sink")))
        pb3.set_property("audio-sink", asink_bin)
    else:
        afake = _make("fakesink", "afake")
        afake.set_property("sync", True)
        pb3.set_property("audio-sink", afake)

    def on_about_to_finish(element):
        elapsed = tracker.elapsed()
        tracker.about_to_finish_times.append(elapsed)

        n = len(entries)
        idx = entry_index[0]
        next_idx = idx + 1

        if next_idx >= n and not loop:
            logger.warning(
                "about-to-finish at %.2fs — playlist done (no loop)", elapsed
            )
            return

        entry_index[0] = next_idx % n
        next_uri = entries[entry_index[0]]
        element.set_property("uri", next_uri)
        tracker.uris_set.append(next_uri)

        logger.warning(
            "about-to-finish at %.2fs — queued entry %d/%d",
            elapsed,
            entry_index[0] + 1,
            n,
        )

    pb3.connect("about-to-finish", on_about_to_finish)
    return pb3


def _run_playbin3(
    pb3: Gst.Element,
    tracker: PlaybackTracker,
    duration: float,
) -> None:
    """Run playbin3 for a given duration, collecting bus messages."""
    errors = []

    def bus_loop():
        bus = pb3.get_bus()
        while tracker.elapsed() < duration + 1:
            msg = bus.timed_pop_filtered(
                100 * Gst.MSECOND,
                Gst.MessageType.ERROR
                | Gst.MessageType.WARNING
                | Gst.MessageType.STREAM_START
                | Gst.MessageType.EOS,
            )
            if msg is None:
                continue
            if msg.type == Gst.MessageType.STREAM_START:
                tracker.stream_start_times.append(tracker.elapsed())
                logger.warning("STREAM_START at %.2fs", tracker.elapsed())
            elif msg.type == Gst.MessageType.EOS:
                logger.warning("EOS at %.2fs", tracker.elapsed())
            elif msg.type == Gst.MessageType.ERROR:
                err, debug = msg.parse_error()
                src = msg.src.get_name() if msg.src else "?"
                error_msg = f"{src}: {err.message}"
                logger.error("ERROR: %s (debug: %s)", error_msg, debug)
                tracker.errors.append(error_msg)
            elif msg.type == Gst.MessageType.WARNING:
                err, _ = msg.parse_warning()
                src = msg.src.get_name() if msg.src else "?"
                logger.warning("GST WARNING: %s: %s", src, err.message)

    bus_thread = threading.Thread(target=bus_loop, daemon=True)

    tracker.start_time = time.monotonic()
    ret = pb3.set_state(Gst.State.PLAYING)
    logger.warning("set_state(PLAYING) returned %s", ret)

    # Wait for state change to actually complete
    state_ret, state, pending = pb3.get_state(5 * Gst.SECOND)
    logger.warning(
        "get_state: ret=%s state=%s pending=%s",
        state_ret,
        Gst.Element.state_get_name(state),
        Gst.Element.state_get_name(pending),
    )

    bus_thread.start()
    time.sleep(duration)

    pb3.set_state(Gst.State.NULL)
    pb3.get_state(3 * Gst.SECOND)


# ── Tests ──


def test_playbin3_exists():
    """playbin3 element factory exists in this GStreamer build."""
    pb3 = Gst.ElementFactory.make("playbin3", "probe")
    assert pb3 is not None, "playbin3 not available"


def test_playbin3_fakesink_basic(test_clips):
    """playbin3 with fakesink sync=true plays a single file without errors."""
    uri = Path(test_clips[0]).as_uri()
    tracker = PlaybackTracker()
    entry_index = [0]

    pb3 = _build_playbin3_with_fakesink(uri, tracker, [uri], entry_index, loop=False)
    _run_playbin3(pb3, tracker, duration=5.0)

    assert len(tracker.errors) == 0, f"Errors: {tracker.errors}"
    assert len(tracker.stream_start_times) >= 1, "No STREAM_START received"


def test_playbin3_about_to_finish_fires(test_clips):
    """about-to-finish fires for a single clip (no clocksync)."""
    uris = [Path(p).as_uri() for p in test_clips]
    tracker = PlaybackTracker()
    entry_index = [0]

    pb3 = _build_playbin3_with_fakesink(
        uris[0], tracker, uris, entry_index, loop=True, use_clocksync=False
    )
    # Without clocksync, clips play at full speed.
    # 3 clips × ~3s each should complete many times in 5s.
    _run_playbin3(pb3, tracker, duration=5.0)

    assert len(tracker.about_to_finish_times) > 0, (
        "about-to-finish never fired! "
        f"stream_starts={len(tracker.stream_start_times)} "
        f"errors={tracker.errors}"
    )
    logger.warning(
        "about-to-finish fired %d times: %s",
        len(tracker.about_to_finish_times),
        [f"{t:.2f}s" for t in tracker.about_to_finish_times],
    )


def test_clocksync_blocks_preroll(test_clips):
    """clocksync in a playbin3 sink bin prevents PLAYING state (documents bug).

    clocksync blocks the streaming thread during preroll, waiting for the
    pipeline clock.  But the clock isn't fully running until PLAYING, which
    requires preroll to complete first.  Deadlock.

    This is WHY we use tee → fakesink(sync=true) instead of clocksync.
    fakesink inherits GstBaseSink which handles preroll correctly.
    """
    uris = [Path(p).as_uri() for p in test_clips]
    tracker = PlaybackTracker()
    entry_index = [0]

    pb3 = _build_playbin3_with_fakesink(
        uris[0], tracker, uris, entry_index, loop=True, use_clocksync=True
    )

    tracker.start_time = time.monotonic()
    pb3.set_state(Gst.State.PLAYING)
    # Pipeline should fail to reach PLAYING within 3 seconds
    state_ret, state, pending = pb3.get_state(3 * Gst.SECOND)

    # Confirm the pipeline is stuck — READY or PAUSED, not PLAYING
    state_name = Gst.Element.state_get_name(state)
    pending_name = Gst.Element.state_get_name(pending)
    logger.warning(
        "clocksync preroll test: state=%s pending=%s", state_name, pending_name
    )

    pb3.set_state(Gst.State.NULL)
    pb3.get_state(2 * Gst.SECOND)

    # The pipeline should NOT have reached PLAYING
    assert state != Gst.State.PLAYING, (
        "Pipeline unexpectedly reached PLAYING with clocksync — "
        "if this passes, clocksync preroll behavior may have changed"
    )


def test_playbin3_gapless_transition_count(test_clips):
    """Multiple gapless transitions complete in the expected time window."""
    uris = [Path(p).as_uri() for p in test_clips]
    tracker = PlaybackTracker()
    entry_index = [0]

    pb3 = _build_playbin3_with_fakesink(
        uris[0], tracker, uris, entry_index, loop=True, use_clocksync=False
    )
    # Without clocksync, clips play at full speed — many transitions in 5s
    _run_playbin3(pb3, tracker, duration=5.0)

    n_transitions = len(tracker.about_to_finish_times)
    logger.warning(
        "Gapless transitions (no clocksync): %d in 5s, stream_starts=%d",
        n_transitions,
        len(tracker.stream_start_times),
    )

    # Should have multiple transitions (clips play fast without sync)
    assert n_transitions >= 2, f"Expected multiple transitions, got {n_transitions}"


def _build_source_pipeline(
    name: str,
    uri: str,
    entries: list[str],
    entry_index: list[int],
    tracker: PlaybackTracker,
    loop: bool,
) -> Gst.Element:
    """Build a full source pipeline: playbin3 → tee → fakesink + inter sinks.

    Matches the real cathode InputLayer topology.
    """
    pb3 = _make("playbin3", f"{name}_pb3")
    pb3.set_property("uri", uri)
    pb3.set_property("flags", 0x03)

    # Video sink bin: tee → fakesink(sync) + intervideosink
    vsink = Gst.Bin.new(f"{name}_vsink")
    vtee = _make("tee", f"{name}_vtee")
    vsq = _make("queue", f"{name}_vsq")
    vsq.set_property("max-size-buffers", 2)
    vsq.set_property("max-size-time", 0)
    vsq.set_property("max-size-bytes", 0)
    vfake = _make("fakesink", f"{name}_vfake")
    vfake.set_property("sync", True)
    viq = _make("queue", f"{name}_viq")
    viq.set_property("max-size-time", 3 * Gst.SECOND)
    viq.set_property("max-size-buffers", 0)
    viq.set_property("max-size-bytes", 0)
    vi = _make("intervideosink", f"{name}_ivsink")
    vi.set_property("channel", f"{name}-video")
    for e in (vtee, vsq, vfake, viq, vi):
        vsink.add(e)
    vtee.link(vsq)
    vsq.link(vfake)
    vtee.link(viq)
    viq.link(vi)
    vsink.add_pad(Gst.GhostPad.new("sink", vtee.get_static_pad("sink")))

    # Audio sink bin: tee → fakesink(sync) + interaudiosink
    asink = Gst.Bin.new(f"{name}_asink")
    atee = _make("tee", f"{name}_atee")
    asq = _make("queue", f"{name}_asq")
    asq.set_property("max-size-buffers", 10)
    asq.set_property("max-size-time", 0)
    asq.set_property("max-size-bytes", 0)
    afake = _make("fakesink", f"{name}_afake")
    afake.set_property("sync", True)
    aiq = _make("queue", f"{name}_aiq")
    aiq.set_property("max-size-time", 3 * Gst.SECOND)
    aiq.set_property("max-size-buffers", 0)
    aiq.set_property("max-size-bytes", 0)
    ai = _make("interaudiosink", f"{name}_iasink")
    ai.set_property("channel", f"{name}-audio")
    for e in (atee, asq, afake, aiq, ai):
        asink.add(e)
    atee.link(asq)
    asq.link(afake)
    atee.link(aiq)
    aiq.link(ai)
    asink.add_pad(Gst.GhostPad.new("sink", atee.get_static_pad("sink")))

    pb3.set_property("video-sink", vsink)
    pb3.set_property("audio-sink", asink)

    def on_about(element):
        elapsed = tracker.elapsed()
        tracker.about_to_finish_times.append(elapsed)
        n = len(entries)
        entry_index[0] = (entry_index[0] + 1) % n
        element.set_property("uri", entries[entry_index[0]])

    pb3.connect("about-to-finish", on_about)
    return pb3


def _build_mixer_pipeline(channel_names: list[str]) -> Gst.Element:
    """Build a mixer pipeline: inter sources → compositor/audiomixer → fakesink.

    Matches the real cathode Mixer topology (minus encoder).
    """
    pipeline = Gst.Pipeline.new("mc_mixer")

    comp = _make("compositor", "mc_comp")
    comp.set_property("background", 1)  # black
    vcaps = _make("capsfilter", "mc_vcaps")
    vcaps.set_property(
        "caps",
        Gst.Caps.from_string("video/x-raw,width=640,height=360,framerate=15/1"),
    )
    vq = _make("queue", "mc_vq")
    vq.set_property("leaky", 2)  # downstream
    vq.set_property("max-size-buffers", 2)
    vout = _make("fakesink", "mc_vout")
    vout.set_property("sync", True)

    amix = _make("audiomixer", "mc_amix")
    aq = _make("queue", "mc_aq")
    aq.set_property("leaky", 2)
    aq.set_property("max-size-buffers", 10)
    aout = _make("fakesink", "mc_aout")
    aout.set_property("sync", True)

    for e in (comp, vcaps, vq, vout, amix, aq, aout):
        pipeline.add(e)
    comp.link(vcaps)
    vcaps.link(vq)
    vq.link(vout)
    amix.link(aq)
    aq.link(aout)

    # Add inter sources for each channel
    for ch in channel_names:
        ivsrc = _make("intervideosrc", f"mc_{ch}_ivsrc")
        ivsrc.set_property("channel", f"{ch}-video")
        ivq = _make("queue", f"mc_{ch}_ivq")
        ivq.set_property("max-size-time", 1 * Gst.SECOND)
        pipeline.add(ivsrc)
        pipeline.add(ivq)
        ivsrc.link(ivq)
        ivq.link(comp)

        iasrc = _make("interaudiosrc", f"mc_{ch}_iasrc")
        iasrc.set_property("channel", f"{ch}-audio")
        iasrc.set_property("buffer-time", 2_000_000_000)
        iaconv = _make("audioconvert", f"mc_{ch}_iaconv")
        iares = _make("audioresample", f"mc_{ch}_iares")
        iacaps = _make("capsfilter", f"mc_{ch}_iacaps")
        iacaps.set_property(
            "caps",
            Gst.Caps.from_string(
                "audio/x-raw,rate=48000,channels=2,format=F32LE,layout=interleaved"
            ),
        )
        iaq = _make("queue", f"mc_{ch}_iaq")
        iaq.set_property("max-size-time", 1 * Gst.SECOND)
        for e in (iasrc, iaconv, iares, iacaps, iaq):
            pipeline.add(e)
        iasrc.link(iaconv)
        iaconv.link(iares)
        iares.link(iacaps)
        iacaps.link(iaq)
        iaq.link(amix)

    return pipeline


def test_playbin3_multichannel_full_stack(test_clips):
    """Full multichannel test: 2 source pipelines → mixer → output.

    Two playbin3 source pipelines (each with tee+fakesink+inter sinks)
    feed a mixer pipeline (compositor + audiomixer) that outputs to
    fakesink.  This is the real cathode architecture end-to-end, minus
    RTMP — proving multiple gapless playbin3 instances work through
    the full interpipeline stack simultaneously.
    """
    uris = [Path(p).as_uri() for p in test_clips]

    # Source A: clips 0,1,2
    tracker_a = PlaybackTracker()
    idx_a = [0]
    src_a = _build_source_pipeline("mc_a", uris[0], uris, idx_a, tracker_a, loop=True)

    # Source B: clips in reverse (2,1,0)
    uris_rev = list(reversed(uris))
    tracker_b = PlaybackTracker()
    idx_b = [0]
    src_b = _build_source_pipeline(
        "mc_b", uris_rev[0], uris_rev, idx_b, tracker_b, loop=True
    )

    # Mixer reads from both channels
    mixer = _build_mixer_pipeline(["mc_a", "mc_b"])

    # Collect mixer bus errors
    mixer_errors = []

    def mixer_bus_loop():
        bus = mixer.get_bus()
        t0 = tracker_a.start_time
        while time.monotonic() - t0 < 12:
            msg = bus.timed_pop_filtered(
                200 * Gst.MSECOND,
                Gst.MessageType.ERROR | Gst.MessageType.WARNING,
            )
            if msg and msg.type == Gst.MessageType.ERROR:
                err, dbg = msg.parse_error()
                mixer_errors.append(f"{msg.src.get_name()}: {err.message}")

    # Start mixer FIRST — interaudiosrc must call start() and write
    # buffer-time to the shared GstInterSurface before any interaudiosink
    # tries to render.  Without this, interaudiosink sees the default
    # buffer-time (too small) and returns GST_FLOW_ERROR.
    mixer.set_state(Gst.State.PLAYING)
    mixer.get_state(5 * Gst.SECOND)
    time.sleep(0.5)  # let inter sources fully initialize

    # Start sources
    tracker_a.start_time = time.monotonic()
    tracker_b.start_time = tracker_a.start_time
    src_a.set_state(Gst.State.PLAYING)
    src_b.set_state(Gst.State.PLAYING)

    bus_thread = threading.Thread(target=mixer_bus_loop, daemon=True)
    bus_thread.start()

    # Run for ~10 seconds — enough for 3+ clip transitions per channel
    time.sleep(10.0)

    # Tear down
    src_a.set_state(Gst.State.NULL)
    src_b.set_state(Gst.State.NULL)
    mixer.set_state(Gst.State.NULL)
    src_a.get_state(3 * Gst.SECOND)
    src_b.get_state(3 * Gst.SECOND)
    mixer.get_state(3 * Gst.SECOND)

    # Verify
    assert len(mixer_errors) == 0, f"Mixer errors: {mixer_errors}"

    assert len(tracker_a.about_to_finish_times) >= 2, (
        f"Source A: only {len(tracker_a.about_to_finish_times)} transitions"
    )
    assert len(tracker_b.about_to_finish_times) >= 2, (
        f"Source B: only {len(tracker_b.about_to_finish_times)} transitions"
    )

    logger.warning(
        "Multichannel full stack: A=%d transitions %s, B=%d transitions %s, "
        "mixer_errors=%d",
        len(tracker_a.about_to_finish_times),
        [f"{t:.2f}s" for t in tracker_a.about_to_finish_times],
        len(tracker_b.about_to_finish_times),
        [f"{t:.2f}s" for t in tracker_b.about_to_finish_times],
        len(mixer_errors),
    )


def test_playbin3_inter_sink_with_tee(test_clips):
    """playbin3 with tee → fakesink(sync) + inter sink.

    This is the correct cathode topology:
        tee → queue → fakesink(sync=true)   [provides clock sync to playsink]
            → queue → intervideosink         [feeds data to mixer]

    fakesink handles preroll correctly (GstBaseSink), so the pipeline
    reaches PLAYING.  Its sync=true provides real-time throttle.
    tee propagates backpressure from fakesink to playsink, making
    about-to-finish fire at the correct time.
    """
    uris = [Path(p).as_uri() for p in test_clips]
    tracker = PlaybackTracker()
    entry_index = [0]

    pb3 = _make("playbin3", "test_pb3")
    pb3.set_property("uri", uris[0])
    pb3.set_property("flags", 0x03)

    # Video sink bin: tee → [fakesink sync=true] + [intervideosink]
    vsink_bin = Gst.Bin.new("vsinkbin")
    vtee = _make("tee", "vtee")
    # Sync branch: fakesink provides clock sync
    vsync_q = _make("queue", "vsync_q")
    vsync_q.set_property("max-size-buffers", 2)
    vsync_q.set_property("max-size-time", 0)
    vsync_q.set_property("max-size-bytes", 0)
    vfake = _make("fakesink", "vfake")
    vfake.set_property("sync", True)
    # Inter branch: feeds mixer via shared memory
    vinter_q = _make("queue", "vinter_q")
    vinter_q.set_property("max-size-time", 3 * Gst.SECOND)
    vinter_q.set_property("max-size-buffers", 0)
    vinter_q.set_property("max-size-bytes", 0)
    v_inter = _make("intervideosink", "ivsink")
    v_inter.set_property("channel", "test-gapless-video")

    for e in (vtee, vsync_q, vfake, vinter_q, v_inter):
        vsink_bin.add(e)
    vtee.link(vsync_q)
    vsync_q.link(vfake)
    vtee.link(vinter_q)
    vinter_q.link(v_inter)
    vsink_bin.add_pad(Gst.GhostPad.new("sink", vtee.get_static_pad("sink")))

    # Audio sink bin: tee → [fakesink sync=true] + [interaudiosink]
    asink_bin = Gst.Bin.new("asinkbin")
    atee = _make("tee", "atee")
    async_q = _make("queue", "async_q")
    async_q.set_property("max-size-buffers", 10)
    async_q.set_property("max-size-time", 0)
    async_q.set_property("max-size-bytes", 0)
    afake = _make("fakesink", "afake")
    afake.set_property("sync", True)
    ainter_q = _make("queue", "ainter_q")
    ainter_q.set_property("max-size-time", 3 * Gst.SECOND)
    ainter_q.set_property("max-size-buffers", 0)
    ainter_q.set_property("max-size-bytes", 0)
    a_inter = _make("interaudiosink", "iasink")
    a_inter.set_property("channel", "test-gapless-audio")

    for e in (atee, async_q, afake, ainter_q, a_inter):
        asink_bin.add(e)
    atee.link(async_q)
    async_q.link(afake)
    atee.link(ainter_q)
    ainter_q.link(a_inter)
    asink_bin.add_pad(Gst.GhostPad.new("sink", atee.get_static_pad("sink")))

    pb3.set_property("video-sink", vsink_bin)
    pb3.set_property("audio-sink", asink_bin)

    def on_about_to_finish(element):
        elapsed = tracker.elapsed()
        tracker.about_to_finish_times.append(elapsed)
        n = len(uris)
        next_idx = (entry_index[0] + 1) % n
        entry_index[0] = next_idx
        element.set_property("uri", uris[next_idx])
        logger.warning(
            "about-to-finish (tee+inter) at %.2fs → entry %d",
            elapsed,
            next_idx,
        )

    pb3.connect("about-to-finish", on_about_to_finish)

    _run_playbin3(pb3, tracker, duration=8.0)

    assert len(tracker.errors) == 0, f"Errors: {tracker.errors}"
    assert len(tracker.about_to_finish_times) > 0, (
        "about-to-finish never fired with tee+inter sink bins! "
        f"stream_starts={len(tracker.stream_start_times)}"
    )
    logger.warning(
        "Tee+inter about-to-finish times: %s",
        [f"{t:.2f}s" for t in tracker.about_to_finish_times],
    )

    pb3.connect("about-to-finish", on_about_to_finish)

    _run_playbin3(pb3, tracker, duration=8.0)

    assert len(tracker.errors) == 0, f"Errors: {tracker.errors}"
    assert len(tracker.about_to_finish_times) > 0, (
        "about-to-finish never fired with inter sink bins! "
        f"stream_starts={len(tracker.stream_start_times)}"
    )
    logger.warning(
        "Inter sink about-to-finish times: %s",
        [f"{t:.2f}s" for t in tracker.about_to_finish_times],
    )
