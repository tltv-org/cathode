# ── Base: runtime image with GStreamer ──
#
# Single Dockerfile for cathode.  Three targets:
#   slim  — minimal: GStreamer + ffmpeg + PyGObject (no plugin system deps)
#   full  — adds WPE WebKit, numpy, Pillow (all source plugins work)
#   test  — adds pytest on top of full
#
# Build: docker build --target slim -t cathode:slim .
#        docker build --target full -t cathode:latest .
#        docker build --target test -t cathode:test .
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    # ffmpeg (ffprobe, failover generation)
    ffmpeg \
    fonts-dejavu-core \
    tzdata \
    # GStreamer core + plugins
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    # GStreamer RTMP support (in plugins-bad, needs librtmp)
    libgstreamer-plugins-bad1.0-0 \
    # Python GObject introspection bindings (system packages)
    python3-gi \
    gir1.2-gstreamer-1.0 \
    gir1.2-gst-plugins-base-1.0 \
    # Text overlay dependencies (pango + cairo)
    gstreamer1.0-x \
    # PyGObject build deps (needed to pip install for Docker's Python)
    libgirepository-2.0-dev \
    gir1.2-girepository-2.0-dev \
    libcairo2-dev \
    pkg-config \
    python3-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install PyGObject via pip (system python3-gi targets system Python,
# not Docker's /usr/local/bin/python3)
RUN pip install --no-cache-dir PyGObject \
    && apt-get purge -y --auto-remove gcc python3-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY app/ .
RUN mkdir -p /app/plugins

EXPOSE 8000

# ── Slim: minimal image (no optional plugin system deps) ──
FROM base AS slim

CMD ["python3", "main.py"]

# ── Full: adds system deps for source/content plugins ──
FROM base AS full

RUN apt-get update && apt-get install -y --no-install-recommends \
    # WPE WebKit for html-source plugin (wpesrc)
    gstreamer1.0-wpe \
    libgles2 \
    libegl1 \
    libegl-mesa0 \
    libgl1-mesa-dri \
    # numpy + Pillow for script-source plugin (Python frame generation)
    python3-numpy \
    python3-pil \
    && rm -rf /var/lib/apt/lists/*

# Set WPE env vars (required for headless operation in Docker)
ENV WEBKIT_DISABLE_SANDBOX_THIS_IS_DANGEROUS=1
ENV LIBGL_ALWAYS_SOFTWARE=1

CMD ["python3", "main.py"]

# ── Test: adds test deps on top of full ──
FROM full AS test

COPY app/requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt
