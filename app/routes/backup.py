"""Backup and restore endpoints — /api/backup, /api/restore.

Backup creates a tar.gz archive of all cathode configuration and state.
Restore accepts an uploaded archive and replaces the current state.
Keys (Ed25519 private keys) are included in backups — handle with care.
"""

from __future__ import annotations

import io
import logging
import os
import tarfile
import tempfile
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, UploadFile
from starlette.responses import StreamingResponse

import config

logger = logging.getLogger(__name__)

router = APIRouter(tags=["backup"])

# Directories to include in backup, relative label → absolute path.
# Keys are included (operator responsibility to secure the archive).
_BACKUP_DIRS = {
    "keys": config.KEY_DIR,
    "programs": config.PROGRAM_DIR,
    "playlists": config.PLAYLIST_DIR,
    "playout-state": config.PLAYOUT_STATE_DIR,
    "seq": config.SEQ_DIR,
    "tokens": config.TOKEN_DIR,
    "migrations": config.MIGRATION_DIR,
}

_BACKUP_FILES = {
    "peers.json": config.PEER_FILE,
    "relays.json": config.RELAY_FILE,
}

_BACKUP_CONFIG_DIR = config.CHANNEL_CONFIG_DIR


@router.post("/api/backup")
async def create_backup():
    """Create a tar.gz archive of all cathode state and configuration.

    Returns a downloadable archive containing keys, programs, playlists,
    playout state, peer/relay config, tokens, and channel YAML files.
    """
    buf = io.BytesIO()

    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        # Add data directories
        for label, dirpath in _BACKUP_DIRS.items():
            dirpath = str(dirpath)
            if os.path.isdir(dirpath):
                tar.add(dirpath, arcname=label)

        # Add standalone data files
        for label, filepath in _BACKUP_FILES.items():
            filepath = str(filepath)
            if os.path.isfile(filepath):
                tar.add(filepath, arcname=label)

        # Add channel config YAML files
        config_dir = str(_BACKUP_CONFIG_DIR)
        if os.path.isdir(config_dir):
            tar.add(config_dir, arcname="channels")

    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/gzip",
        headers={
            "Content-Disposition": "attachment; filename=cathode-backup.tar.gz",
        },
    )


@router.post("/api/restore")
async def restore_backup(file: UploadFile = File(...)):
    """Restore cathode state from an uploaded tar.gz archive.

    Extracts the archive over the current data directories.  Returns a
    list of restored sections and whether the engine was restarted.
    """
    if not file.filename or not (
        file.filename.endswith(".tar.gz") or file.filename.endswith(".tgz")
    ):
        raise HTTPException(400, "Expected a .tar.gz archive")

    content = await file.read()
    if len(content) > 100 * 1024 * 1024:  # 100MB limit
        raise HTTPException(400, "Archive too large (max 100MB)")

    restored: list[str] = []
    engine_restarted = False

    try:
        buf = io.BytesIO(content)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            # Security: reject paths with .. or absolute paths
            for member in tar.getmembers():
                if member.name.startswith("/") or ".." in member.name:
                    raise HTTPException(400, f"Unsafe path in archive: {member.name}")

            members = tar.getnames()

            # Restore data directories
            for label, dirpath in _BACKUP_DIRS.items():
                dir_members = [
                    m
                    for m in tar.getmembers()
                    if m.name.startswith(label + "/") or m.name == label
                ]
                if dir_members:
                    os.makedirs(dirpath, exist_ok=True)
                    # Extract with path rewriting
                    for member in dir_members:
                        if member.isfile():
                            rel = member.name[len(label) :].lstrip("/")
                            if not rel:
                                continue
                            dest = os.path.join(dirpath, rel)
                            os.makedirs(os.path.dirname(dest), exist_ok=True)
                            f = tar.extractfile(member)
                            if f:
                                with open(dest, "wb") as out:
                                    out.write(f.read())
                    restored.append(label)

            # Restore standalone files
            for label, filepath in _BACKUP_FILES.items():
                if label in members:
                    f = tar.extractfile(label)
                    if f:
                        os.makedirs(os.path.dirname(filepath), exist_ok=True)
                        with open(filepath, "wb") as out:
                            out.write(f.read())
                        restored.append(label.replace(".json", ""))

            # Restore channel configs
            channel_members = [
                m
                for m in tar.getmembers()
                if m.name.startswith("channels/") and m.isfile()
            ]
            if channel_members:
                config_dir = str(_BACKUP_CONFIG_DIR)
                try:
                    os.makedirs(config_dir, exist_ok=True)
                    for member in channel_members:
                        rel = member.name[len("channels/") :]
                        if not rel:
                            continue
                        dest = os.path.join(config_dir, rel)
                        os.makedirs(os.path.dirname(dest), exist_ok=True)
                        f = tar.extractfile(member)
                        if f:
                            with open(dest, "wb") as out:
                                out.write(f.read())
                    restored.append("channels")
                except OSError as exc:
                    logger.warning("Could not restore channel configs: %s", exc)

    except tarfile.TarError as exc:
        raise HTTPException(400, f"Invalid archive: {exc}")

    # Restart engine if playout state or channel config was restored
    if "playout-state" in restored or "channels" in restored:
        try:
            import main

            if main.playout is not None:
                await main.playout.stop()
                await main.playout.start()
                engine_restarted = True
                logger.info("Engine restarted after restore")
        except Exception as exc:
            logger.warning("Engine restart after restore failed: %s", exc)

    logger.info("Restored from backup: %s", restored)
    return {"ok": True, "restored": restored, "engine_restarted": engine_restarted}
