"""Deeznutz - individual songs from Deezer for DroppedNeedle, downloaded in-process with the deemix engine.

Same engine as spotify_sync (github.com/b4ird/spotify_sync): ``deezer-py`` logs in
with the ARL and ``deemix``'s ``generateDownloadObject`` + ``Downloader`` fetch the
track. No deemix server or container: the libraries are vendored in ``vendor/``
(built for the DroppedNeedle image by ``vendor.sh``) and appended to ``sys.path``
only if the host doesn't already provide them, so host packages always win.

Why files mode: for a single-song request DroppedNeedle calls the plugin's
``search_album`` (never ``search_track``) and then picks the one file whose name
matches the wanted song. So every result is a Deezer album (or an artist's top
tracks) listed track-by-track as ``DownloadFileRef``s, and ``enqueue`` downloads
only the tracks DroppedNeedle picked.

Tracks-only mode (default): song requests reach this plugin with
``track_count=1``; anything larger is an album request, answered with ``[]`` so
it falls through to the next source (slskd). Set ``mode = all`` to take albums too.

Each task downloads into ``<downloads_dir>/<task_id>/``, and ``TaskHandle.nzo_id``
holds the Deezer track ids (``|``-joined, paired with ``TaskHandle.filenames``), so
after a DroppedNeedle restart a task either finds its files or restarts itself.

Deliberately no ``from __future__ import annotations``: the host compares real
signature annotations against the protocols.
"""

import asyncio
import copy
import re
import shutil
import sys
import threading
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path

from models.common import ServiceStatus
from infrastructure.plugins.protocols import (
    DownloadFileRef,
    DownloadSearchResult,
    DownloadMaterialization,
    DownloadTaskStatus,
    EnqueueRequest,
    IndexerResult,
    MountDiagnosis,
    PluginSearchResult,
    TaskHandle,
)

SOURCE = "plugin:deeznutz"
DEEZER_API = "https://api.deezer.com"
_VENDOR = Path(__file__).resolve().parent / "vendor"

# deemix bitrate ids (TrackFormats): FLAC=9, MP3_320=3, MP3_128=1
_BITRATES = {
    "flac": (9, "lossless", "FLAC", 1000),  # ~real FLAC rate, for size estimates
    "320": (3, "mp3_320", "MP3 320", 320),
    "128": (1, "low", "MP3 128", 128),
}
_EXT = {9: "flac", 3: "mp3", 1: "mp3"}
_AUDIO_EXT = {".flac", ".mp3", ".m4a", ".ogg", ".opus", ".wav"}
_EXPAND_ALBUMS = 3  # Deezer albums whose tracklists are fetched per search
_REF_PREFIX = "deezer:"
_CONCURRENT_TASKS = 2
# deemix errids that mean "your account/session", not "this track". They are
# kept out of DownloadTaskStatus.error so DroppedNeedle never quarantines a
# release for them (it blocklists any plugin failure that carries a message).
_ACCOUNT_ERRIDS = {"notLoggedIn", "wrongLicense", "infiniteLoopBackoff"}


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def _clean(text: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", text or "").strip()


def _audio_in(folder: Path) -> list[Path]:
    if not folder.is_dir():
        return []
    return sorted(p for p in folder.rglob("*") if p.suffix.lower() in _AUDIO_EXT and p.is_file())


def _deemix():
    """Import deezer-py + deemix, falling back to the vendored copies."""
    try:
        import deemix  # noqa: F401
    except ImportError:
        if _VENDOR.is_dir() and str(_VENDOR) not in sys.path:
            sys.path.append(str(_VENDOR))  # appended: host packages win
    from deezer import Deezer
    from deemix import generateDownloadObject
    from deemix.downloader import Downloader
    from deemix.settings import DEFAULTS

    return Deezer, generateDownloadObject, Downloader, DEFAULTS


class _Job:
    def __init__(self, task_id: str, track_ids: list[str], bitrate: int, folder: Path, bytes_total: int = 0):
        self.task_id = task_id
        self.bytes_total = bytes_total  # estimate from the search; 0 after a host restart
        self.track_ids = track_ids
        self.bitrate = bitrate
        self.folder = folder
        self.objects: dict[str, object] = {}  # track id -> deemix download object
        self.errors: dict[str, str] = {}
        self.running = False
        self.done = False
        self.cancelled = False


class Deeznutz:
    def __init__(self, context):
        self.ctx = context
        self.log = context.logger
        self._dz = None
        self._dz_arl = ""
        self._login_lock = threading.Lock()
        self._jobs: dict[str, _Job] = {}
        self._account_error = ""  # last account-level failure, shown in plugin health
        self._tasks: set[asyncio.Task] = set()
        self._slots = asyncio.Semaphore(_CONCURRENT_TASKS)

    # ------------------------------------------------------------------ settings

    def _setting(self, key: str) -> str:
        return (self.ctx.settings.get(key) or "").strip()

    def _downloads_dir(self) -> Path | None:
        raw = self._setting("downloads_dir")
        return Path(raw) if raw else None

    def _tracks_only(self) -> bool:
        return self._setting("mode").lower() != "all"

    def _bitrate(self) -> tuple[int, str, str, int]:
        chosen = self._setting("bitrate").lower().replace("kbps", "").strip()
        return _BITRATES.get(chosen, _BITRATES["flac"])

    def _max_results(self) -> int:
        try:
            return max(1, min(25, int(self._setting("max_results") or 8)))
        except ValueError:
            return 8

    # ------------------------------------------------------------ deezer / deemix

    def _login(self):
        """Blocking: a logged-in deezer-py client (re-login when the ARL changes)."""
        arl = self._setting("arl")
        if not arl:
            raise RuntimeError("no Deezer ARL configured")
        with self._login_lock:
            if self._dz is not None and self._dz_arl == arl and getattr(self._dz, "logged_in", False):
                return self._dz
            Deezer, *_ = _deemix()
            dz = Deezer()
            if not dz.login_via_arl(arl):
                raise RuntimeError("Deezer rejected the ARL - refresh it from deezer.com")
            if arl != self._dz_arl:
                self._account_error = ""  # a new ARL deserves a clean slate
            self._dz, self._dz_arl = dz, arl
            self.log.info("deeznutz: logged in to Deezer")
            return dz

    def _deemix_settings(self, folder: Path) -> dict:
        *_, defaults = _deemix()
        settings = copy.deepcopy(defaults)
        settings.update(
            downloadLocation=str(folder),
            tracknameTemplate="%tracknumber% - %artist% - %title%",
            albumTracknameTemplate="%tracknumber% - %artist% - %title%",
            createPlaylistFolder=False,
            createArtistFolder=False,
            createAlbumFolder=False,
            createSingleFolder=False,
            createCDFolder=False,
            fallbackBitrate=True,  # no FLAC for a track -> 320 rather than a failure
            fallbackSearch=False,
            saveArtwork=False,
            saveArtworkArtist=False,
            createM3U8File=False,
            logErrors=False,
            logSearched=False,
        )
        return settings

    def _download_track(self, job: _Job, track_id: str) -> None:
        """Blocking: one Deezer track into the job folder, the spotify_sync way."""
        _, generate, Downloader, _ = _deemix()
        dz = self._login()
        obj = generate(dz, f"https://www.deezer.com/track/{track_id}", job.bitrate)
        job.objects[track_id] = obj
        if job.cancelled:
            obj.isCanceled = True
        Downloader(dz, obj, self._deemix_settings(job.folder)).start()

    async def _run(self, job: _Job) -> None:
        async with self._slots:
            job.running = True
            try:
                for track_id in job.track_ids:
                    if job.cancelled:
                        break
                    try:
                        await asyncio.to_thread(self._download_track, job, track_id)
                    except Exception as exc:  # noqa: BLE001 - recorded per track
                        self.log.warning("deeznutz: track %s failed: %s", track_id, exc)
                        job.errors[track_id] = str(exc)
            finally:
                job.running = False
                job.done = True

    def _start(self, job: _Job) -> None:
        self._jobs[job.task_id] = job
        task = asyncio.get_running_loop().create_task(self._run(job), name=f"deeznutz-{job.task_id}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # --------------------------------------------------------------- handle data

    @staticmethod
    def _pairs(handle: TaskHandle) -> list[tuple[str, str]]:
        """(DroppedNeedle filename, Deezer track id) for every track in the task."""
        ids = (handle.nzo_id or "").split("|")
        return [(name, tid) for name, tid in zip(handle.filenames or [], ids) if tid]

    @staticmethod
    def _task_id(handle: TaskHandle) -> str:
        name = handle.job_name or ""
        return name[len("droppedneedle-"):] if name.startswith("droppedneedle-") else name

    def _folder(self, task_id: str) -> Path | None:
        """``<downloads_dir>/<task_id>``, or None when the handle names no task.

        A handle without a task id is DroppedNeedle's pre-enqueue placeholder
        (the enqueue failed, so ours never replaced it). Resolving it to the
        downloads root would hand cleanup the shared folder, so every caller
        treats None as "nothing of ours here"."""
        downloads = self._downloads_dir()
        if downloads is None or not re.fullmatch(r"[A-Za-z0-9_-]+", task_id or ""):
            return None
        return downloads / task_id

    @staticmethod
    def _bitrate_from(handle_or_payload) -> int | None:
        raw = handle_or_payload if isinstance(handle_or_payload, str) else (handle_or_payload.plugin_token or "")
        tail = raw.rpartition(":")[2]
        return int(tail) if tail.isdigit() else None

    def _track_state(self, job: _Job | None, track_id: str) -> tuple[str, str | None, bool]:
        """(queued / downloading / completed / failed, error, is_account_error)."""
        if job is None:
            return "queued", None, False
        if track_id in job.errors:
            # An exception outside deemix's own error handling: login, network, API.
            return "failed", job.errors[track_id][:300], True
        obj = job.objects.get(track_id)
        if obj is None:
            return ("failed", "not downloaded", False) if job.done else ("queued", None, False)
        if getattr(obj, "downloaded", 0):
            return "completed", None, False
        if getattr(obj, "failed", 0):
            errors = getattr(obj, "errors", []) or []
            first = errors[0] if errors and isinstance(errors[0], dict) else {}
            errid = str(first.get("errid") or "")
            message = str(first.get("message") or "deemix could not download the track")[:300]
            if errid in _ACCOUNT_ERRIDS:
                self._dz = None  # force a fresh login next time
                return "failed", f"{message} - check the ARL / Deezer subscription", True
            return "failed", message, False
        if job.done:
            return "failed", "deemix finished without a file", False
        return "downloading", None, False

    def _file_for(self, job: _Job | None, folder: Path, track_id: str, filename: str, total: int) -> Path | None:
        """Blocking: the finished file for one track."""
        obj = job.objects.get(track_id) if job else None
        for entry in getattr(obj, "files", None) or []:
            path = Path(entry.get("path", "")) if isinstance(entry, dict) else None
            if path and path.is_file():
                return path
        files = _audio_in(folder)
        if total == 1 and len(files) == 1:
            return files[0]
        title = _norm(Path(filename).stem.split(" - ", 2)[-1])
        return next((p for p in files if title and title in _norm(p.stem)), None)

    # ------------------------------------------------------ DownloadClientProtocol

    @property
    def client_name(self) -> str:
        return SOURCE

    def is_configured(self) -> bool:
        return bool(self._setting("arl") and self._downloads_dir())

    async def health_check(self) -> ServiceStatus:
        downloads = self._downloads_dir()
        if downloads is None:
            return ServiceStatus(status="error", message="Downloads directory is not configured")
        if not await asyncio.to_thread(downloads.is_dir):
            return ServiceStatus(status="error", message=f"Downloads directory not found: {downloads}")
        try:
            await asyncio.to_thread(_deemix)
        except ImportError as exc:
            return ServiceStatus(status="error", message=f"deemix libraries missing - run vendor.sh ({exc})")
        try:
            dz = await asyncio.to_thread(self._login)
        except Exception as exc:  # noqa: BLE001 - surfaced as health, never raised
            return ServiceStatus(status="error", message=str(exc))
        user = getattr(dz, "current_user", {}) or {}
        if self._account_error:
            return ServiceStatus(status="ok", message=f"Logged in, but the last download hit an account problem: {self._account_error}")
        if self._bitrate()[0] == 9 and not user.get("can_stream_lossless"):
            return ServiceStatus(status="ok", message="Logged in, but this account can't stream FLAC - downloads fall back to 320")
        return ServiceStatus(status="ok", message="Logged in to Deezer")

    async def enqueue(self, request: EnqueueRequest) -> TaskHandle:
        refs = [r for r in request.files if r.username.startswith(_REF_PREFIX)]
        if not refs:
            raise RuntimeError("deeznutz enqueue needs track files produced by the deeznutz indexer")
        downloads = self._downloads_dir()
        if downloads is None:
            raise RuntimeError("Downloads directory is not configured")
        track_ids = [r.username[len(_REF_PREFIX):] for r in refs]
        bitrate = self._bitrate_from(request.payload or "") or self._bitrate()[0]
        # Fail fast on a bad ARL: an enqueue failure fails over without
        # blocklisting, unlike a failed download.
        try:
            await asyncio.to_thread(self._login)
        except Exception as exc:
            self._account_error = str(exc)
            raise
        folder = downloads / request.task_id
        await asyncio.to_thread(folder.mkdir, parents=True, exist_ok=True)
        self._start(_Job(request.task_id, track_ids, bitrate, folder, sum(r.size for r in refs)))
        self.log.info("deeznutz: downloading %d track(s) for task %s", len(track_ids), request.task_id)
        return TaskHandle(
            source=SOURCE,
            job_name=request.job_name or f"droppedneedle-{request.task_id}",
            filenames=[r.filename for r in refs],
            nzo_id="|".join(track_ids),
            plugin_token=request.payload,
        )

    async def get_status(self, handle: TaskHandle) -> DownloadTaskStatus:
        task_id = self._task_id(handle)
        pairs = self._pairs(handle)
        folder = self._folder(task_id)
        if folder is None or not pairs:
            return DownloadTaskStatus(task_id=task_id, status="failed", error=None)
        job = self._jobs.get(task_id)
        if job is None:
            # DroppedNeedle restarted mid-task: adopt finished files or start over.
            found = [await asyncio.to_thread(self._file_for, None, folder, tid, name, len(pairs)) for name, tid in pairs]
            if pairs and all(found):
                return DownloadTaskStatus(
                    task_id=task_id, status="completed", files_total=len(pairs), files_completed=len(pairs),
                    progress_percent=100.0, succeeded_filenames=[name for name, _ in pairs],
                )
            bitrate = self._bitrate_from(handle) or self._bitrate()[0]
            await asyncio.to_thread(folder.mkdir, parents=True, exist_ok=True)
            job = _Job(task_id, [tid for _, tid in pairs], bitrate, folder)
            self._start(job)
            self.log.info("deeznutz: restarted task %s after a host restart", task_id)

        states, errors, succeeded = [], [], []
        for name, tid in pairs:
            state, error, account = self._track_state(job, tid)
            if state == "completed" and await asyncio.to_thread(self._file_for, job, folder, tid, name, len(pairs)) is None:
                state, error, account = "failed", "deemix reported success but the file is missing", False
            states.append(state)
            if error and account:
                if error != self._account_error:
                    self.log.warning("deeznutz: task %s: %s", task_id, error)
                self._account_error = error
            elif error:
                self.log.info("deeznutz: task %s track %s: %s", task_id, tid, error)
                errors.append(error)
            if state == "completed":
                succeeded.append(name)
                self._account_error = ""

        # deemix streams straight into the final file, so the folder's size is
        # real byte progress - what DroppedNeedle's stall watchdog measures.
        bytes_done = await asyncio.to_thread(lambda: sum(p.stat().st_size for p in _audio_in(folder)))

        total = len(pairs)
        done, failed = states.count("completed"), states.count("failed")
        if "downloading" in states:
            status = "downloading"
        elif "queued" in states:
            status = "downloading" if (done or failed or job.running) else "queued"
        elif total and done == total:
            status = "completed"
        elif done:
            status = "partial"
        else:
            status = "failed"
        return DownloadTaskStatus(
            task_id=task_id,
            status=status,
            files_total=total,
            files_completed=done,
            files_failed=failed,
            bytes_total=max(job.bytes_total, bytes_done),
            bytes_downloaded=bytes_done,
            progress_percent=100.0 if status == "completed" else (
                min(99.0, 100.0 * bytes_done / job.bytes_total) if job.bytes_total else 100.0 * done / max(total, 1)
            ),
            error=errors[0] if errors and status in ("failed", "partial") else None,
            succeeded_filenames=succeeded,
            has_active_transfer=status == "downloading",
            matched_transfers=total,
        )

    async def abort(self, handle: TaskHandle) -> bool:
        job = self._jobs.get(self._task_id(handle))
        if job is None:
            return False
        job.cancelled = True
        for obj in job.objects.values():
            obj.isCanceled = True
        return True

    async def inspect_materialization(self, handle: TaskHandle) -> DownloadMaterialization:
        task_id = self._task_id(handle)
        downloads = self._downloads_dir()
        folder = self._folder(task_id)
        if folder is None:
            return DownloadMaterialization(
                state="missing",
                mount_root=str(downloads or ""),
                mount_healthy=bool(downloads) and await asyncio.to_thread(downloads.is_dir),
            )
        job = self._jobs.get(task_id)
        files = await asyncio.to_thread(_audio_in, folder)
        if job is not None and job.running:
            state = "active"
        elif files:
            state = "completed"
        elif job is not None or await asyncio.to_thread(folder.is_dir):
            state = "failed"
        else:
            state = "missing"
        return DownloadMaterialization(
            state=state,
            nzo_id=handle.nzo_id,
            mount_root=str(downloads or ""),
            workspace_path=str(folder),  # per-task folder, safe to clean up
            file_paths=[str(p) for p in files],
            mount_healthy=bool(downloads) and await asyncio.to_thread(downloads.is_dir),
        )

    async def discard_client_artifacts(self, handle: TaskHandle) -> bool:
        task_id = self._task_id(handle)
        job = self._jobs.pop(task_id, None)
        folder = self._folder(task_id)
        if folder is None:
            return False

        def _tidy() -> None:
            # Leftovers only (no audio): partial downloads, empty folders.
            if folder.is_dir() and not _audio_in(folder):
                shutil.rmtree(folder, ignore_errors=True)

        await asyncio.to_thread(_tidy)
        return job is not None

    async def list_completed_files(self, handle: TaskHandle) -> list[Path]:
        folder = self._folder(self._task_id(handle))
        return await asyncio.to_thread(_audio_in, folder) if folder else []

    async def get_file_path(
        self,
        handle: TaskHandle,
        remote_filename: str,
        size: int | None = None,
    ) -> Path | None:
        task_id = self._task_id(handle)
        folder = self._folder(task_id)
        if folder is None:
            return None
        pairs = self._pairs(handle)
        for name, tid in pairs:
            if name == remote_filename:
                return await asyncio.to_thread(
                    self._file_for, self._jobs.get(task_id), folder, tid, name, len(pairs)
                )
        return None

    async def diagnose_downloads_mount(self) -> MountDiagnosis:
        # Downloads happen in-process into DroppedNeedle's own filesystem: no
        # second namespace that could be mis-mounted.
        return MountDiagnosis(supported=False)


    # ------------------------------------------------------------ IndexerProtocol

    @property
    def indexer_name(self) -> str:
        return SOURCE

    async def _deezer(self, path: str, timeout: float, **params) -> list[dict]:
        response = await self.ctx.http.get(f"{DEEZER_API}/{path}", params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"Deezer API error: {data['error']}")
        return [d for d in (data or {}).get("data", []) if isinstance(d, dict)]

    def _match(self, artist: str, wanted: str, candidate_artist: str, candidate_title: str) -> float:
        scoring = getattr(self.ctx, "scoring", None)
        if scoring is not None:
            try:
                return float(scoring.album_match(artist, wanted, f"{candidate_artist} - {candidate_title}"))
            except Exception:  # noqa: BLE001 - fall back to a local matcher
                pass
        a = SequenceMatcher(None, _norm(artist), _norm(candidate_artist)).ratio()
        t = SequenceMatcher(None, _norm(wanted), _norm(candidate_title)).ratio()
        return 0.4 * a + 0.6 * t

    @staticmethod
    def _refs(tracks: list[dict], folder: str, bitrate: tuple) -> list[DownloadFileRef]:
        """Deezer tracks -> file refs DroppedNeedle's track matcher can score by name.

        Built as ``DownloadSearchResult`` (a superset of ``DownloadFileRef``):
        when a one-track *album* request scores this release, DroppedNeedle's
        plugin scorer copies ``release.files`` into ``ScoredCandidate.files``,
        which is typed ``list[DownloadSearchResult]``; plain refs lack
        ``parent_directory``/``extension`` and the saved search can't be read
        back. Decoding ignores the extra fields wherever a ``DownloadFileRef``
        is expected, so the richer shape is safe on every path."""
        bitrate_id, _, _, kbps = bitrate
        refs = []
        for n, track in enumerate(tracks, 1):
            if not track.get("id") or track.get("readable") is False:
                continue
            disk = int(track.get("disk_number") or 1)
            pos = int(track.get("track_position") or n)
            number = f"{disk}-{pos:02d}" if disk > 1 else f"{pos:02d}"
            artist = _clean((track.get("artist") or {}).get("name", ""))
            name = f"{number} - {artist} - {_clean(track.get('title', ''))}.{_EXT[bitrate_id]}"
            duration = int(track.get("duration") or 0)
            refs.append(DownloadSearchResult(
                username=f"{_REF_PREFIX}{track['id']}",
                filename=f"{folder}/{name}",
                parent_directory=folder,
                size=duration * kbps * 125,
                extension=_EXT[bitrate_id],
                bitrate=None if bitrate_id == 9 else kbps,
                duration=float(duration) or None,
            ))
        return refs

    def _release(self, title: str, refs: list[DownloadFileRef], score: float, payload: str, bitrate: tuple) -> IndexerResult:
        return IndexerResult(
            source=SOURCE,
            plugin=PluginSearchResult(
                title=f"{title} [Deezer {bitrate[2]}]",
                size_bytes=sum(r.size for r in refs),
                score=max(0.0, min(1.0, score)),
                quality_tier=bitrate[1],
                files=refs,
                payload=payload,
            ),
        )

    async def search_album(
        self,
        artist_name: str,
        album_title: str,
        year: int | None = None,
        track_count: int | None = None,
        *,
        timeout: float = 30.0,
    ) -> list[IndexerResult]:
        if self._tracks_only() and track_count != 1:
            return []  # an album request: leave it to slskd / other sources
        per_call = min(10.0, timeout / 3)
        bitrate = self._bitrate()
        limit = self._max_results()

        albums: list[tuple[float, dict]] = []
        if album_title and album_title != "Unknown Album":
            items = await self._deezer("search/album", per_call, q=f'artist:"{artist_name}" album:"{album_title}"', limit=limit)
            if not items:
                items = await self._deezer("search/album", per_call, q=f"{artist_name} {album_title}", limit=limit)
            for item in items:
                score = self._match(artist_name, album_title, (item.get("artist") or {}).get("name", ""), item.get("title", ""))
                tracks = int(item.get("nb_tracks") or 0)
                if track_count and track_count > 1 and tracks and tracks != track_count:
                    score *= 0.85
                if score >= 0.5:
                    albums.append((score, item))
            albums.sort(key=lambda pair: pair[0], reverse=True)
            albums = albums[:_EXPAND_ALBUMS]

        async def expand(score: float, album: dict) -> IndexerResult | None:
            tracks = await self._deezer(f"album/{album['id']}/tracks", per_call, limit=300)
            folder = _clean(f"{(album.get('artist') or {}).get('name', '')} - {album.get('title', '')}")
            refs = self._refs(tracks, folder, bitrate)
            return self._release(folder, refs, score, f"album:{album['id']}:{bitrate[0]}", bitrate) if refs else None

        async def top_tracks() -> IndexerResult | None:
            # Songs whose MusicBrainz album doesn't exist on Deezer under that
            # name (compilations, regional singles) usually still sit in the
            # artist's top tracks; the track matcher picks the right one.
            artists = await self._deezer("search/artist", per_call, q=artist_name, limit=1)
            if not artists:
                return None
            artist = artists[0]
            if SequenceMatcher(None, _norm(artist_name), _norm(artist.get("name", ""))).ratio() < 0.8:
                return None
            tracks = await self._deezer(f"artist/{artist['id']}/top", per_call, limit=100)
            folder = _clean(f"{artist.get('name', '')} - Top tracks")
            refs = self._refs(tracks, folder, bitrate)
            return self._release(folder, refs, 0.6, f"artist:{artist['id']}:{bitrate[0]}", bitrate) if refs else None

        jobs = [expand(score, album) for score, album in albums]
        if track_count == 1:
            jobs.append(top_tracks())
        results = []
        for outcome in await asyncio.gather(*jobs, return_exceptions=True):
            if isinstance(outcome, Exception):
                self.log.warning("deeznutz: Deezer lookup failed: %s", outcome)
            elif outcome is not None:
                results.append(outcome)
        return results

    async def search_track(
        self,
        artist_name: str,
        track_title: str,
        album_title: str | None = None,
        duration_seconds: int | None = None,
        *,
        timeout: float = 30.0,
    ) -> list[IndexerResult]:
        # Only reached when another plugin pools this indexer; DroppedNeedle's
        # own track flow goes through search_album + per-file matching.
        bitrate = self._bitrate()
        items = await self._deezer("search/track", timeout, q=f'artist:"{artist_name}" track:"{track_title}"', limit=self._max_results())
        if not items:
            items = await self._deezer("search/track", timeout, q=f"{artist_name} {track_title}", limit=self._max_results())
        results = []
        for item in items:
            score = self._match(artist_name, track_title, (item.get("artist") or {}).get("name", ""), item.get("title", ""))
            if duration_seconds and item.get("duration") and abs(int(item["duration"]) - duration_seconds) > 5:
                score *= 0.8
            folder = _clean(f"{(item.get('artist') or {}).get('name', '')} - {(item.get('album') or {}).get('title', '')}")
            refs = self._refs([item], folder, bitrate)
            if refs:
                results.append(self._release(folder, refs, score, f"track:{item['id']}:{bitrate[0]}", bitrate))
        return results
