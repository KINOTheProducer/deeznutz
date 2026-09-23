# Deeznutz - Deezer plugin for DroppedNeedle

Finds **individual songs** on Deezer and downloads them inside DroppedNeedle, using the
same engine as [spotify_sync](https://github.com/b4ird/spotify_sync): `deezer-py` logs in
with your ARL, and deemix's `generateDownloadObject` + `Downloader` fetch the track. No
deemix server or container is needed.

## Install (Unraid)

1. Add two paths to the DroppedNeedle container:
   - `/app/plugins` → `/mnt/user/appdata/droppedneedle/plugins` (without this, plugins vanish on update)
   - `/deeznutz-downloads` → `/mnt/user/data/media/downloads/deeznutz`
2. Copy this whole folder, including `vendor/`, to `/mnt/user/appdata/droppedneedle/plugins/deeznutz`.
3. In **Settings > Plugins**, read the code, fill in the settings, and enable it.

| Setting | Example |
| --- | --- |
| Deezer ARL | *(secret)* |
| Downloads directory | `/deeznutz-downloads` |
| Mode | `tracks` (default) or `all` |
| Bitrate | `flac` (default), `320` or `128` |
| Max results per search | `8` |

`vendor/` holds `ss-deemx` + `deezer-py` and their dependencies, built for the DroppedNeedle
image (Linux x86_64, Python 3.13). Rebuild it with `./vendor.sh`, or `ARCH=aarch64 ./vendor.sh`
on ARM. The folder is appended to `sys.path` only when the host lacks the libraries, so
DroppedNeedle's own packages always take precedence.

## How it behaves

- **Tracks only by default.** DroppedNeedle asks every source for a song as
  `search_album(artist, album, track_count=1)`; album requests carry the real track count.
  In `tracks` mode the plugin answers only song requests, so albums fall through to slskd.
- **Files mode.** Each result is a Deezer tracklist: the song's album's best Deezer matches,
  plus the artist's top tracks for songs whose album isn't on Deezer under that name.
  DroppedNeedle's track matcher picks the file, and only that track is downloaded.
- **Per-task folders.** Each task downloads into `<downloads dir>/<task id>/`. The handle holds
  the Deezer track ids, so after a DroppedNeedle restart a task adopts its finished files or
  restarts itself. At most 2 tasks download at once.
- **Quality.** FLAC by default. A track with no FLAC falls back to 320 instead of failing.
  DroppedNeedle rates the imported file by what actually arrived.
- **Cleanup.** Cleanup removes a task folder only when it holds no audio.

## Spotify playlists

Importing a Spotify playlist into DroppedNeedle doesn't request missing songs; request them per
track. spotify_sync matches by ISRC, but DroppedNeedle doesn't hand plugins a recording id,
so this plugin matches by artist, album and title instead.
