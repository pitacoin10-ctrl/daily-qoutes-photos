# daily-qoutes-photos
Photo library for the Daily Qoutes app — tagged stock photos (photos.json index).

Thumbnails (TASK-PHOTO-THUMBNAILS, 2026-11-24): every photo has a
pre-made thumb at `thumbs/<same filename>` — 360 px wide, JPEG
quality 60, ≤ 25 KB. `photos.json` carries an explicit `"thumb"`
field per entry; the app loads the thumb for grid cells and falls
back to the full photo when a thumb is missing. Thumbs are made by
`make_thumbs.py` (backfill) and by `harvest.py` for every new photo
in the same run.
