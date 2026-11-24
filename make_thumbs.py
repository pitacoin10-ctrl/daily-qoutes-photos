#!/usr/bin/env python3
"""make_thumbs.py — pre-made 360px thumbnails for the photo library.

TASK-PHOTO-THUMBNAILS (2026-11-24): the library picker grid was
downloading each FULL photo (~175 KB avg) just to show a 240 px
cell. Every photo now gets a small thumbnail at
``thumbs/<same filename>`` so the app can fetch the thumb instead.

Spec: width 360 px, JPEG quality 60, target <= 25,000 bytes.
If quality 60 is still over target, the quality steps down
(50/40/30); only if that still fails does the width step down
(320/288). A thumb that cannot meet the target is still written
at the smallest setting and REPORTED — never silently skipped.

Usage:
  python3 make_thumbs.py            # backfill: every photos/*.jpg
  python3 make_thumbs.py --check    # verify only, no writes

Also refreshes photos.json: every entry gains
``"thumb": "thumbs/<same filename>"`` (explicit field — the app
reads it; a missing thumb falls back to the full photo).
Idempotent: a thumb is regenerated only when missing, or when
its source photo is newer than the thumb.
"""
import argparse
import io
import json
import os
import sys

from PIL import Image

ROOT = os.path.dirname(os.path.abspath(__file__))
PHOTOS = os.path.join(ROOT, "photos")
THUMBS = os.path.join(ROOT, "thumbs")
INDEX = os.path.join(ROOT, "photos.json")

THUMB_WIDTH = 360
THUMB_QUALITY = 60
MAX_BYTES = 25_000


def make_thumb(src_path, dst_path):
    """Write a thumbnail for src_path to dst_path.

    Returns (bytes, width, quality) or None if the source is bad.
    """
    try:
        im = Image.open(src_path).convert("RGB")
    except Exception:
        return None
    w, h = im.size
    for tw in (THUMB_WIDTH, 320, 288):
        th = max(1, round(h * tw / w))
        small = im.resize((tw, th), Image.LANCZOS) if tw != w else im
        for q in (60, 50, 40, 30):
            buf = io.BytesIO()
            small.save(buf, "JPEG", quality=q, optimize=True)
            data = buf.getvalue()
            if len(data) <= MAX_BYTES or (tw == 288 and q == 30):
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                with open(dst_path, "wb") as f:
                    f.write(data)
                return (len(data), tw, q)
    return None


def main():
    ap = argparse.ArgumentParser(description="Backfill library thumbnails")
    ap.add_argument("--check", action="store_true",
                    help="verify only, write nothing")
    args = ap.parse_args()

    files = sorted(f for f in os.listdir(PHOTOS) if f.endswith(".jpg"))
    made, skipped, failed, over = [], [], [], []
    for fn in files:
        src = os.path.join(PHOTOS, fn)
        dst = os.path.join(THUMBS, fn)
        if os.path.exists(dst) and \
                os.path.getmtime(dst) >= os.path.getmtime(src):
            skipped.append(fn)
            continue
        if args.check:
            failed.append(fn + " (missing)")
            continue
        res = make_thumb(src, dst)
        if res is None:
            failed.append(fn)
            continue
        nbytes, tw, q = res
        made.append((fn, nbytes))
        if nbytes > MAX_BYTES:
            over.append((fn, nbytes))
        elif tw != THUMB_WIDTH or q != THUMB_QUALITY:
            over.append((fn, nbytes, tw, q))

    if not args.check:
        # photos.json: add the explicit thumb field per entry.
        with open(INDEX) as f:
            index = json.load(f)
        n_thumb = 0
        for e in index:
            fn = os.path.basename(e["file"])
            thumb = "thumbs/" + fn
            if e.get("thumb") != thumb:
                e["thumb"] = thumb
            n_thumb += 1
        with open(INDEX, "w") as f:
            json.dump(index, f, indent=1)
        print(f"photos.json: {n_thumb} entries carry a thumb field")

    total = len(made)
    if made:
        sizes = [b for _, b in made]
        print(f"thumbs made: {total} (skipped up-to-date: {len(skipped)})")
        print(f"avg: {sum(sizes) / len(sizes) / 1024:.1f} KB, "
              f"max: {max(sizes) / 1024:.1f} KB, "
              f"min: {min(sizes) / 1024:.1f} KB, "
              f"total: {sum(sizes) / 1e6:.1f} MB")
    else:
        print(f"thumbs made: 0 (skipped up-to-date: {len(skipped)})")
    if over:
        print(f"NOTE: {len(over)} thumb(s) needed a quality/width "
              f"step-down or stayed over target: {over[:10]}")
    if failed:
        print(f"FAILED: {len(failed)}: {failed[:10]}")
        return 1
    # Full-library check: every photo must have a thumb on disk.
    missing = [fn for fn in files
               if not os.path.exists(os.path.join(THUMBS, fn))]
    print(f"library check: {len(files) - len(missing)}/{len(files)} "
          f"photos have a thumb on disk")
    return 0 if not missing else 1


if __name__ == "__main__":
    sys.exit(main())
