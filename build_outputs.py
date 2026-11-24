#!/usr/bin/env python3
"""Build photos.json index + QA contact sheets from photos/ + manifest."""
import os, json, math
from PIL import Image, ImageDraw, ImageFont

ROOT = os.path.expanduser("~/workspace/dq-photos-pack")
PHOTOS = os.path.join(ROOT, "photos")
WORK = os.path.join(ROOT, "work")

def main():
    files = sorted(f for f in os.listdir(PHOTOS) if f.endswith(".jpg"))
    index = []
    for f in files:
        slug = f.rsplit("-", 1)[0]
        # TASK-PHOTO-THUMBNAILS: explicit thumb field per entry —
        # the app reads it; a missing thumb falls back to the
        # full photo, so the field must always be regenerated
        # together with the file list.
        index.append({"file": f"photos/{f}", "tags": slug.split("-"),
                      "thumb": f"thumbs/{f}"})
    with open(os.path.join(ROOT, "photos.json"), "w") as fh:
        json.dump(index, fh, indent=1)
    print(f"photos.json: {len(index)} entries")

    # contact sheets: thumbnails 320 wide, label under each, grid 4 cols
    THUMB_W = 320
    LABEL_H = 22
    COLS = 4
    PER_SHEET = 24
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 15)
    except Exception:
        font = ImageFont.load_default()
    sheets = math.ceil(len(files) / PER_SHEET)
    for s in range(sheets):
        batch = files[s * PER_SHEET:(s + 1) * PER_SHEET]
        cells = []
        max_h = 0
        for f in batch:
            im = Image.open(os.path.join(PHOTOS, f)).convert("RGB")
            th = round(im.height * THUMB_W / im.width)
            cells.append((f, im.resize((THUMB_W, th), Image.LANCZOS)))
            max_h = max(max_h, th)
        rows = math.ceil(len(cells) / COLS)
        W = COLS * (THUMB_W + 8) + 8
        H = rows * (max_h + LABEL_H + 8) + 8
        sheet = Image.new("RGB", (W, H), (24, 24, 24))
        d = ImageDraw.Draw(sheet)
        for i, (f, thumb) in enumerate(cells):
            r, c = divmod(i, COLS)
            x = 8 + c * (THUMB_W + 8)
            y = 8 + r * (max_h + LABEL_H + 8)
            sheet.paste(thumb, (x, y))
            d.text((x + 2, y + max_h + 3), f, fill=(255, 255, 120), font=font)
        out = os.path.join(ROOT, f"qa-sheet-{s + 1}.jpg")
        sheet.save(out, "JPEG", quality=88)
        print(f"{out}: {len(batch)} thumbs")

if __name__ == "__main__":
    main()
