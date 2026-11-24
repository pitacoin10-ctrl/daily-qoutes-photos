#!/usr/bin/env python3
"""
harvest.py — reusable photo harvester for the Daily Qoutes photo library.

This is the script that built the starter pack (Day 1) and the same
script the daily pipeline runs to grow the library (+300/day toward
5,000+). It is safe to re-run: it is DEDUPE-AWARE against the existing
pack — before accepting anything it loads

  * every file already in photos/ (hashed; a candidate whose processed
    bytes match a kept photo is skipped, even from another source),
  * work/harvest-state.json (hashes of every original ever downloaded,
    URLs already fetched, and size+dimension fingerprints), and
  * the per-category numbering already on disk (new photos continue
    the NN sequence, they never overwrite).

USAGE
  python3 harvest.py                        # fill every category to 10
  python3 harvest.py --target 25            # fill every category to 25
  python3 harvest.py --target 15 --categories desert-dunes-sand,sea-waves-shore
  python3 harvest.py --sources commons      # only the fill source
  python3 harvest.py --target 25 --cycles 6 # ride out rate-limit waves

Sources (no API keys): Openverse (CC0/PDM/BY/BY-SA, size=large) first,
Wikimedia Commons (JPEG, original width >= 1600) as fill.

Rate limits: the Openverse API is called serially, ~2.5 s apart, with
backoff on 429. Image downloads run a few at a time but each HOST is
paced independently (>= 2.5 s between requests to the same host) and
a host that answers 429/503 is skipped for 5 minutes — the big CDNs
rate-limit this network in waves, so --cycles re-runs the passes
until every category reaches the target.

HARD GATES for every photo: real JPEG, decodable, landscape, source
width >= 1280, photographic (title/tag heuristics drop people, maps,
diagrams, logos, screenshots), no exact or near duplicates. CDN
rendition URLs are upgraded to a larger size of the SAME photo first
(Flickr _h, WordPress originals) because Openverse often points at a
web-sized copy. Processing: resize to width 1600 (then 1400, then
1280 if needed), JPEG quality ladder 82/74/66/58 until the file is
<= 200,000 bytes; a photo whose final file would be < 50,000 bytes
is dropped as over-compressible.

To add a category: add one line to CATEGORIES below (slug -> search
queries). The slug becomes the filename prefix AND the tag words in
photos.json (hyphen-split). Then re-run build_outputs.py (work/) to
regenerate photos.json + QA sheets.
"""
import io, os, sys, json, time, hashlib, argparse, threading, urllib.parse
from concurrent.futures import ThreadPoolExecutor
import requests
from PIL import Image, ImageOps

ROOT = os.path.dirname(os.path.abspath(__file__))
PHOTOS = os.path.join(ROOT, "photos")
THUMBS = os.path.join(ROOT, "thumbs")
WORK = os.path.join(ROOT, "work")
STATE_PATH = os.path.join(WORK, "harvest-state.json")
LEGACY_MANIFEST = os.path.join(WORK, "manifest.json")
MAX_BYTES = 200_000
MIN_BYTES = 50_000
WORKERS = 4
BATCH = 8

HEADERS = {"User-Agent": "DQPhotosPackBuilder/1.0 (personal project; contact via repo owner)"}

# ---------------------------------------------------------------------
# CATEGORIES: slug -> [search queries]. Slug = filename prefix + tags.
# Add new categories here; everything else picks them up automatically.
# ---------------------------------------------------------------------
CATEGORIES = {
    "desert-dunes-sand":        ["desert sand dunes", "sand dunes desert", "sahara desert dunes", "namib desert"],
    "sea-waves-shore":          ["ocean waves shore", "sea waves beach", "waves crashing rocks", "ocean coast waves"],
    "mountain-valley-mist":     ["mountain valley fog", "misty mountain valley", "foggy mountains", "clouds mountain valley"],
    "rain-window-drops":        ["rain window water drops", "rain drops glass", "rainy window", "water drops window rain"],
    "rose-garden-flowers":      ["rose garden", "roses garden flowers", "red roses", "rose flowers close"],
    "olive-tree-sunlight":      ["olive tree", "olive grove", "olive trees field", "olive branch sunlight"],
    "mosque-courtyard-architecture": ["mosque architecture", "mosque dome minaret", "mosque dome", "minaret sky", "islamic architecture"],
    "old-books-library":        ["old books library", "library bookshelves old", "antique books stack", "old library shelves", "old bookshop books", "vintage books shelf"],
    "clay-stove-cooking-fire":  ["campfire fire wood", "cooking fire pot outdoor", "bonfire flames night", "firewood burning fire", "fireplace logs burning", "clay oven fire", "tandoor fire", "campfire night flames", "burning logs fireplace closeup"],
    "starry-night-sky":         ["milky way night sky", "starry night sky stars", "night sky stars landscape", "milky way galaxy", "milky way desert night", "stars over mountains night"],
    "wheat-field-sunset":       ["wheat field sunset", "wheat field golden", "wheat field harvest", "golden wheat field"],
    "forest-river-trees":       ["forest river", "river forest trees", "stream forest", "river valley forest", "forest stream trees sunlight", "green forest creek"],
    "village-houses-mountains": ["mountain village", "village mountains houses", "alpine village", "village valley mountains"],
    "lantern-light-night":      ["lantern night light", "lantern festival night", "paper lantern night", "lanterns dark background", "chinese lanterns festival", "sky lanterns night", "hanging lanterns night market", "ramadan lantern fanous", "moroccan lantern light", "oil lamp flame night", "kerosene lamp"],
    "old-town-street":          ["old town street", "old street alley town", "cobblestone street old town", "narrow old street", "medieval old town street", "historic town centre street"],
    "autumn-forest-path":       ["autumn forest path", "fall forest trail", "autumn trees forest", "forest path fall colors", "autumn path forest sunlight", "fall leaves forest road", "beech forest autumn", "autumn forest road trees"],
    "snow-mountain-lake":       ["snow mountain lake", "mountain lake winter snow", "snowy mountain reflection lake", "frozen lake mountains"],
    "waterfall-rocks-stream":   ["waterfall rocks", "waterfall stream forest", "waterfall long exposure", "cascade waterfall"],
    "sunset-clouds-sky":        ["sunset clouds sky", "dramatic sunset sky clouds", "sunset sky colors", "evening sky clouds sunset"],
    "palm-trees-oasis":         ["palm trees oasis", "date palm trees desert", "palm trees sunset", "oasis desert palms", "date palm oasis water", "palm grove trees"],
    # ADD-PHOTO-CATEGORIES (2026-11-24, ACKed 14:30 PKT) — 18 new
    # categories for the Day-6 batch (2026-11-25) onward. Quiz-topic
    # first (the v0.12.7 fast quiz design needs a photo per
    # question), then mood (quotes/lofi also use these). Same rules:
    # Openverse + Wikimedia only, <=200 KB, no people/faces.
    # NOTE: "flags-flying" will likely harvest ~nothing: BAD_TOKENS
    # rejects any title containing "flag"/"flags", and flag photos
    # are almost always titled with "flag". Flag it in the Day-6
    # report per the task; do NOT loosen BAD_TOKENS for it without
    # a decision — the token also guards other categories.
    "fruits-closeup":           ["fresh fruit close up", "fruit closeup macro", "apples oranges fruit", "berries fruit close"],
    "vegetables-market":        ["vegetables market stall", "fresh vegetables market", "vegetable stall produce", "farmers market vegetables"],
    "animals-wild":             ["wild animals safari", "lion savanna wildlife", "elephant wildlife", "wildlife animals nature"],
    "birds-colorful":           ["colorful birds", "parrot colorful bird", "kingfisher bird", "tropical birds bright"],
    "sea-animals":              ["sea turtle ocean", "dolphin ocean", "whale ocean", "coral reef fish"],
    "vehicles-classic":         ["classic car vintage", "vintage car classic", "old car vintage", "classic automobile"],
    "trains-railway":           ["train railway", "steam train railway", "railway station train", "locomotive train tracks"],
    "airplanes-sky":            ["airplane sky", "aircraft flying sky", "passenger plane sky", "airplane clouds"],
    "famous-landmarks":         ["eiffel tower", "famous landmark", "taj mahal", "great wall china"],
    "flags-flying":             ["flag flying sky", "national flag waving", "flag pole sky", "flags flying wind"],
    "musical-instruments":      ["musical instruments", "guitar closeup", "piano keys", "violin instrument"],
    "science-lab-glassware":    ["science laboratory glassware", "laboratory flasks", "chemistry lab glass", "test tubes laboratory"],
    "space-telescope-planets":  ["telescope night sky", "planet space", "saturn planet", "observatory telescope"],
    "sports-equipment":         ["sports equipment", "football soccer ball", "tennis racket ball", "cricket bat ball"],
    "technology-gadgets":       ["technology gadgets", "smartphone closeup", "laptop computer desk", "headphones gadget"],
    "flowers-closeup":          ["flowers closeup", "sunflower closeup", "tulip flower close", "daisy flower macro"],
    "coffee-tea-cup":           ["coffee cup", "tea cup steam", "coffee latte art", "teapot tea cup"],
    "city-night-streets":       ["city street night", "city lights night street", "night city street lamps", "urban street night"],
}

# tokens that mark a result as people/text/graphic — skip it
BAD_TOKENS = {
    "people","person","persons","man","woman","men","women","girl","boy",
    "child","children","baby","babies","face","faces","portrait","portraits",
    "selfie","crowd","crowds","tourist","tourists","wedding","fashion",
    "model","models","map","maps","diagram","diagrams","chart","charts",
    "graph","logo","logos","screenshot","poster","posters","sign","signs",
    "banner","illustration","illustrations","drawing","drawings","painting",
    "paintings","cartoon","sketch","statue","statues","flag","flags",
    "locator","coat","arms","soldier","soldiers","police","protest",
    "concert","swimmer","swimmers","surfer","beachgoers",
    "silhouette","silhouettes","hand","hands","feet","footprint",
    # added after Day-1 visual QA: these slipped through as titles
    "kid","kids","hiker","hikers","camper","campers","visitor","visitors",
    "student","students","teacher","workers","worker","figurine",
    "figurines","toy","toys","solar","backpack","backpacker","backpackers",
    "audience","spectators","shoppers","pedestrians","pedestrian",
    "cat","cats","dog","dogs","parade","puppet","puppets","costume",
}
BAD_PHRASES = ["coat of arms", "location map", "topographic", "aerial view map"]

def title_is_bad(title, tags_str=""):
    text = (title or "") + " " + (tags_str or "")
    low = text.lower()
    for ph in BAD_PHRASES:
        if ph in low:
            return True
    tokens = set()
    cur = ""
    for ch in low:
        if ch.isalnum():
            cur += ch
        else:
            if cur: tokens.add(cur)
            cur = ""
    if cur: tokens.add(cur)
    return len(tokens & BAD_TOKENS) > 0

def upgrade_rendition(url):
    """Ask the CDN for a bigger rendition of the SAME photo.
    Openverse's url often points at a web-sized copy (Flickr's 1024px
    _b, a WordPress -1024x683 derivative) while the original is much
    larger — those die on the >=1280 gate despite being fine photos.
    Flickr size suffix -> _h (1600px); WordPress derivative suffix
    stripped to the original upload; wp.com Photon w= raised to 1600.
    The downloaded file's true width is still verified afterwards."""
    import re
    if "staticflickr.com" in url:
        if re.search(r"_[a-z]\d?\.jpg$", url):
            return re.sub(r"_[a-z]\d?\.jpg$", "_h.jpg", url)
        if url.endswith(".jpg"):
            return url[:-4] + "_h.jpg"
        return url
    if "wp.com" in url and "w=" in url:
        return re.sub(r"w=\d+", "w=1600", url)
    if "wp-content/uploads" in url:
        return re.sub(r"-\d+x\d+(\.jpe?g)$", r"\1", url, flags=re.I)
    return url

def interleave_hosts(cands, key):
    """Round-robin candidates by image host so a batch rarely queues
    several downloads on the same (possibly rate-limited) CDN."""
    from collections import OrderedDict
    buckets = OrderedDict()
    for c in cands:
        u = key(c) or ""
        host = u.split("/")[2] if "://" in u else "?"
        buckets.setdefault(host, []).append(c)
    out = []
    while any(buckets.values()):
        for host in list(buckets):
            if buckets[host]:
                out.append(buckets[host].pop(0))
    return out

# ---------------- state / dedupe ----------------
def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    if os.path.exists(LEGACY_MANIFEST):  # Day-1 bootstrap import
        with open(LEGACY_MANIFEST) as f:
            m = json.load(f)
        st = {"accepted": m.get("accepted", []),
              "seen_hashes": m.get("seen_hashes", []),
              "seen_sizedims": m.get("seen_sizedims", []),
              "seen_urls": []}
        save_state(st)
        return st
    return {"accepted": [], "seen_hashes": [], "seen_sizedims": [], "seen_urls": []}

def save_state(st):
    os.makedirs(WORK, exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(st, f, indent=1)

def kept_file_hashes():
    """MD5 of every photo already kept in photos/ — the ground truth
    future runs must never duplicate."""
    out = set()
    if os.path.isdir(PHOTOS):
        for fn in os.listdir(PHOTOS):
            if fn.endswith(".jpg"):
                with open(os.path.join(PHOTOS, fn), "rb") as f:
                    out.add(hashlib.md5(f.read()).hexdigest())
    return out

def next_number(slug, state):
    n = sum(1 for a in state["accepted"] if a["category"] == slug)
    if os.path.isdir(PHOTOS):
        for fn in os.listdir(PHOTOS):
            if fn.startswith(slug + "-") and fn.endswith(".jpg"):
                try:
                    n = max(n, int(fn[len(slug) + 1:-4]))
                except ValueError:
                    pass
    return n + 1

def count_cat(state, slug):
    on_disk = 0
    if os.path.isdir(PHOTOS):
        on_disk = sum(1 for fn in os.listdir(PHOTOS)
                      if fn.startswith(slug + "-") and fn.endswith(".jpg"))
    return max(on_disk, sum(1 for a in state["accepted"] if a["category"] == slug))

# ---------------- processing ----------------
def process_image(data):
    """Validate + process raw bytes. Returns final JPEG bytes or None."""
    try:
        im = Image.open(io.BytesIO(data))
        if im.format != "JPEG":
            return None
        im = ImageOps.exif_transpose(im).convert("RGB")
    except Exception:
        return None
    w, h = im.size
    if w < 1280 or w < h:
        return None
    widths = sorted({min(tw, w) for tw in (1600, 1400, 1280)}, reverse=True)
    for tw in widths:
        if tw == w:
            cur = im
        else:
            th = round(h * tw / w)
            cur = im.resize((tw, th), Image.LANCZOS)
        for q in (82, 74, 66, 58):
            buf = io.BytesIO()
            cur.save(buf, "JPEG", quality=q, optimize=True, progressive=False)
            b = buf.getvalue()
            if len(b) <= MAX_BYTES:
                if len(b) < MIN_BYTES:
                    return None  # too flat / over-compressible
                return b
    return None

# TASK-PHOTO-THUMBNAILS (2026-11-24): pre-made grid thumbnails.
# Every accepted photo gets thumbs/<same filename>: width 360,
# JPEG quality 60, target <= 25,000 bytes (quality steps down
# 50/40/30, then width 320/288, only if needed). The thumb is
# written in try_accept, in the same run as the photo — never
# as a separate step a batch can skip.
THUMB_WIDTH = 360
THUMB_MAX_BYTES = 25_000

def make_thumb(src_path, dst_path):
    """Write a 360px thumbnail for src_path. Returns byte size
    or None if the source cannot be read."""
    try:
        im = Image.open(src_path).convert("RGB")
    except Exception:
        return None
    w, h = im.size
    for tw in (THUMB_WIDTH, 320, 288):
        th = max(1, round(h * tw / w))
        small = im.resize((tw, th), Image.LANCZOS)
        for q in (60, 50, 40, 30):
            buf = io.BytesIO()
            small.save(buf, "JPEG", quality=q, optimize=True)
            data = buf.getvalue()
            if len(data) <= THUMB_MAX_BYTES or (tw == 288 and q == 30):
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                with open(dst_path, "wb") as f:
                    f.write(data)
                return len(data)
    return None

# ---------------- downloads: per-host pacing + 429 cooldown ----------------
_host_lock = threading.Lock()
_host_next = {}    # host -> earliest next request time
_hot_until = {}    # host -> epoch when it may be tried again
HOST_GAP = 2.5
HOT_SECONDS = 300

def _host(url):
    return url.split("/")[2] if "://" in url else "?"

def download(url, timeout=45):
    """GET paced per host. On 429/503 the host is marked hot and
    skipped for HOT_SECONDS — other hosts keep flowing. A failed
    (429/network) URL is NOT blacklisted; a later run retries it.
    Returns content bytes or None."""
    host = _host(url)
    with _host_lock:
        if time.time() < _hot_until.get(host, 0):
            return None
        wait = _host_next.get(host, 0) - time.time()
    if wait > 0:
        time.sleep(wait)
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
    except Exception:
        with _host_lock:
            _host_next[host] = time.time() + HOST_GAP
        return None
    with _host_lock:
        _host_next[host] = time.time() + HOST_GAP
    if r.status_code == 200:
        return r.content
    if r.status_code in (429, 503):
        with _host_lock:
            _hot_until[host] = time.time() + HOT_SECONDS
        print(f"  host {host} {r.status_code}: cooling down {HOT_SECONDS}s", flush=True)
    return None

class Harvester:
    def __init__(self, state, target):
        self.state = state
        self.target = target
        os.makedirs(PHOTOS, exist_ok=True)  # fix: photos/ did not exist on a fresh pack
        self.lock = threading.Lock()
        self.kept_hashes = kept_file_hashes()
        self.seen_hashes = set(state["seen_hashes"])
        self.seen_sizedims = set(state["seen_sizedims"])
        self.seen_urls = set(state["seen_urls"])

    def persist(self):
        self.state["seen_hashes"] = sorted(self.seen_hashes)
        self.state["seen_sizedims"] = sorted(self.seen_sizedims)
        self.state["seen_urls"] = sorted(self.seen_urls)
        save_state(self.state)

    def try_accept(self, slug, data, meta):
        """Thread-safe: all shared-state mutation + file writes are
        serialized; downloads happen outside this lock."""
        with self.lock:
            if count_cat(self.state, slug) >= self.target:
                return False
            h = hashlib.md5(data).hexdigest()
            if h in self.seen_hashes:
                return False
            self.seen_hashes.add(h)
            try:
                probe = Image.open(io.BytesIO(data))
                sd = f"{len(data)}:{probe.size[0]}x{probe.size[1]}"
            except Exception:
                self.persist(); return False
            if sd in self.seen_sizedims:
                self.persist(); return False
            self.seen_sizedims.add(sd)
            final = process_image(data)
            if final is None:
                self.persist(); return False
            fh = hashlib.md5(final).hexdigest()
            if fh in self.kept_hashes:
                self.persist(); return False  # already in the library
            self.kept_hashes.add(fh)
            n = next_number(slug, self.state)
            fname = f"{slug}-{n:02d}.jpg"
            with open(os.path.join(PHOTOS, fname), "wb") as f:
                f.write(final)
            # TASK-PHOTO-THUMBNAILS (2026-11-24): every NEW photo gets
            # its 360px thumb in the same run, before the batch is
            # handed over — a batch without thumbs is incomplete.
            make_thumb(os.path.join(PHOTOS, fname),
                       os.path.join(THUMBS, fname))
            meta.update({"file": fname, "category": slug,
                         "final_bytes": len(final), "kept_hash": fh})
            self.state["accepted"].append(meta)
            self.persist()
            print(f"  ACCEPT {fname} ({len(final)//1024} KB) <- {meta.get('title','')[:60]}", flush=True)
            return True

    def fetch_candidates(self, slug, tasks):
        """tasks: list of (url, meta). Download in small parallel
        batches (per-host pacing still applies inside download())
        until the category target is met."""
        tasks = interleave_hosts(tasks, key=lambda t: t[0])
        i = 0
        while i < len(tasks) and count_cat(self.state, slug) < self.target:
            batch = tasks[i:i + BATCH]
            i += BATCH
            def job(t):
                url, meta = t
                data = download(url, timeout=60)
                if data is None:
                    return
                with self.lock:
                    self.seen_urls.add(url)
                self.try_accept(slug, data, meta)
            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                list(ex.map(job, batch))

    # ---------------- Openverse ----------------
    def openverse_candidates(self, query, pages=5):
        out = []
        for page in range(1, pages + 1):
            url = ("https://api.openverse.org/v1/images/?q=" + urllib.parse.quote(query)
                   + "&license=cc0,pdm,by,by-sa&size=large&filter_dead=false"
                   + f"&per_page=20&page={page}")
            for attempt in range(4):
                try:
                    r = requests.get(url, headers=HEADERS, timeout=30)
                except Exception as e:
                    print(f"  OV req error {e}; retry", flush=True)
                    time.sleep(5); continue
                if r.status_code == 429:
                    wait = 20 * (attempt + 1)
                    print(f"  OV 429, backoff {wait}s", flush=True)
                    time.sleep(wait); continue
                if r.status_code != 200:
                    print(f"  OV status {r.status_code}", flush=True)
                    return out
                out.extend(r.json().get("results", []))
                break
            time.sleep(2.5)
            if not out:
                break
        return out

    def pass_openverse(self, cats):
        for slug, queries in cats.items():
            if count_cat(self.state, slug) >= self.target:
                continue
            print(f"== {slug} (have {count_cat(self.state, slug)}) ==", flush=True)
            tasks, used = [], set()
            for q in queries:
                if count_cat(self.state, slug) >= self.target:
                    break
                cands = self.openverse_candidates(q)
                print(f"  query '{q}': {len(cands)} candidates", flush=True)
                for c in cands:
                    if "rawpixel" in (c.get("url") or "") or \
                       "rawpixel" in (c.get("source") or ""):
                        continue  # watermarked free renditions (QA lesson)
                    img_url = upgrade_rendition(c.get("url") or "") or None
                    if not img_url or img_url in self.seen_urls or img_url in used:
                        continue
                    title = c.get("title") or ""
                    tags = " ".join(t.get("name", "") for t in (c.get("tags") or []))
                    if title_is_bad(title, tags):
                        continue
                    # Free pre-filter from API metadata: most gate
                    # rejects (portrait / small / non-JPEG) die here
                    # without spending a download.
                    cw, ch = c.get("width"), c.get("height")
                    if cw and ch and (cw < 1280 or ch > cw):
                        continue
                    ft = (c.get("filetype") or "").lower()
                    if ft and ft not in ("jpg", "jpeg"):
                        continue
                    used.add(img_url)
                    tasks.append((img_url, {
                        "source": "openverse",
                        "source_url": c.get("foreign_landing_url") or img_url,
                        "image_url": img_url,
                        "title": title,
                        "license": c.get("license"),
                    }))
            self.fetch_candidates(slug, tasks)

    # ---------------- Wikimedia Commons fill ----------------
    def commons_candidates(self, query):
        params = {
            "action": "query", "generator": "search",
            "gsrsearch": query + " filetype:bitmap",
            "gsrnamespace": 6, "gsrlimit": 40,
            "prop": "imageinfo", "iiprop": "url|size|mime",
            "iiurlwidth": 1600, "format": "json",
        }
        try:
            r = requests.get("https://commons.wikimedia.org/w/api.php",
                             params=params, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                print(f"  Commons status {r.status_code}", flush=True)
                return []
            pages = dict((r.json().get("query") or {}).get("pages") or {})
            cont = (r.json().get("continue") or {}).get("gsroffset")
        except Exception as e:
            print(f"  Commons error {e}", flush=True)
            return []
        if cont is not None:  # one continuation page for a deeper pool
            try:
                params["gsroffset"] = cont
                r2 = requests.get("https://commons.wikimedia.org/w/api.php",
                                  params=params, headers=HEADERS, timeout=30)
                if r2.status_code == 200:
                    pages.update((r2.json().get("query") or {}).get("pages") or {})
                time.sleep(2)
            except Exception:
                pass
        out = []
        for p in pages.values():
            ii = (p.get("imageinfo") or [{}])[0]
            if ii.get("mime") != "image/jpeg":
                continue
            if (ii.get("width") or 0) < 1600:
                continue
            title = p.get("title", "")
            if title_is_bad(title):
                continue
            out.append({"title": title.replace("File:", ""),
                        "url": ii.get("thumburl") or ii.get("url"),
                        "full": ii.get("url")})
        time.sleep(2)
        return out

    def pass_commons(self, cats):
        for slug, queries in cats.items():
            if count_cat(self.state, slug) >= self.target:
                continue
            print(f"== Commons fill {slug} (have {count_cat(self.state, slug)}) ==", flush=True)
            tasks, used = [], set()
            for q in queries:
                if count_cat(self.state, slug) >= self.target:
                    break
                for c in self.commons_candidates(q):
                    if c["url"] in self.seen_urls or c["url"] in used:
                        continue
                    used.add(c["url"])
                    tasks.append((c["url"], {
                        "source": "wikimedia-commons",
                        "source_url": c["full"],
                        "image_url": c["url"],
                        "title": c["title"],
                        "license": "see-commons-page",
                    }))
            self.fetch_candidates(slug, tasks)

def main():
    ap = argparse.ArgumentParser(description="Daily Qoutes photo library harvester")
    ap.add_argument("--target", type=int, default=10,
                    help="photos per category to reach (default 10)")
    ap.add_argument("--categories", default="",
                    help="comma-separated slugs (default: all)")
    ap.add_argument("--sources", default="both",
                    choices=["ov", "commons", "both"])
    ap.add_argument("--cycles", type=int, default=1,
                    help="repeat the passes this many times, riding "
                         "out rate-limit waves (default 1)")
    args = ap.parse_args()

    cats = CATEGORIES
    if args.categories:
        want = {s.strip() for s in args.categories.split(",") if s.strip()}
        cats = {k: v for k, v in CATEGORIES.items() if k in want}
        missing = want - set(cats)
        if missing:
            print(f"WARNING: unknown categories ignored: {missing}", flush=True)

    state = load_state()
    hv = Harvester(state, args.target)
    # The big CDNs rate-limit this network in waves (HTTP 429 for a
    # while, then fine again). A single pass can stall in a bad wave,
    # so cycle the passes: dedupe state makes re-tries cheap, hot-host
    # cooldowns expire between cycles, and the loop stops as soon as
    # every category reaches the target.
    for cycle in range(1, args.cycles + 1):
        if all(count_cat(state, s) >= args.target for s in cats):
            break
        if cycle > 1:
            print(f"--- cycle {cycle}: pausing 60s ---", flush=True)
            time.sleep(60)
        if args.sources in ("ov", "both"):
            hv.pass_openverse(cats)
        if args.sources in ("commons", "both"):
            hv.pass_commons(cats)
    print(f"TOTAL accepted records: {len(state['accepted'])}", flush=True)
    for slug in cats:
        print(f"  {slug}: {count_cat(state, slug)}", flush=True)

if __name__ == "__main__":
    main()
