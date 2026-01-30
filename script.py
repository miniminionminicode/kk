import os
import time
import random
import requests
import json
import re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================
# CONFIG (FROM SECRETS / ENV)
# =============================
BASE = os.getenv("BASE_URL")
if not BASE:
    raise RuntimeError("Missing BASE_URL secret")

_keywords_env = os.getenv("KEYWORDS")
if not _keywords_env:
    raise RuntimeError("Missing KEYWORDS secret")

KEYWORDS = [k.strip().lower() for k in _keywords_env.split(",") if k.strip()]

HEADERS = {
    "Referer": os.getenv("REFERER"),
    "Origin": os.getenv("ORIGIN"),
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
}

THREADS = int(os.getenv("THREADS", "3"))  # ✅ recommend 2-3 to avoid 500 overload
MASTER_JSON_FILE = "master_courses.json"
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "25"))

# retry / throttling knobs
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "0.8"))     # seconds
BACKOFF_CAP = float(os.getenv("BACKOFF_CAP", "10"))        # seconds
REQUEST_JITTER = float(os.getenv("REQUEST_JITTER", "0.15"))# seconds (per request)

# =============================
# HTTP
# =============================
session = requests.Session()

def ensure_list(x):
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        return [x]
    return []

def safe_get(url):
    """
    Retry on 5xx and network errors. Return [] on final failure.
    Prints status/body snippet on hard errors.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        # tiny jitter to reduce thundering herd
        if REQUEST_JITTER > 0:
            time.sleep(random.uniform(0, REQUEST_JITTER))

        try:
            r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            code = r.status_code

            # Retryable server errors
            if code in (500, 502, 503, 504):
                wait = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
                if attempt < MAX_RETRIES:
                    print(f"[!] {code} for {url} (attempt {attempt}/{MAX_RETRIES}) -> retry in {wait:.2f}s")
                    time.sleep(wait)
                    continue
                print(f"[!] {code} for {url} (attempt {attempt}/{MAX_RETRIES}) -> giving up")
                return []

            # Non-200, non-retryable
            if code != 200:
                print(f"[!] {code} for {url} | body: {r.text[:200]!r}")
                return []

            # Parse JSON
            try:
                return r.json()
            except Exception:
                print(f"[!] Non-JSON response for {url} | body: {r.text[:200]!r}")
                return []

        except requests.RequestException as e:
            wait = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
            if attempt < MAX_RETRIES:
                print(f"[!] Network error for {url}: {e} (attempt {attempt}/{MAX_RETRIES}) -> retry in {wait:.2f}s")
                time.sleep(wait)
                continue
            print(f"[!] Network error for {url}: {e} (attempt {attempt}/{MAX_RETRIES}) -> giving up")
            return []

# =============================
# STORAGE
# =============================
def load_master_json():
    try:
        with open(MASTER_JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except FileNotFoundError:
        return []
    except Exception as e:
        print(f"[!] Failed to read {MASTER_JSON_FILE}: {e}")
        return []

def save_master_json(data):
    with open(MASTER_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# =============================
# MERGE HELPERS (NO DELETE)
# =============================
def is_blank(x):
    return x in (None, "", [], {})

def merge_scalar_fill_only(existing_dict, k, new_val):
    if is_blank(existing_dict.get(k)) and not is_blank(new_val):
        existing_dict[k] = new_val

def merge_dict_fill_only(existing, new):
    for k, v in (new or {}).items():
        if isinstance(v, dict):
            if not isinstance(existing.get(k), dict):
                existing[k] = {}
            merge_dict_fill_only(existing[k], v)
        elif isinstance(v, list):
            if not isinstance(existing.get(k), list):
                existing[k] = []
        else:
            merge_scalar_fill_only(existing, k, v)

def merge_list_by_key(existing_list, new_list, key="id"):
    if not isinstance(existing_list, list):
        existing_list = []
    if not isinstance(new_list, list):
        return existing_list

    index = {}
    for idx, item in enumerate(existing_list):
        if isinstance(item, dict):
            item_id = item.get(key)
            if item_id not in (None, ""):
                index[str(item_id)] = idx

    for item in new_list:
        if not isinstance(item, dict):
            if item not in existing_list:
                existing_list.append(item)
            continue

        item_id = item.get(key)
        if item_id in (None, ""):
            if item not in existing_list:
                existing_list.append(item)
            continue

        sid = str(item_id)
        if sid in index:
            merge_dict_fill_only(existing_list[index[sid]], item)
        else:
            existing_list.append(item)
            index[sid] = len(existing_list) - 1

    return existing_list

def fingerprint(item):
    if not isinstance(item, dict):
        return str(item)

    for k in ("id", "notice_id", "_id", "update_id", "uid"):
        v = item.get(k)
        if v not in (None, "", [], {}):
            return f"{k}:{v}"

    title = item.get("title") or item.get("heading") or item.get("name") or ""
    body  = item.get("message") or item.get("description") or item.get("content") or ""
    ts    = (item.get("created_at") or item.get("createdAt") or
             item.get("published_at") or item.get("publishedAt") or
             item.get("date") or item.get("time") or "")

    if ts or title:
        body_short = body[:60] if isinstance(body, str) else str(body)[:60]
        return f"ts:{ts}|t:{title}|b:{body_short}"

    return "json:" + json.dumps(item, sort_keys=True, ensure_ascii=False)

def merge_list_by_fingerprint(existing_list, new_list):
    if not isinstance(existing_list, list):
        existing_list = []
    if not isinstance(new_list, list):
        return existing_list

    idx = {fingerprint(item): i for i, item in enumerate(existing_list)}

    for item in new_list:
        fp = fingerprint(item)
        if fp in idx:
            if isinstance(existing_list[idx[fp]], dict) and isinstance(item, dict):
                merge_dict_fill_only(existing_list[idx[fp]], item)
        else:
            existing_list.append(item)
            idx[fp] = len(existing_list) - 1

    return existing_list

def merge_course(existing_course, new_course):
    merge_dict_fill_only(existing_course, new_course)

    existing_course["classroom"] = merge_list_by_key(
        existing_course.get("classroom", []),
        new_course.get("classroom", []),
        key="id"
    )
    existing_course["live_classes"] = merge_list_by_key(
        existing_course.get("live_classes", []),
        new_course.get("live_classes", []),
        key="id"
    )
    existing_course["announcements"] = merge_list_by_fingerprint(
        existing_course.get("announcements", []),
        new_course.get("announcements", [])
    )

    existing_lessons = existing_course.get("lessons", [])
    if not isinstance(existing_lessons, list):
        existing_lessons = []
    new_lessons = new_course.get("lessons", [])
    if not isinstance(new_lessons, list):
        new_lessons = []

    lesson_idx = {}
    for i, l in enumerate(existing_lessons):
        if isinstance(l, dict):
            lid = l.get("lesson_id")
            if lid not in (None, ""):
                lesson_idx[str(lid)] = i

    for lesson in new_lessons:
        if not isinstance(lesson, dict):
            if lesson not in existing_lessons:
                existing_lessons.append(lesson)
            continue

        lid = lesson.get("lesson_id")
        if lid in (None, ""):
            if lesson not in existing_lessons:
                existing_lessons.append(lesson)
            continue

        lid = str(lid)
        if lid in lesson_idx:
            target = existing_lessons[lesson_idx[lid]]
            merge_dict_fill_only(target, lesson)
            target["videos"] = merge_list_by_key(
                target.get("videos", []),
                lesson.get("videos", []),
                key="id"
            )
            if isinstance(target.get("videos"), list):
                target["lesson_count"] = len(target["videos"])
        else:
            if isinstance(lesson.get("videos"), list):
                lesson["lesson_count"] = len(lesson["videos"])
            existing_lessons.append(lesson)
            lesson_idx[lid] = len(existing_lessons) - 1

    existing_course["lessons"] = existing_lessons
    existing_course["lesson_count"] = sum(
        len(l.get("videos", [])) for l in existing_lessons if isinstance(l, dict)
    )

def upsert_course(master_json, course_id, new_course):
    for c in master_json:
        if c.get("course_id") == course_id:
            merge_course(c, new_course)
            return
    master_json.append(new_course)

# =============================
# STEP 1: FETCH & FILTER COURSES
# =============================
batches = ensure_list(safe_get(f"{BASE}/batches"))
if not batches:
    raise SystemExit("[!] No batches fetched")

keyword_patterns = []
for kw in KEYWORDS:
    m = re.match(r"(\d+)\s+(.*)", kw)
    if m:
        n, w = m.groups()
        keyword_patterns.append(re.compile(rf"\b{n}(?:st|nd|rd|th)?\s*{re.escape(w)}\b", re.I))
    else:
        keyword_patterns.append(re.compile(rf"\b{re.escape(kw)}\b", re.I))

filtered_courses = []
for item in batches:
    if not isinstance(item, dict):
        continue
    title = (item.get("title") or "").lower()
    if not any(p.search(title) for p in keyword_patterns):
        continue
    filtered_courses.append({
        "id": item.get("id"),
        "title": item.get("title"),
        "image_large": item.get("image_large"),
        "image_thumb": item.get("image_thumb"),
    })

print(f"\nTotal courses matched: {len(filtered_courses)}\n")
if not filtered_courses:
    raise SystemExit("[!] No courses matched your KEYWORDS")

# =============================
# STEP 2: FETCH DETAILS
# =============================
def fetch_course_details(course, rank, total):
    cid = course["id"]
    cname = course.get("title", "")

    out = {
        "ranking": rank,
        "course_id": str(cid) if cid is not None else None,
        "course_name": cname,
        "image_large": course.get("image_large"),
        "image_thumb": course.get("image_thumb"),
        "classroom": [],
        "lessons": [],
        "live_classes": [],
        "announcements": [],
        "lesson_count": 0,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    classroom = ensure_list(safe_get(f"{BASE}/classroom/{cid}"))
    out["classroom"] = classroom

    for cls in classroom:
        if not isinstance(cls, dict):
            continue
        lesson_group_id = cls.get("id")
        if not lesson_group_id:
            continue

        lessons = ensure_list(safe_get(f"{BASE}/lesson/{lesson_group_id}"))

        for l in lessons:
            if not isinstance(l, dict):
                continue

            videos = []
            for v in (l.get("videos") or []):
                if not isinstance(v, dict):
                    continue

                vid = v.get("id")
                vd = safe_get(f"{BASE}/video/{vid}") if vid else {}
                if not isinstance(vd, dict):
                    vd = {}

                videos.append({
                    "id": str(vid) if vid is not None else None,
                    "name": v.get("name", ""),
                    "published_at": v.get("published_at", ""),
                    "thumb": v.get("thumb", ""),
                    "type": v.get("type", ""),
                    "pdfs": v.get("pdfs", []) or [],
                    "m3u": vd.get("video_url", "") or "",
                    "yt": vd.get("hd_video_url", "") or "",
                })

            out["lessons"].append({
                "lesson_id": str(l.get("id")) if l.get("id") is not None else None,
                "lesson_name": l.get("name", ""),
                "lesson_count": len(videos),
                "videos": videos,
                "notes": l.get("notes", []) or [],
            })

    out["live_classes"] = ensure_list(safe_get(f"{BASE}/today/{cid}"))
    out["announcements"] = ensure_list(safe_get(f"{BASE}/updates/{cid}"))

    print(f"[+] Fetched {cname} ({rank}/{total}) | classroom={len(out['classroom'])} lessons={len(out['lessons'])} notices={len(out['announcements'])}")
    return out

# =============================
# STEP 3: RUN & MERGE (merge in main thread)
# =============================
master_json = load_master_json()
results = []

with ThreadPoolExecutor(max_workers=THREADS) as ex:
    futures = [
        ex.submit(fetch_course_details, c, i + 1, len(filtered_courses))
        for i, c in enumerate(filtered_courses)
    ]
    for f in as_completed(futures):
        try:
            results.append(f.result())
        except Exception as e:
            print(f"[!] Worker error: {e}")

for r in results:
    cid = r.get("course_id")
    if cid:
        upsert_course(master_json, cid, r)

# =============================
# STEP 4: SAVE
# =============================
save_master_json(master_json)
print("\n✅ master_courses.json saved\n")
