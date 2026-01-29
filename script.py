import os
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
    "Referer": os.getenv("REFERER", "https://example.com/"),
    "Origin": os.getenv("ORIGIN", "https://example.com"),
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
}

THREADS = int(os.getenv("THREADS", "5"))
MASTER_JSON_FILE = "master_courses.json"
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))

# =============================
# HELPERS
# =============================

session = requests.Session()

def safe_get(url):
    try:
        r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[!] Error fetching {url}: {e}")
        return []

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None

def load_master_json():
    try:
        with open(MASTER_JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except FileNotFoundError:
        return []

def save_master_json(data):
    with open(MASTER_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def is_blank(x):
    return x in (None, "", [], {})

def merge_scalar_fill_only(existing_dict, k, new_val):
    if is_blank(existing_dict.get(k)) and not is_blank(new_val):
        existing_dict[k] = new_val

def merge_dict_fill_only(existing, new):
    for k, v in new.items():
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

    index = {str(i.get(key)): idx for idx, i in enumerate(existing_list) if isinstance(i, dict)}

    for item in new_list:
        if not isinstance(item, dict):
            if item not in existing_list:
                existing_list.append(item)
            continue

        sid = str(item.get(key))
        if sid in index:
            merge_dict_fill_only(existing_list[index[sid]], item)
        else:
            existing_list.append(item)
            index[sid] = len(existing_list) - 1

    return existing_list

def merge_lessons(existing_course, new_course):
    merge_dict_fill_only(existing_course, new_course)

    existing_course["classroom"] = merge_list_by_key(
        existing_course.get("classroom", []),
        new_course.get("classroom", [])
    )
    existing_course["live_classes"] = merge_list_by_key(
        existing_course.get("live_classes", []),
        new_course.get("live_classes", [])
    )
    existing_course["announcements"] = merge_list_by_key(
        existing_course.get("announcements", []),
        new_course.get("announcements", [])
    )

    existing_lessons = existing_course.get("lessons", [])
    new_lessons = new_course.get("lessons", [])

    lesson_idx = {l.get("lesson_id"): i for i, l in enumerate(existing_lessons) if isinstance(l, dict)}

    for lesson in new_lessons:
        lid = lesson.get("lesson_id")
        if lid in lesson_idx:
            target = existing_lessons[lesson_idx[lid]]
            merge_dict_fill_only(target, lesson)
            target["videos"] = merge_list_by_key(
                target.get("videos", []),
                lesson.get("videos", [])
            )
        else:
            existing_lessons.append(lesson)

    existing_course["lessons"] = existing_lessons

    existing_course["lesson_count"] = sum(
        len(l.get("videos", [])) for l in existing_lessons if isinstance(l, dict)
    )

def upsert_course(master_json, course_id, new_course_data):
    for course in master_json:
        if course.get("course_id") == course_id:
            merge_lessons(course, new_course_data)
            return
    master_json.append(new_course_data)

# =============================
# STEP 1: FETCH & FILTER COURSES
# =============================

batches = safe_get(f"{BASE}/batches")
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
    title = (item.get("title") or "").lower()
    if not any(p.search(title) for p in keyword_patterns):
        continue

    filtered_courses.append({
        "id": item.get("id"),
        "title": item.get("title"),
        "start_at": item.get("start_at"),
        "end_at": item.get("end_at"),
        "image_large": item.get("image_large"),
        "image_thumb": item.get("image_thumb"),
    })

print(f"\nTotal courses matched: {len(filtered_courses)}\n")

# =============================
# STEP 2: FETCH DETAILS
# =============================

def fetch_course_details(course, rank, total):
    cid = course["id"]

    out = {
        "ranking": rank,
        "course_id": cid,
        "course_name": course["title"],
        "image_large": course.get("image_large"),
        "image_thumb": course.get("image_thumb"),
        "classroom": [],
        "lessons": [],
        "live_classes": [],
        "announcements": [],
        "lesson_count": 0,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    classroom = safe_get(f"{BASE}/classroom/{cid}") or []
    out["classroom"] = classroom

    for cls in classroom:
        lid = (cls or {}).get("id")
        if not lid:
            continue

        lessons = safe_get(f"{BASE}/lesson/{lid}")
        if not isinstance(lessons, list):
            lessons = [lessons]

        for l in lessons:
            videos = []
            for v in l.get("videos", []) or []:
                vid = v.get("id")
                vd = safe_get(f"{BASE}/video/{vid}") if vid else {}
                videos.append({
                    "id": str(vid),
                    "name": v.get("name", ""),
                    "published_at": v.get("published_at", ""),
                    "thumb": v.get("thumb", ""),
                    "type": v.get("type", ""),
                    "pdfs": v.get("pdfs", []) or [],
                    "m3u": (vd or {}).get("video_url", ""),
                    "yt": (vd or {}).get("hd_video_url", ""),
                })

            out["lessons"].append({
                "lesson_id": str(l.get("id")),
                "lesson_name": l.get("name", ""),
                "lesson_count": len(videos),
                "videos": videos,
                "notes": l.get("notes", []) or [],
            })

    out["live_classes"] = safe_get(f"{BASE}/today/{cid}") or []
    out["announcements"] = safe_get(f"{BASE}/updates/{cid}") or []

    print(f"[+] Fetched {course['title']} ({rank}/{total})")
    return out

# =============================
# STEP 3: RUN & MERGE
# =============================

master_json = load_master_json()
results = []

with ThreadPoolExecutor(max_workers=THREADS) as ex:
    futures = [
        ex.submit(fetch_course_details, c, i + 1, len(filtered_courses))
        for i, c in enumerate(filtered_courses)
    ]
    for f in as_completed(futures):
        results.append(f.result())

for r in results:
    upsert_course(master_json, r["course_id"], r)

# =============================
# STEP 4: SAVE
# =============================

save_master_json(master_json)
print("\n✅ master_courses.json saved\n")
