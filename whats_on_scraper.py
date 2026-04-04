#!/usr/bin/env python3
"""
WTW Cinemas What's On scraper.

Scrapes whats-on pages across WTW cinemas, optionally enriches with TMDb data,
writes whats_on_data.json and regenerates docs/index.html (and assets) on every run.
Commits (e.g. in CI) are driven by fingerprint change.
"""
import hashlib
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HTTP_TIMEOUT = 60
HTTP_RETRIES = 3
HTTP_RETRY_DELAY = 1
HTTP_RETRY_MULTIPLIER = 2
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
)

WTW_BASE = "https://wtwcinemas.co.uk"
# WTW cinemas are in England; use UK civil date for "Today", tabs, and now/coming split.
DISPLAY_TZ = ZoneInfo("Europe/London")
SHOWTIMES_JSON_VERSION = 1
WTW_CINEMAS = {
    "st-austell": {
        "enabled": True,
        "name": "White River Cinema, St Austell",
        "url": "https://wtwcinemas.co.uk/st-austell/whats-on/",
    },
    "newquay": {
        "enabled": True,
        "name": "Lighthouse Cinema, Newquay",
        "url": "https://wtwcinemas.co.uk/newquay/whats-on/",
    },
    "truro": {
        "enabled": True,
        "name": "Plaza Cinema, Truro",
        "url": "https://wtwcinemas.co.uk/truro/whats-on/",
    },
    "wadebridge": {
        "enabled": True,
        "name": "Regal Cinema, Wadebridge",
        "url": "https://wtwcinemas.co.uk/wadebridge/whats-on/",
    },
}

DATA_FILE = "whats_on_data.json"
FINGERPRINT_FILE = ".whats_on_fingerprint"
TMDB_CACHE_FILE = ".tmdb_cache.json"
CINEMA_FAILURE_STATE_FILE = ".cinema_failure_state.json"
SITE_DIR = "docs"  # GitHub Pages: only /(root) and /docs are offered; use Deploy from branch → /docs
POSTERS_DIR = "docs/posters"
CERTS_DIR = "docs/certs"
WTW_CERT_BASE = "https://wtwcinemas.co.uk/wp-content/themes/wtw-2017/dist/images"
CERT_IMAGES = {"U": "cert-u.png", "PG": "cert-pg.png", "12A": "cert-12a.png", "15": "cert-15.png", "18": "cert-18.png"}
ICONS_DIR = "docs/icons"
WTW_3D_ICON_URL = "https://wtwcinemas.co.uk/wp-content/uploads/2022/11/3D-Performance.png"
TMDB_CACHE_DAYS = 30
TMDB_DELAY_SEC = 0.2
TMDB_EMPTY_CACHE_TTL_DAYS = 7
POSTER_PLACEHOLDER_REL = "posters/placeholder.svg"
ENRICHMENT_FIELDS = ("poster_url", "trailer_url", "vote_average", "genres", "imdb_id", "overview", "director", "writer", "cast")


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        logging.getLogger(__name__).warning("Invalid %s value %r, using default %d", name, raw, default)
        return default
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_csv(name: str, default: str = "") -> List[str]:
    raw = (os.environ.get(name) or default or "").strip()
    if not raw:
        return []
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


INITIAL_SHOWINGS_VISIBLE = _env_int("WTW_INITIAL_SHOWINGS_VISIBLE", default=10, minimum=3, maximum=30)
MIN_TOTAL_FILMS = _env_int("WTW_MIN_TOTAL_FILMS", default=1, minimum=0, maximum=10000)
MIN_TOTAL_SHOWTIMES = _env_int("WTW_MIN_TOTAL_SHOWTIMES", default=1, minimum=0, maximum=100000)
MIN_FILMS_PER_CINEMA = _env_int("WTW_MIN_FILMS_PER_CINEMA", default=0, minimum=0, maximum=10000)
FAIL_ON_MARKUP_DRIFT = _env_bool("WTW_FAIL_ON_MARKUP_DRIFT", default=False)
HEALTH_MIN_TOTAL_FILMS = _env_int("HEALTH_MIN_TOTAL_FILMS", default=MIN_TOTAL_FILMS, minimum=0, maximum=10000)
HEALTH_MIN_TOTAL_SHOWTIMES = _env_int("HEALTH_MIN_TOTAL_SHOWTIMES", default=MIN_TOTAL_SHOWTIMES, minimum=0, maximum=100000)
HEALTH_MIN_CINEMAS_WITH_FILMS = _env_int("HEALTH_MIN_CINEMAS_WITH_FILMS", default=0, minimum=0, maximum=1000)
HEALTH_MIN_NOW_SHOWING_FILMS = _env_int("HEALTH_MIN_NOW_SHOWING_FILMS", default=0, minimum=0, maximum=10000)
HEALTH_MAX_MARKUP_SUSPECT_CINEMAS = _env_int("HEALTH_MAX_MARKUP_SUSPECT_CINEMAS", default=1000, minimum=0, maximum=1000)
HEALTHCHECK_ENFORCE = _env_bool("HEALTHCHECK_ENFORCE", default=FAIL_ON_MARKUP_DRIFT)
HEALTH_EXCLUDED_CINEMAS = set(_env_csv("HEALTH_EXCLUDED_CINEMAS", default=""))
MAX_CONSECUTIVE_CINEMA_FAILURES = _env_int("MAX_CONSECUTIVE_CINEMA_FAILURES", default=2, minimum=1, maximum=30)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
HTTP_SESSION = requests.Session()


def uk_today_iso() -> str:
    return datetime.now(DISPLAY_TZ).date().isoformat()


def showtimes_compact_json(showtimes: List[Dict[str, Any]]) -> str:
    """Compact JSON for embedded HTML: shorter than list of objects (field names omitted per row)."""
    rows = [
        [
            st.get("date", ""),
            st.get("time", ""),
            st.get("screen", ""),
            st.get("cinema_name", ""),
            st.get("booking_url", ""),
            st.get("tags") or [],
        ]
        for st in showtimes
    ]
    return json.dumps(
        {"v": SHOWTIMES_JSON_VERSION, "r": rows},
        ensure_ascii=False,
        separators=(",", ":"),
    ).replace("</", "<\\/")


# BBFC rating pattern in titles: (15), (12A), (PG), (18), (U), (R18), (with subtitles) etc.
RATING_PATTERN = re.compile(r"\((\d+A?|U|PG|R18)\)", re.IGNORECASE)
SUBTITLE_SUFFIX = re.compile(r"\s*\(with subtitles\)\s*$", re.IGNORECASE)
AUTISM_FRIENDLY_SUFFIX = re.compile(r"\s+autism\s+friendly\s+screening\s*$", re.IGNORECASE)
# Format suffix: " - HFR 3D" (high frame rate 3D) is not part of the movie name
FORMAT_SUFFIX = re.compile(r"\s*-\s*HFR\s*3D\s*$", re.IGNORECASE)


def get_selected_cinemas() -> Dict[str, Dict[str, Any]]:
    """Return cinemas selected for this run.

    Selection order:
    1. `WTW_ENABLED_CINEMAS` env (comma-separated slugs, e.g. "st-austell,truro")
    2. `enabled: True` flags in `WTW_CINEMAS` (default: all enabled)
    """
    selected_env = (os.environ.get("WTW_ENABLED_CINEMAS") or "").strip().lower()
    if not selected_env or selected_env == "all":
        selected = {slug: info for slug, info in WTW_CINEMAS.items() if info.get("enabled", True)}
    else:
        requested = [s.strip() for s in selected_env.split(",") if s.strip()]
        selected = {slug: WTW_CINEMAS[slug] for slug in requested if slug in WTW_CINEMAS}
        unknown = [slug for slug in requested if slug not in WTW_CINEMAS]
        if unknown:
            logger.warning("Ignoring unknown cinema slug(s) in WTW_ENABLED_CINEMAS: %s", ", ".join(unknown))
    if not selected:
        raise RuntimeError("No cinemas selected. Enable at least one in WTW_CINEMAS or set WTW_ENABLED_CINEMAS.")
    return selected


def fetch_with_retries(url: str, retries: int = HTTP_RETRIES, timeout: int = HTTP_TIMEOUT) -> requests.Response:
    """Fetch URL with exponential backoff on failure."""
    headers = {"User-Agent": USER_AGENT}
    delay = HTTP_RETRY_DELAY
    for attempt in range(retries):
        try:
            r = HTTP_SESSION.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            logger.warning("Attempt %d failed: %s", attempt + 1, e)
            if attempt == retries - 1:
                raise
            time.sleep(delay)
            delay *= HTTP_RETRY_MULTIPLIER
    raise requests.RequestException("Max retries exceeded")


def strip_format_suffix(title: str) -> str:
    """Remove format suffixes like ' - HFR 3D' (high frame rate 3D) from the end of a title."""
    return FORMAT_SUFFIX.sub("", title).strip().strip(" -")


def extract_search_title(title: str) -> str:
    """Strip age rating, 'with subtitles', 'autism friendly screening' for search/links. E.g. 'GOAT AUTISM FRIENDLY SCREENING' -> 'GOAT'."""
    t = strip_format_suffix(title)
    t = SUBTITLE_SUFFIX.sub("", t).strip()
    t = AUTISM_FRIENDLY_SUFFIX.sub("", t).strip()
    t = RATING_PATTERN.sub("", t).strip()
    return t.strip(" -")


def extract_bbfc_rating(title: str) -> Optional[str]:
    """Extract BBFC rating from title: (15) -> 15, (12A) -> 12A, (PG) -> PG, etc."""
    m = RATING_PATTERN.search(title)
    if m:
        return m.group(1).upper().replace("R18", "R18")
    return None


def format_runtime(minutes: Optional[int]) -> str:
    """Format runtime as '121 min (2 hours 1 min)' or '90 min (1 hour 30 mins)'."""
    if not minutes:
        return ""
    parts = []
    if minutes >= 60:
        h = minutes // 60
        parts.append(f"{h} hour{'s' if h != 1 else ''}")
    m = minutes % 60
    if m > 0:
        parts.append(f"{m} min{'s' if m != 1 else ''}")
    if not parts:
        return f"{minutes} min"
    return " ".join(parts)


def parse_runtime_minutes(text: str) -> Optional[int]:
    """Parse '113 minutes' or 'Running time:113 minutes' -> 113."""
    m = re.search(r"(\d+)\s*minutes?", text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_uk_date(text: str, scrape_date: datetime) -> Optional[str]:
    """Parse 'Today 8 February 2026', 'Tomorrow 9 February', 'Tuesday 10 February 2026' -> YYYY-MM-DD."""
    text = text.strip()
    today = scrape_date.date()
    # "Today 8 February 2026" or "Today\n8 February 2026"
    if "today" in text.lower():
        return today.isoformat()
    if "tomorrow" in text.lower():
        return (today + timedelta(days=1)).isoformat()
    # "Tuesday 10 February 2026" or "Tuesday 10 February"
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text)
    if m:
        day, month_str, year = int(m.group(1)), m.group(2), int(m.group(3))
        try:
            dt = datetime.strptime(f"{day} {month_str} {year}", "%d %B %Y")
            return dt.date().isoformat()
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)(?:\s|$)", text)
    if m:
        day, month_str = int(m.group(1)), m.group(2)
        year = scrape_date.year
        try:
            dt = datetime.strptime(f"{day} {month_str} {year}", "%d %B %Y")
            if dt.date() < today:
                dt = datetime.strptime(f"{day} {month_str} {year + 1}", "%d %B %Y")
            return dt.date().isoformat()
        except ValueError:
            pass
    return None


def load_tmdb_cache() -> Dict[str, Dict]:
    """Load TMDb cache; drop expired entries."""
    if not Path(TMDB_CACHE_FILE).exists():
        return {}
    try:
        with open(TMDB_CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Cache load failed: %s", e)
        return {}
    cutoff = (datetime.now() - timedelta(days=TMDB_CACHE_DAYS)).isoformat()
    return {k: v for k, v in cache.items() if v.get("cached_at", "") > cutoff}


def save_tmdb_cache(cache: Dict[str, Dict]) -> None:
    """Persist TMDb cache."""
    try:
        with open(TMDB_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except OSError as e:
        logger.warning("Cache save failed: %s", e)


def load_cinema_failure_state() -> Dict[str, int]:
    """Load per-cinema consecutive scrape failures."""
    path = Path(CINEMA_FAILURE_STATE_FILE)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {}
        return {str(k): int(v) for k, v in payload.items() if str(v).isdigit() or isinstance(v, int)}
    except Exception:
        return {}


def save_cinema_failure_state(state: Dict[str, int]) -> None:
    """Persist per-cinema consecutive scrape failures."""
    try:
        Path(CINEMA_FAILURE_STATE_FILE).write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    except OSError as e:
        logger.warning("Cinema failure state save failed: %s", e)


def tmdb_get_json(url: str, params: Dict[str, Any], max_retries: int = 4) -> Dict[str, Any]:
    """TMDb GET with 429-aware retry/backoff."""
    delay = 1.0
    for attempt in range(max_retries):
        resp = HTTP_SESSION.get(url, params=params, timeout=10)
        if resp.status_code == 429:
            retry_after_raw = resp.headers.get("Retry-After", "")
            try:
                retry_after = float(retry_after_raw) if retry_after_raw else delay
            except ValueError:
                retry_after = delay
            logger.warning("TMDb rate-limited (429). Retrying in %.1fs (attempt %d/%d)", retry_after, attempt + 1, max_retries)
            time.sleep(retry_after)
            delay = min(delay * 2, 16)
            continue
        if 500 <= resp.status_code < 600 and attempt < max_retries - 1:
            logger.warning("TMDb server error %s. Retrying in %.1fs (attempt %d/%d)", resp.status_code, delay, attempt + 1, max_retries)
            time.sleep(delay)
            delay = min(delay * 2, 16)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"TMDb request failed after {max_retries} retries: {url}")


def slug_from_film_url(url: str) -> str:
    """Extract slug from film URL for cache key. E.g. /film/send-help/?screen=truro -> send-help."""
    if not url:
        return ""
    path = url.split("?")[0].rstrip("/")
    return path.split("/")[-1] if "/" in path else path


def _download_cert_images() -> None:
    """Download WTW age-rating cert images so we can use them without hotlinking."""
    Path(CERTS_DIR).mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": USER_AGENT, "Referer": WTW_BASE + "/"}
    for rating, filename in CERT_IMAGES.items():
        path = Path(CERTS_DIR) / filename
        if path.exists():
            continue
        url = f"{WTW_CERT_BASE}/{filename}"
        try:
            r = HTTP_SESSION.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            path.write_bytes(r.content)
            logger.info("Downloaded cert %s", filename)
        except Exception as e:
            logger.warning("Cert download failed %s: %s", filename, e)


def _download_3d_icon() -> None:
    """Download WTW 3D overlay icon for poster badges."""
    path = Path(ICONS_DIR) / "3D-Performance.png"
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = HTTP_SESSION.get(WTW_3D_ICON_URL, headers={"User-Agent": USER_AGENT, "Referer": WTW_BASE + "/"}, timeout=10)
        r.raise_for_status()
        path.write_bytes(r.content)
        logger.info("Downloaded 3D icon")
    except Exception as e:
        logger.warning("3D icon download failed: %s", e)


def _ensure_placeholder_poster() -> None:
    """Ensure a local placeholder poster exists for films without a TMDb poster."""
    path = Path(SITE_DIR) / POSTER_PLACEHOLDER_REL
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="420" height="630" viewBox="0 0 420 630" role="img" aria-label="Poster unavailable">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#111827"/>
      <stop offset="100%" stop-color="#1f2937"/>
    </linearGradient>
  </defs>
  <rect width="420" height="630" fill="url(#bg)"/>
  <rect x="24" y="24" width="372" height="582" rx="18" ry="18" fill="none" stroke="#334155" stroke-width="2"/>
  <circle cx="210" cy="250" r="68" fill="none" stroke="#64748b" stroke-width="8"/>
  <path d="M210 200v60m0 45h.01" stroke="#94a3b8" stroke-width="10" stroke-linecap="round"/>
  <text x="210" y="390" text-anchor="middle" fill="#cbd5e1" font-family="Arial, sans-serif" font-size="26" font-weight="700">Poster unavailable</text>
  <text x="210" y="425" text-anchor="middle" fill="#94a3b8" font-family="Arial, sans-serif" font-size="18">WTW Cinemas</text>
</svg>
"""
    path.write_text(svg, encoding="utf-8")


def _parse_cached_at(value: str) -> Optional[datetime]:
    """Parse cached_at timestamp safely."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _download_poster(url: str, slug: str) -> str:
    """Download poster image and save under POSTERS_DIR; return relative path or '' on failure."""
    if not url or not url.startswith("http"):
        return ""
    slug = re.sub(r"[^a-z0-9-]", "", slug.lower()) or "poster"
    posters_dir = Path(POSTERS_DIR)
    posters_dir.mkdir(parents=True, exist_ok=True)
    # Reuse any existing poster for this slug to avoid unnecessary downloads.
    existing = sorted(posters_dir.glob(f"{slug}.*"))
    if existing:
        return f"posters/{existing[0].name}"
    ext = "jpg"
    if ".webp" in url.lower():
        ext = "webp"
    elif ".png" in url.lower():
        ext = "png"
    path = posters_dir / f"{slug}.{ext}"
    if path.exists():
        return f"posters/{slug}.{ext}"
    try:
        headers = {"User-Agent": USER_AGENT}
        if "wtwcinemas.co.uk" in url:
            headers["Referer"] = WTW_BASE + "/"
        r = HTTP_SESSION.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        path.write_bytes(r.content)
        return f"posters/{slug}.{ext}"  # relative to SITE_DIR for HTML
    except Exception as e:
        logger.warning("Poster download failed %s: %s", url[:50], e)
        return ""


def _tmdb_cache_key(film: Dict[str, Any]) -> str:
    """Stable cache key by search title so e.g. 'Send Help' and 'Send Help (with subtitles)' share one TMDb entry."""
    search_title = film.get("search_title") or extract_search_title(film.get("title", ""))
    if not search_title:
        slug = film.get("film_slug") or slug_from_film_url(film.get("film_url", ""))
        return slug or "unknown"
    return re.sub(r"[^a-z0-9]+", "-", search_title.lower()).strip("-") or "unknown"


def _normalize_title_for_match(title: str) -> str:
    """Normalize title for TMDb result matching: lower, collapse punctuation to space."""
    if not title:
        return ""
    return re.sub(r"[\s\-:]+", " ", title.lower()).strip()


def _pick_best_tmdb_result(results: List[Dict], search_title: str) -> Optional[Dict]:
    """Pick the TMDb result that best matches our search title (e.g. 'Avatar: Fire and Ash' not 'Avatar' 2009)."""
    if not results or not search_title:
        return results[0] if results else None
    norm_search = _normalize_title_for_match(search_title)
    if not norm_search:
        return results[0]
    best = None
    best_score = -1
    for r in results:
        title = (r.get("title") or "").strip()
        norm_title = _normalize_title_for_match(title)
        if norm_title == norm_search:
            return r  # Exact match
        score = 0
        if norm_search in norm_title:
            score = 90  # Our search is contained in result title (e.g. result "Avatar: Fire and Ash")
        elif norm_title in norm_search:
            score = 30  # Result is shorter (e.g. "Avatar" when we want "Avatar: Fire and Ash") - prefer longer
        else:
            # Partial: prefer recent films for sequel-style titles
            release = (r.get("release_date") or "")[:4]
            try:
                year = int(release) if release else 0
                if year >= 2020:
                    score = 50
                else:
                    score = 10
            except ValueError:
                score = 10
        if score > best_score:
            best_score = score
            best = r
    return best if best is not None else results[0]


def _event_cinema_fallback_queries(title: str) -> List[str]:
    """For RBO/event cinema titles, return TMDb search queries to try in order. RBO uses 'Royal Ballet & Opera 2025/26: X'; Met Opera also tries 'The Metropolitan Opera: X'."""
    if not title or "RBO" not in title.upper():
        return []
    # Match: "PRODUCTION - RBO 2025-26" or "PRODUCTION - The MET Opera - RBO 2025-26"
    m = re.search(r"^(.+?)\s+-\s+(?:The MET Opera\s+-\s+)?RBO\s+2025-26", title, re.IGNORECASE)
    if not m:
        return []
    production = m.group(1).strip().title()
    if not production:
        return []
    queries = [f"Royal Ballet & Opera 2025/26: {production}"]
    # Met Opera titles: TMDb lists as "The Metropolitan Opera: Eugene Onegin"
    if "MET Opera" in title or "Met Opera" in title:
        queries.append(f"The Metropolitan Opera: {production}")
    return queries


def _empty_tmdb_entry() -> Dict[str, Any]:
    """Empty cache entry so we never refetch after a miss or error."""
    return {
        "poster_url": "",
        "trailer_url": "",
        "vote_average": None,
        "genres": [],
        "imdb_id": "",
        "overview": "",
        "director": "",
        "writer": "",
        "cast": "",
        "cached_at": datetime.now().isoformat(),
    }


def enrich_film_tmdb(
    film: Dict[str, Any],
    api_key: str,
    cache: Dict[str, Dict],
) -> None:
    """Enrich film with poster, trailer, vote_average, genres, imdb_id from TMDb. Modifies film in place."""
    search_title = film.get("search_title") or extract_search_title(film.get("title", ""))
    if not search_title:
        return
    cache_key = _tmdb_cache_key(film)

    if cache_key in cache:
        entry = cache[cache_key]
        cached_at = _parse_cached_at(entry.get("cached_at", ""))
        stale_empty_cache = False
        if not entry.get("poster_url"):
            if not cached_at:
                stale_empty_cache = True
            else:
                stale_empty_cache = datetime.now(cached_at.tzinfo) - cached_at >= timedelta(days=TMDB_EMPTY_CACHE_TTL_DAYS)
        # Refetch if we have poster but no genres (backfill for old cache entries)
        if (not (entry.get("genres") or [])) and entry.get("poster_url"):
            pass  # Fall through to API call to get genres (and refresh cache)
        # Retry empty-cache misses after TTL so new TMDb records can be picked up later
        elif stale_empty_cache:
            pass
        # Retry with event cinema fallback if cache has no poster and this is an RBO title
        elif not entry.get("poster_url") and _event_cinema_fallback_queries(film.get("title", "")):
            pass  # Fall through to API call with fallback
        else:
            film["poster_url"] = entry.get("poster_url") or film.get("poster_url") or ""
            film["trailer_url"] = entry.get("trailer_url") or ""
            film["vote_average"] = entry.get("vote_average")
            film["genres"] = entry.get("genres") or []
            film["imdb_id"] = entry.get("imdb_id") or ""
            film["overview"] = entry.get("overview") or ""
            film["director"] = entry.get("director") or ""
            film["writer"] = entry.get("writer") or ""
            film["cast"] = entry.get("cast") or film.get("cast") or ""
            return

    time.sleep(TMDB_DELAY_SEC)
    try:
        search_url = "https://api.themoviedb.org/3/search/movie"
        data = tmdb_get_json(
            search_url,
            params={"api_key": api_key, "query": search_title, "language": "en-GB"},
        )
        results = data.get("results") or []
        match_title = search_title
        # Event cinema fallback: try RBO/Met Opera queries when full title returns nothing
        fallback_queries = _event_cinema_fallback_queries(film.get("title", "")) if not results else []
        for fq in fallback_queries:
            time.sleep(TMDB_DELAY_SEC)
            data = tmdb_get_json(
                search_url,
                params={"api_key": api_key, "query": fq, "language": "en-GB"},
            )
            results = data.get("results") or []
            if results:
                match_title = fq
                break
        if not results:
            cache[cache_key] = _empty_tmdb_entry()
            return
        chosen = _pick_best_tmdb_result(results, match_title)
        if not chosen:
            cache[cache_key] = _empty_tmdb_entry()
            return
        # If best match has no poster, try next results that do (e.g. TMDb sometimes omits poster on new entries)
        movie_id = chosen.get("id")
        if not chosen.get("poster_path") and results:
            for r in results:
                if r.get("poster_path") and _normalize_title_for_match(r.get("title") or "") == _normalize_title_for_match(chosen.get("title") or ""):
                    chosen = r
                    movie_id = r.get("id")
                    break
        if not movie_id:
            cache[cache_key] = _empty_tmdb_entry()
            return

        time.sleep(TMDB_DELAY_SEC)
        detail_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        movie = tmdb_get_json(
            detail_url,
            params={"api_key": api_key, "append_to_response": "videos,credits", "language": "en-GB"},
        )

        poster_path = (movie.get("poster_path") or "").lstrip("/")
        poster_url = f"https://image.tmdb.org/t/p/w342/{poster_path}" if poster_path else ""

        trailer_url = ""
        for v in (movie.get("videos", {}).get("results") or []):
            if v.get("site") == "YouTube" and v.get("type", "").lower() in ("trailer", "teaser"):
                key = v.get("key")
                if key:
                    trailer_url = f"https://www.youtube.com/watch?v={key}"
                    break

        GENRE_MAP = {
            28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
            99: "Documentary", 18: "Drama", 10751: "Family", 14: "Fantasy", 36: "History",
            27: "Horror", 10402: "Music", 9648: "Mystery", 10749: "Romance", 878: "Science Fiction",
            10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western",
        }
        genre_list = movie.get("genres") or []
        genres = [g.get("name", "").strip() for g in genre_list if g.get("name")] if genre_list else []
        if not genres:
            # Detail response may omit genres; use genre_ids from detail or from search result
            genre_ids = movie.get("genre_ids") or chosen.get("genre_ids") or []
            genres = [GENRE_MAP[g] for g in genre_ids if g in GENRE_MAP]

        imdb_id = movie.get("imdb_id") or ""
        overview = (movie.get("overview") or "").strip()

        # Credits: director, writer, cast (with character names)
        director_names: List[str] = []
        writer_names: List[str] = []
        cast_parts: List[str] = []
        credits = movie.get("credits") or {}
        for c in credits.get("crew") or []:
            job = (c.get("job") or "").strip()
            name = (c.get("name") or "").strip()
            if not name:
                continue
            if job == "Director" and name not in director_names:
                director_names.append(name)
            if job in ("Screenplay", "Writer", "Story", "Characters", "Novel") and name not in writer_names:
                writer_names.append(name)
        for c in (credits.get("cast") or [])[:12]:
            name = (c.get("name") or "").strip()
            char = (c.get("character") or "").strip()
            if name:
                cast_parts.append(f"{name} ({char})" if char else name)
        director_str = ", ".join(director_names[:3])
        writer_str = ", ".join(writer_names[:5])
        cast_str = ", ".join(cast_parts) if cast_parts else film.get("cast") or ""

        film["poster_url"] = poster_url or film.get("poster_url") or ""
        film["trailer_url"] = trailer_url
        film["vote_average"] = movie.get("vote_average")
        film["genres"] = genres
        film["imdb_id"] = imdb_id
        film["overview"] = overview
        film["director"] = director_str
        film["writer"] = writer_str
        film["cast"] = cast_str

        cache[cache_key] = {
            "poster_url": film["poster_url"],
            "trailer_url": trailer_url,
            "vote_average": film["vote_average"],
            "genres": genres,
            "imdb_id": imdb_id,
            "overview": overview,
            "director": director_str,
            "writer": writer_str,
            "cast": cast_str,
            "cached_at": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.warning("TMDb enrich failed for %s: %s", search_title, e)
        cache[cache_key] = _empty_tmdb_entry()


def _merge_subtitle_variants(films: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge '(with subtitles)' and 'Autism Friendly Screening' variants into the main film card; variant showtimes go at the bottom."""
    by_base: Dict[str, List[Dict[str, Any]]] = {}
    for f in films:
        base = (f.get("search_title") or "").strip() or extract_search_title(f.get("title", ""))
        if base not in by_base:
            by_base[base] = []
        by_base[base].append(f)

    merged: List[Dict[str, Any]] = []
    for base, group in by_base.items():
        if len(group) == 1:
            merged.append(group[0])
            continue
        # Prefer the canonical title (no variant suffix)
        def is_variant(f: Dict) -> bool:
            t = (f.get("title") or "").lower()
            return "(with subtitles)" in t or "autism friendly screening" in t
        main = next((f for f in group if not is_variant(f)), group[0])
        others = [f for f in group if f is not main]
        all_showtimes = list(main.get("showtimes") or [])
        seen_keys: set = set()
        for st in all_showtimes:
            seen_keys.add((st["date"], st["time"], st["screen"]))
        for other in others:
            for st in (other.get("showtimes") or []):
                key = (st["date"], st["time"], st["screen"])
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_showtimes.append(dict(st))
        tags = lambda s: s.get("tags") or []
        all_showtimes.sort(
            key=lambda s: (
                "Subtitles" in tags(s) or "Autism Friendly" in tags(s),
                s["date"],
                s["time"],
            )
        )
        main = dict(main)
        main["showtimes"] = all_showtimes
        merged.append(main)
    return merged


def _scrape_single_cinema(cinema_slug: str, cinema_info: Dict[str, str], scrape_date: datetime) -> Dict[str, Any]:
    """Fetch one WTW cinema whats-on page and return a cinema payload."""
    cinema_url = cinema_info["url"]
    logger.info("Fetching %s", cinema_url)
    resp = fetch_with_retries(cinema_url)
    soup = BeautifulSoup(resp.text, "html.parser")

    films: List[Dict[str, Any]] = []
    # Primary parser + fallback parser mode for markup drift.
    primary_items = soup.select("li.js-film")
    fallback_items = soup.select("div[data-film]")
    parser_mode = "primary"
    film_items = primary_items
    if not film_items:
        if fallback_items:
            parser_mode = "fallback"
            film_items = fallback_items
            logger.warning("Primary selector empty for %s; using fallback selector div[data-film]", cinema_slug)
        else:
            parser_mode = "none"
            logger.warning("No film nodes matched selectors for %s (%s)", cinema_slug, cinema_url)
    tag_names = ("Audio Description", "Subtitles", "Wheelchair access", "Silver Screen", "2D", "3D", "Event cinema", "Strobe Light warning", "Parent & Baby", "Autism Friendly", "Kids Club")

    for li in film_items:
        # Be permissive: some WTW film links may omit explicit ?screen=<cinema>.
        film_a = li.find("a", href=lambda h: h and "/film/" in h)
        if not film_a:
            continue
        href = film_a.get("href", "")
        full_url = urljoin(WTW_BASE, href) if not href.startswith("http") else href
        title_elem = li.find("h2")
        title = (title_elem.get_text(strip=True) if title_elem else film_a.get_text(strip=True) or "").replace("\u2013", "-").replace("\u2014", "-")
        title = strip_format_suffix(title)
        if not title:
            continue
        if any(skip in title.lower() for skip in ("looking ahead", "gaming", "private cinema", "onscreen magazine", "book the cinema")):
            continue

        search_title = extract_search_title(title)
        film_slug = slug_from_film_url(href)
        # Poster: only from TMDb (portrait). WTW listing uses landscape WEB/card images, not movie posters.
        poster_url = ""

        synopsis = ""
        for p in li.find_all("p"):
            s = p.get_text(strip=True)
            if len(s) > 80 and "starring" not in s.lower() and "running time" not in s.lower():
                synopsis = s[:500]
                break
        cast = ""
        for elem in li.find_all(string=lambda t: t and "starring" in (t or "").lower()):
            parent = elem.parent
            if parent:
                full = parent.get_text(separator=" ", strip=True)
                if ":" in full:
                    cast = full.split(":", 1)[-1].strip()
                    if cast and len(cast) > 2:
                        break
        if not cast:
            for s in li.stripped_strings:
                if "starring" in s.lower() and ":" in s:
                    cast = s.split(":", 1)[-1].strip()
                    break
        runtime_min = None
        for s in li.stripped_strings:
            runtime_min = parse_runtime_minutes(s)
            if runtime_min is not None:
                break

        showtimes: List[Dict[str, Any]] = []
        dates_ul = li.find("ul", class_="dates")
        if dates_ul:
            for date_li in dates_ul.find_all("li", class_="js-performance-date"):
                date_text = date_li.get_text(separator=" ", strip=True)
                parsed_date = parse_uk_date(date_text, scrape_date)
                if not parsed_date:
                    continue
                for perf in date_li.find_all("li", class_="js-performance"):
                    perf_text = perf.get_text(separator=" ", strip=True)
                    time_m = re.search(r"(\d{1,2}:\d{2})", perf_text)
                    time_str = time_m.group(1) if time_m else ""
                    if not time_str:
                        continue
                    screen = 1
                    sm = re.search(r"Screen:\s*(\d+)", perf_text, re.IGNORECASE)
                    if sm:
                        screen = int(sm.group(1))
                    tags = [t for t in tag_names if t.lower() in perf_text.lower()]
                    if not tags and "2D" in perf_text:
                        tags = ["2D"]
                    book_a = perf.find("a", href=lambda h: h and "performance=" in (h or ""))
                    booking_url = urljoin(WTW_BASE, book_a.get("href", "").replace("&#038;", "&")) if book_a else ""
                    showtimes.append({
                        "date": parsed_date,
                        "time": time_str,
                        "screen": screen,
                        "cinema_name": cinema_info["name"],
                        "cinema_url": cinema_url,
                        "booking_url": booking_url,
                        "tags": tags or ["2D"],
                    })

        seen_st = set()
        unique_showtimes = []
        for st in showtimes:
            key = (st["date"], st["time"], st["screen"])
            if key not in seen_st:
                seen_st.add(key)
                unique_showtimes.append(st)

        films.append({
            "title": title,
            "search_title": search_title,
            "film_slug": film_slug,
            "synopsis": synopsis,
            "cast": cast,
            "runtime_min": runtime_min,
            "film_url": full_url,
            "poster_url": poster_url,
            "showtimes": unique_showtimes,
        })

    for f in films:
        f["showtimes"].sort(key=lambda s: (s["date"], s["time"]))

    films = _merge_subtitle_variants(films)
    showtimes_count = sum(len(f.get("showtimes") or []) for f in films)
    return {
        "name": cinema_info["name"],
        "url": cinema_url,
        "films": films,
        "_health": {
            "parser_mode": parser_mode,
            "raw_cards": len(film_items),
            "selector_film_nodes": len(film_items),
            "primary_selector_nodes": len(primary_items),
            "fallback_selector_nodes": len(fallback_items),
            "parsed_films": len(films),
            "parsed_showtimes": showtimes_count,
        },
    }


def scrape_whats_on(scrape_date: Optional[datetime] = None) -> Dict[str, Any]:
    """Fetch whats-on pages for all configured WTW cinemas."""
    scrape_date = scrape_date or datetime.now(timezone.utc)
    cinemas_scraped: Dict[str, Dict[str, Any]] = {}
    scrape_errors: Dict[str, str] = {}
    selected_cinemas = get_selected_cinemas()

    max_workers = max(1, min(len(selected_cinemas), (os.cpu_count() or 4)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_scrape_single_cinema, cinema_slug, cinema_info, scrape_date): cinema_slug
            for cinema_slug, cinema_info in selected_cinemas.items()
        }
        for future in as_completed(futures):
            cinema_slug = futures[future]
            try:
                payload = future.result()
            except Exception as e:
                logger.error("Failed scraping cinema %s: %s", cinema_slug, e)
                scrape_errors[cinema_slug] = str(e)
                continue
            cinemas_scraped[cinema_slug] = payload

    if scrape_errors:
        previous_cinemas = {}
        if Path(DATA_FILE).exists():
            try:
                old_data = json.loads(Path(DATA_FILE).read_text(encoding="utf-8"))
                previous_cinemas = old_data.get("cinemas") or {}
            except Exception:
                previous_cinemas = {}
        for slug, err in scrape_errors.items():
            if slug in cinemas_scraped:
                continue
            old_payload = previous_cinemas.get(slug)
            if old_payload:
                restored = deepcopy(old_payload)
                restored_health = restored.get("_health") or {}
                restored_health.update({
                    "restored_from_previous": True,
                    "scrape_error": err,
                })
                restored["_health"] = restored_health
                cinemas_scraped[slug] = restored
                logger.warning("Restored previous data for failed cinema %s", slug)

    failure_state = load_cinema_failure_state()
    threshold_breaches: List[str] = []
    for slug in selected_cinemas:
        if slug in scrape_errors:
            failure_state[slug] = int(failure_state.get(slug, 0)) + 1
            if failure_state[slug] >= MAX_CONSECUTIVE_CINEMA_FAILURES:
                threshold_breaches.append(f"{slug}={failure_state[slug]}")
        else:
            failure_state[slug] = 0
    save_cinema_failure_state(failure_state)
    if threshold_breaches:
        raise RuntimeError(
            "Exceeded MAX_CONSECUTIVE_CINEMA_FAILURES="
            f"{MAX_CONSECUTIVE_CINEMA_FAILURES} for cinema(s): {', '.join(threshold_breaches)}"
        )

    ordered_cinemas = {slug: cinemas_scraped[slug] for slug in selected_cinemas if slug in cinemas_scraped}
    if not ordered_cinemas:
        raise RuntimeError("No cinema data scraped successfully.")
    return {"updated_at": scrape_date.strftime("%Y-%m-%dT%H:%M:%SZ"), "cinemas": ordered_cinemas}


def validate_scrape_health(data: Dict[str, Any]) -> None:
    """Fail fast when scraped payload looks suspiciously small or selectors stop matching."""
    cinemas = data.get("cinemas") or {}
    if not cinemas:
        raise RuntimeError("Health check failed: no cinemas scraped.")

    total_films = 0
    total_showtimes = 0
    cinemas_with_films = 0
    markup_suspect_cinemas = 0
    now_showing_films = 0
    today_iso = uk_today_iso()
    issues: List[str] = []

    for slug, cinema in cinemas.items():
        if slug in HEALTH_EXCLUDED_CINEMAS:
            continue
        films = cinema.get("films") or []
        health = cinema.get("_health") or {}
        selector_nodes = int(health.get("selector_film_nodes") or 0)
        parser_mode = str(health.get("parser_mode") or "unknown")
        parsed_films = int(health.get("parsed_films") or len(films))
        parsed_showtimes = int(
            health.get("parsed_showtimes")
            or sum(len(f.get("showtimes") or []) for f in films)
        )
        total_films += parsed_films
        total_showtimes += parsed_showtimes
        if parsed_films > 0:
            cinemas_with_films += 1
        now_showing_films += sum(
            1
            for film in films
            if (film.get("showtimes") or [])
            and min((st.get("date") or "9999-99-99") for st in (film.get("showtimes") or [])) <= today_iso
        )

        if selector_nodes == 0 or parser_mode in {"fallback", "none"}:
            markup_suspect_cinemas += 1
        if selector_nodes == 0:
            issues.append(f"{slug}: selector matched 0 nodes")
        if parsed_films < MIN_FILMS_PER_CINEMA:
            issues.append(f"{slug}: parsed_films={parsed_films} below MIN_FILMS_PER_CINEMA={MIN_FILMS_PER_CINEMA}")

    logger.info(
        "Health summary: cinemas=%d films=%d showtimes=%d cinemas_with_films=%d now_showing=%d markup_suspect=%d",
        len(cinemas),
        total_films,
        total_showtimes,
        cinemas_with_films,
        now_showing_films,
        markup_suspect_cinemas,
    )

    if total_films < HEALTH_MIN_TOTAL_FILMS:
        issues.append(f"total films {total_films} below HEALTH_MIN_TOTAL_FILMS={HEALTH_MIN_TOTAL_FILMS}")
    if total_showtimes < HEALTH_MIN_TOTAL_SHOWTIMES:
        issues.append(f"total showtimes {total_showtimes} below HEALTH_MIN_TOTAL_SHOWTIMES={HEALTH_MIN_TOTAL_SHOWTIMES}")
    if cinemas_with_films < HEALTH_MIN_CINEMAS_WITH_FILMS:
        issues.append(
            f"cinemas_with_films {cinemas_with_films} below HEALTH_MIN_CINEMAS_WITH_FILMS={HEALTH_MIN_CINEMAS_WITH_FILMS}"
        )
    if now_showing_films < HEALTH_MIN_NOW_SHOWING_FILMS:
        issues.append(
            f"now_showing_films {now_showing_films} below HEALTH_MIN_NOW_SHOWING_FILMS={HEALTH_MIN_NOW_SHOWING_FILMS}"
        )
    if markup_suspect_cinemas > HEALTH_MAX_MARKUP_SUSPECT_CINEMAS:
        issues.append(
            f"markup_suspect_cinemas {markup_suspect_cinemas} above HEALTH_MAX_MARKUP_SUSPECT_CINEMAS={HEALTH_MAX_MARKUP_SUSPECT_CINEMAS}"
        )
    # Backward compatibility gates.
    if total_films < MIN_TOTAL_FILMS:
        issues.append(f"total films {total_films} below WTW_MIN_TOTAL_FILMS={MIN_TOTAL_FILMS}")
    if total_showtimes < MIN_TOTAL_SHOWTIMES:
        issues.append(f"total showtimes {total_showtimes} below WTW_MIN_TOTAL_SHOWTIMES={MIN_TOTAL_SHOWTIMES}")

    if issues:
        msg = "Health check warnings: " + "; ".join(issues)
        if HEALTHCHECK_ENFORCE:
            raise RuntimeError(msg)
        logger.warning(msg)


def compute_fingerprint(data: Dict[str, Any]) -> str:
    """Stable hash of data (film titles + showtime counts + dates) for change detection."""
    parts: List[str] = []
    cinemas = data.get("cinemas") or {}
    for cinema_slug in sorted(cinemas.keys()):
        cinema = cinemas.get(cinema_slug) or {}
        films = sorted(
            cinema.get("films") or [],
            key=lambda f: (
                (f.get("film_slug") or ""),
                (f.get("search_title") or ""),
                (f.get("title") or ""),
            ),
        )
        for film in films:
            parts.append(f"T|{cinema_slug}|{film.get('title', '')}")
            showtimes = sorted(
                film.get("showtimes") or [],
                key=lambda st: (
                    st.get("date", ""),
                    st.get("time", ""),
                    str(st.get("screen", "")),
                    st.get("booking_url", ""),
                    st.get("cinema_name", ""),
                ),
            )
            for st in showtimes:
                parts.append(
                    "S|{slug}|{cinema}|{date}|{time}|{screen}|{booking}".format(
                        slug=cinema_slug,
                        cinema=st.get("cinema_name", ""),
                        date=st.get("date", ""),
                        time=st.get("time", ""),
                        screen=st.get("screen", ""),
                        booking=st.get("booking_url", ""),
                    )
                )
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def build_html(data: Dict[str, Any]) -> str:
    """Generate single self-contained index.html with Web3 style and date filtering."""
    def short_cinema_name(name: str) -> str:
        value = (name or "").strip()
        if not value:
            return ""
        if "," in value:
            return value.split(",")[-1].strip()
        return value

    aggregated: Dict[str, Dict[str, Any]] = {}
    for cinema in (data.get("cinemas") or {}).values():
        cinema_name = cinema.get("name", "")
        for film in cinema.get("films") or []:
            search_title = (film.get("search_title") or extract_search_title(film.get("title", ""))).strip()
            film_slug = (film.get("film_slug") or "").strip()
            key = f"slug:{film_slug}" if film_slug else f"title:{re.sub(r'[^a-z0-9]+', '-', search_title.lower()).strip('-')}"
            if key not in aggregated:
                base = dict(film)
                base["showtimes"] = [dict(st) for st in (film.get("showtimes") or [])]
                base["cinema_names"] = set([cinema_name]) if cinema_name else set()
                base["genres"] = list(base.get("genres") or [])
                aggregated[key] = base
                continue
            current = aggregated[key]
            if cinema_name:
                current["cinema_names"].add(cinema_name)
            current["showtimes"].extend(dict(st) for st in (film.get("showtimes") or []))
            for field in ("poster_url", "trailer_url", "overview", "synopsis", "cast", "director", "writer", "imdb_id", "film_url"):
                if not (current.get(field) or "").strip() and (film.get(field) or "").strip():
                    current[field] = film[field]
            if current.get("runtime_min") is None and film.get("runtime_min") is not None:
                current["runtime_min"] = film.get("runtime_min")
            if current.get("vote_average") is None and film.get("vote_average") is not None:
                current["vote_average"] = film.get("vote_average")
            current["genres"] = sorted(set((current.get("genres") or []) + (film.get("genres") or [])))

    films: List[Dict[str, Any]] = []
    for film in aggregated.values():
        seen_showtimes = set()
        deduped_showtimes = []
        for st in sorted(film.get("showtimes") or [], key=lambda s: (s.get("date", ""), s.get("time", ""), str(s.get("screen", "")), s.get("booking_url", ""))):
            k = (st.get("date"), st.get("time"), st.get("screen"), st.get("booking_url"), st.get("cinema_name"))
            if k in seen_showtimes:
                continue
            seen_showtimes.add(k)
            deduped_showtimes.append(st)
        film["showtimes"] = deduped_showtimes
        cinema_names_full = sorted(film.pop("cinema_names", set()))
        cinema_names_short = sorted({short_cinema_name(x) for x in cinema_names_full if short_cinema_name(x)})
        film["cinema_names_list"] = cinema_names_short
        film["cinema_name"] = ", ".join(cinema_names_short)
        films.append(film)
    build_today_iso = uk_today_iso()

    # Collect unique dates for tabs
    all_dates = set()
    for f in films:
        for st in f.get("showtimes") or []:
            all_dates.add(st.get("date", ""))
    sorted_dates = sorted(all_dates) if all_dates else []

    def cert_span(rating: Optional[str]) -> str:
        """WTW-style cert icon: <span class="cert cert--15"></span>. Uses scraped cert images for U, PG, 12A, 15, 18; fallback for 12/R18."""
        if not rating:
            return ""
        r = rating.upper()
        if r in CERT_IMAGES:
            return f'<span class="cert cert--{r}" aria-label="{r}"></span>'
        # Fallback for 12, R18 or unknown (WTW has no cert icons for these)
        fallback = {"12": "12", "R18": "R18"}
        return f'<span class="cert cert-fallback" aria-label="{r}">{fallback.get(r, r)}</span>'

    def film_card(f: Dict) -> str:
        title = f.get("title", "")
        search_title = f.get("search_title") or extract_search_title(title)
        bbfc = extract_bbfc_rating(title)
        runtime = f.get("runtime_min")
        if runtime:
            runtime_str = f"{runtime} min"
            if runtime >= 60:
                runtime_str += f" ({format_runtime(runtime)})"
        else:
            runtime_str = ""
        cast_raw = (f.get("cast") or "").strip()
        cast_parts = [p.strip() for p in cast_raw.split(",") if p.strip()]
        cast_first = cast_parts[:6]
        cast_rest = cast_parts[6:]
        director = (f.get("director") or "").strip()
        writer = (f.get("writer") or "").strip()
        overview = (f.get("overview") or "").strip()
        synopsis = (f.get("synopsis") or "").strip()
        description = overview or synopsis
        description = description[:500] if description else ""
        film_url = f.get("film_url", "")
        poster_url = f.get("poster_url", "")
        trailer_url = f.get("trailer_url", "")
        vote = f.get("vote_average")
        if vote is not None:
            pct = min(100, max(0, (vote / 10.0) * 100))
            vote_str = f'<span class="rating-wrap" title="TMDb rating"><span class="rating-bar" aria-hidden="true"><span class="rating-fill" style="width:{pct:.0f}%"></span></span><span class="rating-text">{vote:.1f}/10</span></span>'
        else:
            vote_str = ""
        genres = f.get("genres") or []
        imdb_id = f.get("imdb_id", "")
        imdb_link = f"https://www.imdb.com/title/{imdb_id}/" if imdb_id else f"https://www.imdb.com/find/?q={quote_plus(search_title)}"
        rt_link = f"https://www.rottentomatoes.com/search?search={quote_plus(search_title)}"
        trakt_link = f"https://trakt.tv/search?query={quote_plus(search_title)}"

        tag_icon_ids = {
            "Audio Description": "icon-audio-desc",
            "Wheelchair access": "icon-wheelchair",
            "2D": "icon-2d",
            "3D": "icon-3d",
            "Subtitles": "icon-subtitles",
            "Silver Screen": "icon-silver-screen",
            "Event cinema": "icon-event-cinema",
            "Strobe Light warning": "icon-strobe",
            "Parent & Baby": "icon-parent-baby",
            "Autism Friendly": "icon-autism-friendly",
            "Kids Club": "icon-kids-club",
        }
        tag_short_labels = {"Audio Description": "AD", "Subtitles": "Subs", "Wheelchair access": "WA", "Strobe Light warning": "Strobe"}
        tag_tooltips = {
            "Audio Description": "Audio description",
            "Subtitles": "Subtitled screening",
            "Wheelchair access": "Wheelchair accessible",
            "2D": "Standard 2D screening",
            "Strobe Light warning": "Strobe lighting may affect photosensitive viewers",
        }

        def tag_html(tag: str) -> str:
            icon_id = tag_icon_ids.get(tag)
            label = tag_short_labels.get(tag, tag)
            tooltip = tag_tooltips.get(tag) or (tag if tag in tag_short_labels else None)
            title_esc = (tooltip or "").replace("&", "&amp;").replace('"', "&quot;")
            title_attr = f' title="{title_esc}"' if title_esc else ""
            if icon_id:
                return f'<span class="tag"{title_attr}><svg class="tag-icon" aria-hidden="true"><use href="#{icon_id}"/></svg>{label}</span>'
            return f'<span class="tag"{title_attr}>{label}</span>'

        def render_showings(showings: List[Dict[str, Any]]) -> str:
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for st in showings:
                d = st.get("date", "")
                grouped.setdefault(d, []).append(st)
            rows: List[str] = []
            for d in sorted(grouped.keys()):
                time_parts = []
                for st in grouped[d]:
                    t = st.get("time", "")
                    cinema_short = short_cinema_name(str(st.get("cinema_name") or ""))
                    screen_num = st.get("screen", "")
                    screen_label = f"{cinema_short} (Screen {screen_num})" if cinema_short and screen_num else (cinema_short or f"Screen {screen_num}")
                    booking = st.get("booking_url", "")
                    tags = st.get("tags") or []
                    tag_span = " ".join(tag_html(tag) for tag in tags[:4])
                    time_el = f'<a href="{booking}">{t}</a>' if booking else f'<span class="past">{t}</span>'
                    time_parts.append(
                        f'<div class="st-row">'
                        f'<span class="st-time">{time_el}</span>'
                        f'<span class="st-screen">{screen_label}</span>'
                        f'<span class="st-tags">{tag_span}</span>'
                        f'</div>'
                    )
                date_label = d
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d")
                    date_label = dt.strftime("%a %d %b")
                except ValueError:
                    pass
                rows.append(f'<div class="day-group"><div class="st-date">{date_label}</div>' + "".join(time_parts) + "</div>")
            return "\n".join(rows)

        showtimes_all = sorted(f.get("showtimes") or [], key=lambda s: (s.get("date", ""), s.get("time", ""), str(s.get("screen", "")), s.get("booking_url", "")))
        showtimes_by_date: Dict[str, List[Dict]] = {}
        for st in showtimes_all:
            d = st.get("date", "")
            showtimes_by_date.setdefault(d, []).append(st)
        visible_showings = showtimes_all[:INITIAL_SHOWINGS_VISIBLE]
        hidden_showings = showtimes_all[INITIAL_SHOWINGS_VISIBLE:]
        showtimes_html = render_showings(visible_showings)
        show_more_block = ""
        full_showtimes_json = showtimes_compact_json(showtimes_all)
        if hidden_showings:
            hidden_json = showtimes_compact_json(hidden_showings)
            count = len(hidden_showings)
            noun = "showing" if count == 1 else "showings"
            show_more_block = (
                f'<script type="application/json" class="showtimes-more-data">{hidden_json}</script>'
                f'<div class="showtimes-more" hidden></div>'
                f'<button type="button" class="showtimes-more-btn" aria-expanded="false">Show {count} more {noun}</button>'
            )

        has_3d = any("3D" in (st.get("tags") or []) for st in (f.get("showtimes") or []))
        poster_src = poster_url or POSTER_PLACEHOLDER_REL
        poster_alt = f"Poster for {title}" if poster_url else f"No poster available for {title}"
        poster_inner = f'<img src="{poster_src}" alt="{poster_alt}" loading="lazy"/>'
        if poster_url and has_3d:
            poster_inner += '<i class="icon--hints icon--3d" aria-hidden="true"></i>'
        if not poster_url:
            poster_inner += '<span class="poster-fallback-label">No poster yet</span>'
        poster_div = f'<div class="poster">{poster_inner}</div>'
        # YouTube embed URL for lightbox; use nocookie domain and add fallback watch URL for Error 153 (embed disabled)
        trailer_embed = ""
        trailer_watch_esc = ""
        if trailer_url:
            v_match = re.search(r"(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})", trailer_url)
            if v_match:
                vid = v_match.group(1)
                trailer_embed = f"https://www.youtube-nocookie.com/embed/{vid}?autoplay=1&rel=0"
                trailer_watch_esc = trailer_url.replace("&", "&amp;").replace('"', "&quot;")
        trailer_embed_esc = (trailer_embed or "").replace("&", "&amp;").replace('"', "&quot;")
        trailer_a = f'<button type="button" class="trailer trailer-lightbox-trigger" data-embed="{trailer_embed_esc}" data-watch="{trailer_watch_esc}" aria-label="Play trailer">Trailer</button>' if trailer_embed else ""
        genre_span = f'<span class="genres">{", ".join(genres[:4])}</span>' if genres else ""
        # Escape for HTML (e.g. "Smith & Jones" -> "Smith &amp; Jones")
        def esc(s: str) -> str:
            return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        cast_first_esc = ", ".join(esc(a) for a in cast_first)
        cast_rest_esc = ", ".join(esc(a) for a in cast_rest)
        director_esc = esc(director)
        writer_esc = esc(writer)
        description_esc = esc(description)
        meta_lines = []
        cinema_name = (f.get("cinema_name") or "").strip()
        if cinema_name:
            meta_lines.append(f'<p class="crew"><strong>Cinemas:</strong> {esc(cinema_name)}</p>')
        if director_esc:
            meta_lines.append(f'<p class="crew"><strong>Director:</strong> {director_esc}</p>')
        if writer_esc:
            meta_lines.append(f'<p class="crew"><strong>Writer(s):</strong> {writer_esc}</p>')
        if cast_first_esc or cast_rest_esc:
            cast_rest_html = f'<span class="cast-rest" hidden>, {cast_rest_esc}</span>' if cast_rest_esc else ""
            more_btn = f' <button type="button" class="cast-more-btn">More</button>' if cast_rest_esc else ""
            meta_lines.append(f'<p class="cast"><strong>Starring:</strong> {cast_first_esc}{cast_rest_html}{more_btn}</p>')
        crew_html = "\n      ".join(meta_lines)

        earliest = min(showtimes_by_date.keys()) if showtimes_by_date else ""
        status = "now" if earliest and earliest <= build_today_iso else "coming-soon"
        status_label = "Now Showing" if status == "now" else "Coming Soon"
        return f"""
<article class="film-card" data-dates="{",".join(showtimes_by_date.keys())}" data-status="{status}">
  <script type="application/json" class="film-showtimes-full">{full_showtimes_json}</script>
  <span class="status-pill status-pill--{status}">{status_label}</span>
  <div class="film-header">
    {poster_div}
    <div class="film-meta">
      <h2>{title} {cert_span(bbfc)}</h2>
      <div class="meta-line">{runtime_str} {vote_str} {genre_span}</div>
      {trailer_a}
      {crew_html}
      <p class="synopsis">{description_esc}</p>
      <div class="links">
        <a href="{film_url}" class="btn book">Book at WTW</a>
        <a href="{imdb_link}" class="link ext-link" target="_blank" rel="noopener" title="IMDb"><svg class="ext-logo" aria-hidden="true"><use href="#imdb-logo"/></svg> IMDb</a>
        <a href="{rt_link}" class="link ext-link" target="_blank" rel="noopener" title="Rotten Tomatoes"><svg class="ext-logo" aria-hidden="true"><use href="#rt-logo"/></svg> RT</a>
        <a href="{trakt_link}" class="link ext-link" target="_blank" rel="noopener" title="Trakt"><svg class="ext-logo" aria-hidden="true"><use href="#trakt-logo"/></svg> Trakt</a>
      </div>
    </div>
  </div>
  <div class="showtimes">{showtimes_html}{show_more_block}</div>
</article>"""

    films_sorted = sorted(films, key=lambda f: len(f.get("showtimes") or []), reverse=True)
    now_showing = [f for f in films_sorted if f.get("showtimes") and min(st.get("date", "9999") for st in f["showtimes"]) <= build_today_iso]
    coming_soon = [f for f in films_sorted if f not in now_showing]
    now_cards = "\n".join(film_card(f) for f in now_showing)
    coming_cards = "\n".join(film_card(f) for f in coming_soon)
    section_now = (
        f'<section class="film-section film-section--now" data-section="now">\n'
        f'  <div class="section-title-wrap">\n'
        f'    <h3 class="section-title" data-section="now">Now Showing</h3>\n'
        f'    <span class="section-count">{len(now_showing)} films</span>\n'
        f'  </div>\n'
        f'{now_cards}\n'
        f'</section>'
    ) if now_showing else ""
    section_coming = (
        f'<section class="film-section film-section--coming" data-section="coming">\n'
        f'  <div class="section-title-wrap">\n'
        f'    <h3 class="section-title" data-section="coming">Coming Soon</h3>\n'
        f'    <span class="section-count">{len(coming_soon)} films</span>\n'
        f'  </div>\n'
        f'{coming_cards}\n'
        f'</section>'
    ) if coming_soon else ""
    cards_html = "\n".join(s for s in (section_now, section_coming) if s)

    # Date filter tabs
    tabs = ['<button type="button" class="tab active" data-date="all">All</button>']
    for d in sorted_dates[:14]:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            label = dt.strftime("%a %d")
            if d == build_today_iso:
                label = "Today"
            tabs.append(f'<button type="button" class="tab" data-date="{d}">{label}</button>')
        except ValueError:
            tabs.append(f'<button type="button" class="tab" data-date="{d}">{d}</button>')
    tabs_html = "\n".join(tabs)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>What's on at WTW Cinemas — ratings, trailers &amp; links</title>
  <link rel="preconnect" href="https://fonts.googleapis.com"/>
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
  <style>
    :root {{
      --bg: #0a0a0f;
      --card-bg: #12121a;
      --surface: #12121a;
      --surface-2: #12121a;
      --surface-3: #1a1a24;
      --border: rgba(168,85,247,0.25);
      --text: #e2e8f0;
      --text-muted: #94a3b8;
      --cyan: #00d4ff;
      --purple: #a855f7;
      --accent: #00d4ff;
      --accent-dim: rgba(0,212,255,0.15);
      --accent-glow: rgba(0,212,255,0.25);
      --radius: 16px;
      --radius-sm: 10px;
      --radius-lg: 24px;
      --transition: 0.25s cubic-bezier(0.4, 0, 0.2, 1);
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      font-family: 'Space Grotesk', system-ui, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.6;
      min-height: 100vh;
      overflow-x: hidden;
      -webkit-font-smoothing: antialiased;
    }}
    .bg-mesh {{
      position: fixed;
      inset: 0;
      background:
        radial-gradient(ellipse 100% 80% at 50% -30%, var(--accent-dim) 0%, transparent 50%),
        radial-gradient(ellipse 60% 50% at 80% 100%, rgba(0,212,255,0.08) 0%, transparent 40%),
        radial-gradient(ellipse 40% 40% at 10% 90%, rgba(168,85,247,0.05) 0%, transparent 50%);
      pointer-events: none;
      z-index: 0;
    }}
    .bg-grid {{
      position: fixed;
      inset: 0;
      background-image: linear-gradient(rgba(255,255,255,0.02) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,255,255,0.02) 1px, transparent 1px);
      background-size: 60px 60px;
      pointer-events: none;
      z-index: 0;
    }}
    .page {{ position: relative; z-index: 1; max-width: 1400px; margin: 0 auto; padding: 2rem 1.25rem 4rem; }}
    @media (min-width: 640px) {{ .page {{ padding: 3rem 2rem 5rem; }} }}
    header {{
      text-align: center;
      padding: 3rem 0 2rem;
      border-bottom: 1px solid var(--border);
      animation: fadeUp 0.8s ease-out;
    }}
    @keyframes fadeUp {{
      from {{ opacity: 0; transform: translateY(20px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    header h1 {{
      font-size: clamp(2rem, 5vw, 2.5rem);
      font-weight: 800;
      letter-spacing: -0.04em;
      background: linear-gradient(90deg, var(--cyan), var(--purple));
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }}
    header p {{ color: var(--text-muted); font-size: 0.95rem; margin-top: 0.25rem; }}
    .tabs {{ display: flex; flex-wrap: wrap; gap: 0.5rem; justify-content: center; padding: 1rem 0; }}
    .tab {{
      font-family: inherit;
      background: var(--surface-2);
      border: 1px solid var(--border);
      color: var(--text);
      padding: 0.5rem 0.75rem;
      border-radius: var(--radius-sm);
      cursor: pointer;
      font-size: 0.9rem;
      transition: all var(--transition);
    }}
    .tab:hover {{ border-color: var(--cyan); }}
    .tab.active {{
      background: linear-gradient(135deg, var(--accent-dim), rgba(168,85,247,0.15));
      border-color: var(--cyan);
    }}
    #films {{ display: grid; grid-template-columns: 1fr; gap: 1.5rem; }}
    .film-section {{
      grid-column: 1 / -1;
      display: grid;
      grid-template-columns: 1fr;
      gap: 1rem;
      border: 1px solid var(--border);
      border-radius: var(--radius-lg);
      padding: 1rem;
      background: linear-gradient(160deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01));
    }}
    @media (min-width: 900px) {{
      .film-section {{ grid-template-columns: repeat(2, 1fr); }}
    }}
    .film-section--now {{
      border-color: rgba(0,212,255,0.45);
      box-shadow: inset 0 0 0 1px rgba(0,212,255,0.08);
    }}
    .film-section--coming {{
      border-color: rgba(168,85,247,0.45);
      box-shadow: inset 0 0 0 1px rgba(168,85,247,0.1);
    }}
    .section-title-wrap {{
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.75rem;
      padding: 0.9rem 1rem;
      border-radius: var(--radius);
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.02);
    }}
    .film-section--now .section-title-wrap {{
      border-color: rgba(0,212,255,0.5);
      background: linear-gradient(90deg, rgba(0,212,255,0.2), rgba(0,212,255,0.06));
    }}
    .film-section--coming .section-title-wrap {{
      border-color: rgba(168,85,247,0.5);
      background: linear-gradient(90deg, rgba(168,85,247,0.24), rgba(168,85,247,0.08));
    }}
    .section-title {{
      font-size: 1.15rem;
      font-weight: 700;
      letter-spacing: 0.06em;
      margin: 0;
      text-transform: uppercase;
    }}
    .film-section--now .section-title {{ color: var(--cyan); }}
    .film-section--coming .section-title {{ color: #cf90ff; }}
    .section-count {{ font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: var(--text); opacity: 0.95; }}
    .film-card {{
      background: linear-gradient(135deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0.01) 100%);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 1.25rem;
      transition: all var(--transition);
      position: relative;
      overflow: hidden;
      animation: fadeUp 0.6s ease-out backwards;
    }}
    .status-pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 1.7rem;
      padding: 0.2rem 0.55rem;
      border-radius: 999px;
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.07em;
      text-transform: uppercase;
      border: 1px solid transparent;
      margin-bottom: 0.7rem;
    }}
    .status-pill--now {{ background: rgba(0,212,255,0.16); border-color: rgba(0,212,255,0.45); color: #8beeff; }}
    .status-pill--coming-soon {{ background: rgba(168,85,247,0.2); border-color: rgba(168,85,247,0.5); color: #e0b8ff; }}
    .film-card::before {{
      content: '';
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      height: 2px;
      background: linear-gradient(90deg, transparent, var(--cyan), transparent);
      opacity: 0;
      transition: opacity var(--transition);
    }}
    .film-card:hover {{
      border-color: rgba(0,212,255,0.4);
      transform: translateY(-4px);
      box-shadow: 0 20px 40px rgba(0,0,0,0.4), 0 0 0 1px rgba(0,212,255,0.1);
    }}
    .film-card:hover::before {{ opacity: 1; }}
    .film-header {{ display: flex; gap: 1.25rem; flex-wrap: wrap; }}
    .poster {{ position: relative; flex-shrink: 0; }}
    .poster img {{ width: 210px; height: 315px; object-fit: cover; border-radius: var(--radius-sm); box-shadow: 0 4px 12px rgba(0,0,0,0.3); }}
    .poster-fallback-label {{
      position: absolute;
      left: 0.5rem;
      right: 0.5rem;
      bottom: 0.55rem;
      padding: 0.2rem 0.4rem;
      border-radius: 6px;
      background: rgba(0, 0, 0, 0.62);
      color: #e2e8f0;
      font-size: 0.72rem;
      text-align: center;
      letter-spacing: 0.02em;
    }}
    .poster .icon--hints {{ position: absolute; right: 0; top: 0; width: 105px; height: 105px; pointer-events: none; }}
    .poster .icon--hints.icon--3d {{ background: url(icons/3D-Performance.png) no-repeat; background-size: 100% auto; background-position: top right; }}
    .film-meta {{ flex: 1; min-width: 200px; }}
    .film-meta h2 {{ font-size: 1.25rem; margin: 0 0 0.5rem; display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; }}
    .cert {{ display: inline-block; width: 29px; height: 29px; background-position: center; background-repeat: no-repeat; background-size: 100% auto; margin-right: 6px; vertical-align: middle; }}
    .cert--U {{ background-image: url(certs/cert-u.png); }}
    .cert--PG {{ background-image: url(certs/cert-pg.png); }}
    .cert--12A {{ background-image: url(certs/cert-12a.png); }}
    .cert--15 {{ background-image: url(certs/cert-15.png); }}
    .cert--18 {{ background-image: url(certs/cert-18.png); }}
    .cert-fallback {{ background: var(--surface-3); color: #fff; font-size: 0.65rem; font-weight: 700; display: inline-flex; align-items: center; justify-content: center; border-radius: 4px; }}
    .meta-line {{ color: var(--text-muted); font-size: 0.9rem; margin-bottom: 0.5rem; display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem 1rem; }}
    .rating-wrap {{ display: inline-flex; align-items: center; gap: 0.4rem; }}
    .rating-bar {{ display: block; width: 3rem; height: 0.5rem; background: rgba(255,255,255,0.25); border-radius: 3px; overflow: hidden; }}
    .rating-fill {{ display: block; height: 0.5rem; background: linear-gradient(90deg, #00d4ff, #a855f7); border-radius: 3px; transition: width 0.2s; }}
    .rating-text {{ font-variant-numeric: tabular-nums; font-size: 0.85em; color: var(--cyan); }}
    .genres {{ color: var(--purple); }}
    .trailer {{ display: inline-block; margin-bottom: 0.5rem; color: var(--cyan); font-size: 0.9rem; background: none; border: none; cursor: pointer; font-family: inherit; padding: 0; text-decoration: underline; }}
    .trailer:hover {{ color: var(--purple); }}
    .trailer-lightbox {{ position: fixed; inset: 0; z-index: 1000; display: none; align-items: center; justify-content: center; padding: 1rem; box-sizing: border-box; }}
    .trailer-lightbox.is-open {{ display: flex; }}
    .trailer-lightbox-backdrop {{ position: absolute; inset: 0; background: rgba(0,0,0,0.85); cursor: pointer; }}
    .trailer-lightbox-inner {{ position: relative; width: 100%; max-width: 90vw; max-height: 90vh; aspect-ratio: 16/9; background: #000; border-radius: var(--radius); box-shadow: 0 0 40px var(--accent-glow); overflow: hidden; }}
    .trailer-lightbox-inner iframe {{ position: absolute; top: 0; left: 0; width: 100%; height: 100%; border: none; }}
    .trailer-lightbox-close {{ position: absolute; top: -2.5rem; right: 0; background: var(--surface-2); border: 1px solid var(--border); color: var(--text); width: 2rem; height: 2rem; border-radius: var(--radius-sm); cursor: pointer; font-size: 1.25rem; line-height: 1; display: flex; align-items: center; justify-content: center; z-index: 1; transition: all var(--transition); }}
    .trailer-lightbox-close:hover {{ border-color: var(--cyan); color: var(--cyan); }}
    .trailer-lightbox-fallback {{ position: absolute; bottom: 0.5rem; left: 0.5rem; font-size: 0.85rem; color: var(--cyan); }}
    .trailer-lightbox-fallback:hover {{ color: var(--purple); }}
    .film-meta .crew {{ font-size: 0.9rem; color: var(--text-muted); margin: 0; padding: 0.5rem 0; border-bottom: 1px solid var(--border); }}
    .film-meta .crew:first-of-type {{ padding-top: 0; }}
    .film-meta .cast {{ font-size: 0.9rem; color: var(--text-muted); margin: 0; padding: 0.5rem 0; border-bottom: 1px solid var(--border); }}
    .film-meta .synopsis {{ font-size: 0.9rem; color: var(--text-muted); margin: 0; padding: 0.75rem 0 0.5rem; line-height: 1.5; max-width: 56em; border-top: 1px solid var(--border); }}
    .links {{ margin-top: 0.75rem; display: flex; flex-wrap: wrap; gap: 0.5rem; align-items: center; }}
    .links a {{
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      padding: 0.5rem 0.75rem;
      border-radius: var(--radius-sm);
      font-size: 0.9rem;
      text-decoration: none;
      transition: all var(--transition);
    }}
    .links .btn {{
      background: linear-gradient(135deg, var(--cyan), var(--purple));
      color: var(--bg);
      font-weight: 600;
      border: none;
    }}
    .links .btn:hover {{
      background: linear-gradient(135deg, #20dfff, #b366ff);
      transform: scale(1.02);
      box-shadow: 0 4px 20px var(--accent-glow);
    }}
    .links .link {{ color: var(--accent); background: rgba(255,255,255,0.06); border: 1px solid var(--border); }}
    .links .link:hover {{ background: rgba(0,212,255,0.12); border-color: var(--cyan); color: var(--purple); }}
    .ext-logo {{ width: 18px; height: 18px; flex-shrink: 0; }}
    .showtimes {{ margin-top: 1rem; padding-top: 1rem; border-top: 1px solid var(--border); font-size: 0.9rem; }}
    .day-group {{ margin-bottom: 0.75rem; }}
    .day-group:last-child {{ margin-bottom: 0; }}
    .st-date {{ font-weight: 600; margin-bottom: 0.25rem; color: var(--text); }}
    .st-row {{ display: grid; grid-template-columns: 4.5rem minmax(9rem, 1fr) 2fr; gap: 0 0.75rem; align-items: center; margin-bottom: 0.2rem; }}
    .st-row:last-child {{ margin-bottom: 0; }}
    .st-time {{ font-variant-numeric: tabular-nums; }}
    .st-time a, .showtime a {{ color: var(--cyan); }}
    .st-time .past {{ color: var(--text-muted); }}
    .st-screen {{ color: var(--text-muted); }}
    .st-tags {{ display: flex; align-items: center; flex-wrap: wrap; gap: 0.25rem; }}
    .showtimes-more {{ margin-top: 0.5rem; }}
    .showtimes-more-btn {{
      margin-top: 0.65rem;
      padding: 0.38rem 0.7rem;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.04);
      color: var(--accent);
      font-family: inherit;
      font-size: 0.82rem;
      cursor: pointer;
      transition: all var(--transition);
    }}
    .showtimes-more-btn:hover {{ border-color: var(--cyan); background: rgba(0,212,255,0.1); }}
    .cast-more-btn {{ background: none; border: none; color: var(--cyan); cursor: pointer; font-size: 0.85em; padding: 0 0.25rem; font-family: inherit; }}
    .cast-more-btn:hover {{ text-decoration: underline; }}
    .tag {{ font-size: 0.75rem; color: var(--text-muted); margin-left: 0.25rem; display: inline-flex; align-items: center; gap: 0.25rem; }}
    .tag-icon {{ width: 14px; height: 14px; flex-shrink: 0; vertical-align: middle; }}
    .cal-link {{ color: var(--purple); text-decoration: none; margin-left: 0.25rem; }}
    footer {{
      margin-top: 4rem;
      padding-top: 2.5rem;
      border-top: 1px solid var(--border);
      text-align: center;
      color: var(--text-muted);
      font-size: 0.85rem;
      animation: fadeUp 0.6s ease-out backwards;
    }}
    footer a {{ color: var(--accent); text-decoration: none; font-weight: 500; transition: color var(--transition); }}
    footer a:hover {{ color: var(--purple); }}
    .footer-disclaimer {{ font-size: 0.9rem; max-width: 36rem; margin: 0 auto 1rem; line-height: 1.6; }}
    .footer-links {{ display: flex; flex-wrap: wrap; justify-content: center; gap: 0.5rem 1.5rem; margin-bottom: 1rem; }}
    .footer-attribution {{ font-size: 0.8rem; opacity: 0.85; margin: 0; line-height: 1.5; }}
  </style>
</head>
<body>
  <div class="bg-mesh"></div>
  <div class="bg-grid"></div>
  <div id="trailer-lightbox" class="trailer-lightbox" aria-hidden="true" role="dialog" aria-modal="true" aria-label="Trailer video">
    <div class="trailer-lightbox-backdrop" id="trailer-lightbox-backdrop"></div>
    <div class="trailer-lightbox-inner">
      <button type="button" class="trailer-lightbox-close" id="trailer-lightbox-close" aria-label="Close">×</button>
      <iframe id="trailer-lightbox-iframe" title="Trailer" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
      <a id="trailer-lightbox-fallback" class="trailer-lightbox-fallback" href="#" target="_blank" rel="noopener">Watch on YouTube</a>
    </div>
  </div>
  <svg xmlns="http://www.w3.org/2000/svg" style="position:absolute;width:0;height:0;">
    <defs>
      <symbol id="imdb-logo" viewBox="0 0 64 32"><rect width="64" height="32" rx="2" fill="#F5C518"/><text x="32" y="21" text-anchor="middle" font-family="Arial,sans-serif" font-size="14" font-weight="bold" fill="#000">imdb</text></symbol>
      <symbol id="rt-logo" viewBox="0 0 32 32"><circle cx="16" cy="17" r="11" fill="#E50914"/><path d="M16 6 L18 4 L20 6 L18 8 L16 6" fill="#00B140"/><ellipse cx="16" cy="7" rx="4" ry="2.5" fill="#00B140"/></symbol>
      <symbol id="trakt-logo" viewBox="0 0 32 32"><rect x="6" y="8" width="20" height="16" rx="3" fill="none" stroke="#ED1C24" stroke-width="2"/><path d="M14 12 L14 20 M18 12 L18 20 M14 16 L18 16" stroke="#ED1C24" stroke-width="1.5" fill="none"/></symbol>
      <symbol id="icon-audio-desc" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 18v-6a9 9 0 0 1 18 0v6"/><path d="M21 19a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3zM3 19a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2v-3a2 2 0 0 0-2-2H3z"/></symbol>
      <symbol id="icon-wheelchair" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="5" r="1"/><path d="M19 13v3h-2"/><path d="M6 21a3 3 0 0 1 0-6 2 2 0 0 1 2 2v4"/><path d="M6 21a5 5 0 0 0 5-5v-3h2"/><circle cx="18" cy="16" r="4"/><path d="M14 10h4v4"/></symbol>
      <symbol id="icon-2d" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="14" rx="1"/><path d="M7 12h4M7 16h6"/></symbol>
      <symbol id="icon-3d" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 8v8M4 8l8-4 8 4-8 4zM4 8l8 4M20 8l-8 4M12 12v8"/></symbol>
      <symbol id="icon-subtitles" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="6" width="20" height="12" rx="1"/><path d="M6 12h.01M10 12h.01M14 12h.01M18 12h.01M6 16h12"/></symbol>
      <symbol id="icon-silver-screen" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 15 9 22 9 17 14 18 22 12 18 6 22 7 14 2 9 9 9"/></symbol>
      <symbol id="icon-event-cinema" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/></symbol>
      <symbol id="icon-strobe" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></symbol>
      <symbol id="icon-parent-baby" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="7" r="4"/><path d="M3 21v-2a4 4 0 0 1 4-4h4"/><circle cx="17" cy="11" r="2.5"/><path d="M17 13.5v4M15 18h4"/></symbol>
      <symbol id="icon-autism-friendly" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 8v8M8 12h8"/></symbol>
      <symbol id="icon-kids-club" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="8" r="3"/><path d="M5 21v-2a4 4 0 0 1 4-4h0"/><circle cx="15" cy="10" r="2"/><path d="M15 21v-2a2 2 0 0 0-2-2h0"/></symbol>
    </defs>
  </svg>
  <div class="page">
    <header>
      <h1>What's on at WTW Cinemas</h1>
      <p>Ratings, trailers &amp; links to IMDb, RT and Trakt</p>
    </header>
    <div class="tabs">{tabs_html}</div>
    <main id="films" data-initial-showings="{INITIAL_SHOWINGS_VISIBLE}">{cards_html}</main>
    <footer>
      <p class="footer-disclaimer">An open source fan-made project. Not affiliated with WTW Cinemas.</p>
      <div class="footer-links">
        <a href="https://wtwcinemas.co.uk/">WTW Cinemas</a>
        <span aria-hidden="true">·</span>
        <a href="https://github.com/evenwebb/wtw-whats-on">Source</a>
        <span aria-hidden="true">·</span>
        <a href="https://github.com/evenwebb/">evenwebb</a>
      </div>
      <p class="footer-attribution">
        Posters and ratings via <a href="https://www.themoviedb.org/" target="_blank" rel="noopener">TMDb</a>. This product uses the TMDB API but is not endorsed or certified by TMDB.
      </p>
    </footer>
  </div>
  <script>
    document.querySelectorAll('.cast-more-btn').forEach(function(btn) {{
      btn.addEventListener('click', function() {{
        var rest = btn.previousElementSibling;
        if (rest && rest.classList.contains('cast-rest')) {{
          var on = rest.hasAttribute('hidden');
          if (on) {{ rest.removeAttribute('hidden'); btn.textContent = 'Less'; }}
          else {{ rest.setAttribute('hidden', ''); btn.textContent = 'More'; }}
        }}
      }});
    }});
    (function() {{
      var filmsMain = document.getElementById('films');
      var initialShowings = parseInt((filmsMain && filmsMain.getAttribute('data-initial-showings')) || '10', 10);
      var se = 'script';

      function showtimeFromRow(row) {{
        if (!row) return {{ date: '', time: '', screen: '', cinema_name: '', booking_url: '', tags: [] }};
        if (!Array.isArray(row)) return row;
        return {{
          date: row[0] || '',
          time: row[1] || '',
          screen: row[2],
          cinema_name: row[3] || '',
          booking_url: row[4] || '',
          tags: Array.isArray(row[5]) ? row[5] : []
        }};
      }}
      function showtimesFromParsed(raw) {{
        if (!raw) return [];
        if (raw.v === 1 && Array.isArray(raw.r)) return raw.r.map(showtimeFromRow);
        if (Array.isArray(raw)) {{
          if (raw.length && Array.isArray(raw[0])) return raw.map(showtimeFromRow);
          return raw.slice();
        }}
        return [];
      }}
      function showtimeToCompactRow(st) {{
        return [st.date || '', st.time || '', st.screen, st.cinema_name || '', st.booking_url || '', st.tags || []];
      }}

      function escHtml(value) {{
        return String(value || '')
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;');
      }}
      function formatDateLabel(isoDate) {{
        if (!isoDate) return '';
        var dt = new Date(isoDate + 'T12:00:00Z');
        if (Number.isNaN(dt.getTime())) return isoDate;
        return dt.toLocaleDateString('en-GB', {{ weekday: 'short', day: '2-digit', month: 'short', timeZone: 'Europe/London' }});
      }}
      function tagHtml(tag) {{
        var iconMap = {{
          'Audio Description': 'icon-audio-desc',
          'Wheelchair access': 'icon-wheelchair',
          '2D': 'icon-2d',
          '3D': 'icon-3d',
          'Subtitles': 'icon-subtitles',
          'Silver Screen': 'icon-silver-screen',
          'Event cinema': 'icon-event-cinema',
          'Strobe Light warning': 'icon-strobe',
          'Parent & Baby': 'icon-parent-baby',
          'Autism Friendly': 'icon-autism-friendly',
          'Kids Club': 'icon-kids-club'
        }};
        var shortLabelMap = {{ 'Audio Description': 'AD', 'Subtitles': 'Subs', 'Wheelchair access': 'WA', 'Strobe Light warning': 'Strobe' }};
        var tooltipMap = {{
          'Audio Description': 'Audio description',
          'Subtitles': 'Subtitled screening',
          'Wheelchair access': 'Wheelchair accessible',
          '2D': 'Standard 2D screening',
          'Strobe Light warning': 'Strobe lighting may affect photosensitive viewers'
        }};
        var iconId = iconMap[tag];
        var label = shortLabelMap[tag] || tag;
        var tooltip = tooltipMap[tag] || (shortLabelMap[tag] ? tag : '');
        var titleAttr = tooltip ? ' title="' + escHtml(tooltip) + '"' : '';
        if (iconId) {{
          return '<span class="tag"' + titleAttr + '><svg class="tag-icon" aria-hidden="true"><use href="#' + iconId + '"/></svg>' + escHtml(label) + '</span>';
        }}
        return '<span class="tag"' + titleAttr + '>' + escHtml(label) + '</span>';
      }}
      function renderShowingsHtml(list) {{
        var grouped = {{}};
        list.forEach(function(st) {{
          var d = st.date || '';
          if (!grouped[d]) grouped[d] = [];
          grouped[d].push(st);
        }});
        return Object.keys(grouped).sort().map(function(d) {{
          var rows = grouped[d].map(function(st) {{
            var time = escHtml(st.time || '');
            var cinema = String(st.cinema_name || '').trim();
            cinema = cinema.indexOf(',') !== -1 ? cinema.split(',').pop().trim() : cinema;
            var screen = String(st.screen || '');
            var screenLabel = cinema && screen ? (cinema + ' (Screen ' + screen + ')') : (cinema || ('Screen ' + screen));
            var booking = String(st.booking_url || '');
            var timeEl = booking ? '<a href="' + escHtml(booking) + '">' + time + '</a>' : '<span class="past">' + time + '</span>';
            var tags = Array.isArray(st.tags) ? st.tags.slice(0, 4) : [];
            var tagSpan = tags.map(tagHtml).join(' ');
            return '<div class="st-row"><span class="st-time">' + timeEl + '</span><span class="st-screen">' + escHtml(screenLabel) + '</span><span class="st-tags">' + tagSpan + '</span></div>';
          }}).join('');
          return '<div class="day-group"><div class="st-date">' + escHtml(formatDateLabel(d)) + '</div>' + rows + '</div>';
        }}).join('');
      }}

      function sortShowtimes(list) {{
        list.sort(function(a, b) {{
          var ad = a.date || '';
          var bd = b.date || '';
          if (ad !== bd) return ad < bd ? -1 : ad > bd ? 1 : 0;
          var at = a.time || '';
          var bt = b.time || '';
          if (at !== bt) return at < bt ? -1 : at > bt ? 1 : 0;
          var as = String(a.screen || '');
          var bs = String(b.screen || '');
          if (as !== bs) return as < bs ? -1 : as > bs ? 1 : 0;
          var ab = a.booking_url || '';
          var bb = b.booking_url || '';
          return ab < bb ? -1 : ab > bb ? 1 : 0;
        }});
      }}

      function buildShowtimesInner(list) {{
        var copy = list.slice();
        sortShowtimes(copy);
        var visible = copy.slice(0, initialShowings);
        var hidden = copy.slice(initialShowings);
        var html = renderShowingsHtml(visible);
        if (hidden.length) {{
          var hiddenJson = JSON.stringify({{ v: 1, r: hidden.map(showtimeToCompactRow) }}).replace(/<\\//g, '<\\/');
          var count = hidden.length;
          var noun = count === 1 ? 'showing' : 'showings';
          html += '<' + se + ' type="application/json" class="showtimes-more-data">' + hiddenJson + '</' + se + '>';
          html += '<div class="showtimes-more" hidden></div>';
          html += '<button type="button" class="showtimes-more-btn" aria-expanded="false">Show ' + count + ' more ' + noun + '</button>';
        }}
        return html;
      }}

      function applyShowtimesForSelectedDate(selectedDate) {{
        var isAll = selectedDate === 'all';
        document.querySelectorAll('.film-card').forEach(function(card) {{
          var dataScript = card.querySelector('script.film-showtimes-full');
          var showtimesEl = card.querySelector('.showtimes');
          if (!dataScript || !showtimesEl) return;
          var all = [];
          try {{
            all = showtimesFromParsed(JSON.parse(dataScript.textContent || 'null'));
          }} catch (e1) {{
            return;
          }}
          var picked = isAll ? all.slice() : all.filter(function(st) {{ return st.date === selectedDate; }});
          showtimesEl.innerHTML = buildShowtimesInner(picked);
        }});
      }}

      document.querySelectorAll('.tab').forEach(function(btn) {{
        btn.addEventListener('click', function() {{
          document.querySelectorAll('.tab').forEach(function(b) {{ b.classList.remove('active'); }});
          btn.classList.add('active');
          var date = btn.getAttribute('data-date');
          var isAll = date === 'all';
          var sectionVisibility = {{ now: false, coming: false }};
          document.querySelectorAll('.film-card').forEach(function(card) {{
            var dates = (card.getAttribute('data-dates') || '').split(',');
            var show = isAll || dates.indexOf(date) !== -1;
            card.style.display = show ? 'block' : 'none';
            if (show) {{
              var status = card.getAttribute('data-status') || '';
              if (status === 'now') sectionVisibility.now = true;
              if (status === 'coming-soon') sectionVisibility.coming = true;
            }}
          }});
          document.querySelectorAll('.film-section').forEach(function(section) {{
            var sectionType = section.getAttribute('data-section') || '';
            var showSection = sectionType === 'now' ? sectionVisibility.now : sectionVisibility.coming;
            section.style.display = showSection ? 'grid' : 'none';
          }});
          applyShowtimesForSelectedDate(date);
        }});
      }});

      if (filmsMain) {{
        filmsMain.addEventListener('click', function(e) {{
          var btn = e.target.closest('.showtimes-more-btn');
          if (!btn || !filmsMain.contains(btn)) return;
          var card = btn.closest('.film-card');
          if (!card) return;
          var holder = card.querySelector('.showtimes-more');
          var dataNode = card.querySelector('.showtimes-more-data');
          if (!holder || !dataNode) return;

          var expanded = btn.getAttribute('aria-expanded') === 'true';
          if (expanded) {{
            holder.setAttribute('hidden', '');
            btn.setAttribute('aria-expanded', 'false');
            btn.textContent = btn.getAttribute('data-show-label') || 'Show more';
            return;
          }}

          if (!holder.hasChildNodes()) {{
            try {{
              var list = showtimesFromParsed(JSON.parse(dataNode.textContent || 'null'));
              holder.innerHTML = renderShowingsHtml(list);
            }} catch (e2) {{
              holder.innerHTML = '';
            }}
          }}
          holder.removeAttribute('hidden');
          btn.setAttribute('aria-expanded', 'true');
          if (!btn.getAttribute('data-show-label')) btn.setAttribute('data-show-label', btn.textContent);
          btn.textContent = 'Show less';
        }});
      }}
    }})();
    (function() {{
      var lb = document.getElementById('trailer-lightbox');
      var iframe = document.getElementById('trailer-lightbox-iframe');
      var backdrop = document.getElementById('trailer-lightbox-backdrop');
      var closeBtn = document.getElementById('trailer-lightbox-close');
      var fallbackLink = document.getElementById('trailer-lightbox-fallback');
      function closeLightbox() {{
        lb.classList.remove('is-open');
        lb.setAttribute('aria-hidden', 'true');
        iframe.src = '';
        if (fallbackLink) fallbackLink.href = '#';
      }}
      function openLightbox(embedUrl, watchUrl) {{
        iframe.src = embedUrl;
        if (fallbackLink && watchUrl) fallbackLink.href = watchUrl;
        lb.classList.add('is-open');
        lb.setAttribute('aria-hidden', 'false');
      }}
      document.querySelectorAll('.trailer-lightbox-trigger').forEach(function(btn) {{
        btn.addEventListener('click', function() {{
          var embedUrl = this.getAttribute('data-embed');
          var watchUrl = this.getAttribute('data-watch') || '';
          if (embedUrl) openLightbox(embedUrl, watchUrl);
        }});
      }});
      if (backdrop) backdrop.addEventListener('click', closeLightbox);
      if (closeBtn) closeBtn.addEventListener('click', closeLightbox);
      document.addEventListener('keydown', function(e) {{
        if (e.key === 'Escape' && lb.classList.contains('is-open')) closeLightbox();
      }});
    }})();
  </script>
</body>
</html>
"""
    return html


def main() -> None:
    scrape_date = datetime.now(timezone.utc)
    data = scrape_whats_on(scrape_date)
    validate_scrape_health(data)
    fingerprint = compute_fingerprint(data)
    prev_fingerprint = ""
    if Path(FINGERPRINT_FILE).exists():
        prev_fingerprint = Path(FINGERPRINT_FILE).read_text(encoding="utf-8").strip()
    unchanged = (
        fingerprint == prev_fingerprint
        and Path(DATA_FILE).exists()
        and Path(SITE_DIR, "index.html").exists()
    )
    force_rebuild = os.environ.get("FORCE_REBUILD", "").strip().lower() in {"1", "true", "yes"}
    if unchanged and not force_rebuild:
        logger.info("Fingerprint unchanged; skipping TMDb enrichment and HTML rebuild.")
        return

    all_films = [film for cinema in (data.get("cinemas") or {}).values() for film in (cinema.get("films") or [])]
    for film in all_films:
        film["film_slug"] = film.get("film_slug") or slug_from_film_url(film.get("film_url", ""))
    films_by_tmdb_key: Dict[str, List[Dict[str, Any]]] = {}
    for film in all_films:
        films_by_tmdb_key.setdefault(_tmdb_cache_key(film), []).append(film)

    api_key = os.environ.get("TMDB_API_KEY")
    tmdb_cache = load_tmdb_cache()
    if api_key:
        for group in films_by_tmdb_key.values():
            primary = group[0]
            enrich_film_tmdb(primary, api_key, tmdb_cache)
            for sibling in group[1:]:
                for key in ENRICHMENT_FIELDS:
                    value = primary.get(key)
                    if value not in (None, "", []):
                        sibling[key] = deepcopy(value) if isinstance(value, (dict, list)) else value
        save_tmdb_cache(tmdb_cache)
    else:
        logger.info("TMDB_API_KEY not set; skipping TMDb enrichment")
        # Preserve poster and other TMDb fields from last run so posters don't disappear
        if Path(DATA_FILE).exists():
            try:
                with open(DATA_FILE, encoding="utf-8") as f:
                    old_data = json.load(f)
                old_all_films = [f for c in (old_data.get("cinemas") or {}).values() for f in (c.get("films") or [])]
                old_films = {f.get("film_url"): f for f in old_all_films if f.get("film_url")}
                old_films_by_key = {
                    _tmdb_cache_key(f): f
                    for f in old_all_films
                }
                for film in all_films:
                    old = old_films_by_key.get(_tmdb_cache_key(film)) or old_films.get(film.get("film_url"))
                    if old:
                        for key in ENRICHMENT_FIELDS:
                            if film.get(key) in (None, "", []) and old.get(key):
                                film[key] = old[key]
            except Exception as e:
                logger.warning("Could not merge previous TMDb data: %s", e)

    # Ensure search_title and default enrichment keys
    for film in all_films:
        film.setdefault("search_title", extract_search_title(film.get("title", "")))
        film.setdefault("poster_url", film.get("poster_url") or "")
        film.setdefault("trailer_url", "")
        film.setdefault("vote_average", None)
        film.setdefault("genres", [])
        film.setdefault("imdb_id", "")
        film.setdefault("overview", "")
        film.setdefault("director", "")
        film.setdefault("writer", "")

    # Download TMDb posters locally in parallel (deduped by slug).
    poster_jobs: Dict[str, str] = {}
    for film in all_films:
        poster_url = (film.get("poster_url") or "").strip()
        if not poster_url.startswith("http"):
            continue
        slug = film.get("film_slug") or _tmdb_cache_key(film)
        poster_jobs.setdefault(slug, poster_url)
    poster_results: Dict[str, str] = {}
    if poster_jobs:
        max_workers = max(1, min(8, len(poster_jobs)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_download_poster, url, slug): slug
                for slug, url in poster_jobs.items()
            }
            for future in as_completed(futures):
                slug = futures[future]
                try:
                    local = future.result() or ""
                except Exception as e:
                    logger.warning("Poster download task failed for %s: %s", slug, e)
                    local = ""
                poster_results[slug] = local
        for film in all_films:
            slug = film.get("film_slug") or _tmdb_cache_key(film)
            local = poster_results.get(slug) or ""
            if local:
                film["poster_url"] = local
    _ensure_placeholder_poster()

    missing_by_key: Dict[str, str] = {}
    for film in all_films:
        if (film.get("poster_url") or "").strip():
            continue
        missing_by_key.setdefault(_tmdb_cache_key(film), film.get("title", ""))
    missing_titles = sorted(t for t in missing_by_key.values() if t)
    if missing_titles:
        logger.warning("Missing posters for %d unique film(s): %s", len(missing_titles), ", ".join(missing_titles))
    fail_threshold_raw = os.environ.get("POSTER_MISSING_FAIL_THRESHOLD", "").strip()
    if fail_threshold_raw:
        try:
            fail_threshold = int(fail_threshold_raw)
            if fail_threshold >= 0 and len(missing_titles) > fail_threshold:
                raise RuntimeError(
                    f"Poster quality gate failed: {len(missing_titles)} missing unique film poster(s) exceeds threshold {fail_threshold}"
                )
        except ValueError:
            logger.warning("Invalid POSTER_MISSING_FAIL_THRESHOLD value: %s", fail_threshold_raw)

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %s", DATA_FILE)

    _download_cert_images()
    _download_3d_icon()
    html = build_html(data)
    Path(SITE_DIR).mkdir(parents=True, exist_ok=True)
    Path(SITE_DIR, "index.html").write_text(html, encoding="utf-8")
    logger.info("Wrote %s/index.html", SITE_DIR)
    Path(FINGERPRINT_FILE).write_text(fingerprint, encoding="utf-8")
    if fingerprint == prev_fingerprint:
        logger.info("Fingerprint unchanged; nothing new to commit.")
    else:
        logger.info("Fingerprint updated; commit and push to publish.")


if __name__ == "__main__":
    main()
