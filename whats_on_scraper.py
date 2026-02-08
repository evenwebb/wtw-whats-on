#!/usr/bin/env python3
"""
WTW Cinemas What's On scraper.

Scrapes the St Austell whats-on page, optionally enriches with TMDb data,
writes whats_on_data.json and regenerates index.html on every run.
Commits (e.g. in CI) are driven by fingerprint change.
"""
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
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

WTW_WHATS_ON_URL = "https://wtwcinemas.co.uk/st-austell/whats-on/"
WTW_BASE = "https://wtwcinemas.co.uk"

DATA_FILE = "whats_on_data.json"
FINGERPRINT_FILE = ".whats_on_fingerprint"
TMDB_CACHE_FILE = ".tmdb_cache.json"
POSTERS_DIR = "posters"
CERTS_DIR = "certs"
WTW_CERT_BASE = "https://wtwcinemas.co.uk/wp-content/themes/wtw-2017/dist/images"
CERT_IMAGES = {"U": "cert-u.png", "PG": "cert-pg.png", "12A": "cert-12a.png", "15": "cert-15.png", "18": "cert-18.png"}
ICONS_DIR = "icons"
WTW_3D_ICON_URL = "https://wtwcinemas.co.uk/wp-content/uploads/2022/11/3D-Performance.png"
TMDB_CACHE_DAYS = 30
TMDB_DELAY_SEC = 0.2

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# BBFC rating pattern in titles: (15), (12A), (PG), (18), (U), (R18), (with subtitles) etc.
RATING_PATTERN = re.compile(r"\((\d+A?|U|PG|R18)\)", re.IGNORECASE)
SUBTITLE_SUFFIX = re.compile(r"\s*\(with subtitles\)\s*$", re.IGNORECASE)
# Format suffix: " - HFR 3D" (high frame rate 3D) is not part of the movie name
FORMAT_SUFFIX = re.compile(r"\s*-\s*HFR\s*3D\s*$", re.IGNORECASE)


def fetch_with_retries(url: str, retries: int = HTTP_RETRIES, timeout: int = HTTP_TIMEOUT) -> requests.Response:
    """Fetch URL with exponential backoff on failure."""
    headers = {"User-Agent": USER_AGENT}
    delay = HTTP_RETRY_DELAY
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
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
    """Strip age rating and 'with subtitles' for search/links. E.g. 'Send Help (15)' -> 'Send Help'."""
    t = strip_format_suffix(title)
    t = SUBTITLE_SUFFIX.sub("", t).strip()
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


def slug_from_film_url(url: str) -> str:
    """Extract slug from film URL for cache key. E.g. /film/send-help/?screen=st-austell -> send-help."""
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
            r = requests.get(url, headers=headers, timeout=10)
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
        r = requests.get(WTW_3D_ICON_URL, headers={"User-Agent": USER_AGENT, "Referer": WTW_BASE + "/"}, timeout=10)
        r.raise_for_status()
        path.write_bytes(r.content)
        logger.info("Downloaded 3D icon")
    except Exception as e:
        logger.warning("3D icon download failed: %s", e)


def _download_poster(url: str, slug: str) -> str:
    """Download poster image and save under POSTERS_DIR; return relative path or '' on failure."""
    if not url or not url.startswith("http"):
        return ""
    slug = re.sub(r"[^a-z0-9-]", "", slug.lower()) or "poster"
    ext = "jpg"
    if ".webp" in url.lower():
        ext = "webp"
    elif ".png" in url.lower():
        ext = "png"
    path = Path(POSTERS_DIR) / f"{slug}.{ext}"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        headers = {"User-Agent": USER_AGENT}
        if "wtwcinemas.co.uk" in url:
            headers["Referer"] = WTW_BASE + "/"
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        path.write_bytes(r.content)
        return f"{POSTERS_DIR}/{slug}.{ext}"
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
        # Refetch if we have poster but no genres (backfill for old cache entries)
        if (not (entry.get("genres") or [])) and entry.get("poster_url"):
            pass  # Fall through to API call to get genres (and refresh cache)
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
        search_r = requests.get(
            search_url,
            params={"api_key": api_key, "query": search_title, "language": "en-GB"},
            timeout=10,
        )
        search_r.raise_for_status()
        data = search_r.json()
        results = data.get("results") or []
        if not results:
            cache[cache_key] = _empty_tmdb_entry()
            return
        chosen = _pick_best_tmdb_result(results, search_title)
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
        detail_r = requests.get(
            detail_url,
            params={"api_key": api_key, "append_to_response": "videos,credits", "language": "en-GB"},
            timeout=10,
        )
        detail_r.raise_for_status()
        movie = detail_r.json()

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
    """Merge '(with subtitles)' variants into the main film card; subtitled showtimes go at the bottom."""
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
        main = next(
            (f for f in group if "(with subtitles)" not in (f.get("title") or "").lower()),
            group[0],
        )
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
        all_showtimes.sort(
            key=lambda s: (
                "Subtitles" in (s.get("tags") or []),
                s["date"],
                s["time"],
            )
        )
        main = dict(main)
        main["showtimes"] = all_showtimes
        merged.append(main)
    return merged


def scrape_whats_on(scrape_date: Optional[datetime] = None) -> Dict[str, Any]:
    """Fetch whats-on page and return structured data for st-austell."""
    scrape_date = scrape_date or datetime.utcnow()
    logger.info("Fetching %s", WTW_WHATS_ON_URL)
    resp = fetch_with_retries(WTW_WHATS_ON_URL)
    soup = BeautifulSoup(resp.text, "html.parser")

    films: List[Dict[str, Any]] = []
    # Page structure: ul.listing--items > li.js-film per film; each has ul.dates > li.js-performance-date > li.js-performance
    film_items = soup.select("li.js-film")
    tag_names = ("Audio Description", "Subtitles", "Wheelchair access", "Silver Screen", "2D", "3D", "Event cinema", "Strobe Light warning", "Parent & Baby", "Autism Friendly", "Kids Club")

    for li in film_items:
        film_a = li.find("a", href=lambda h: h and "/film/" in h and "st-austell" in h)
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
        text = li.get_text(separator=" ", strip=True)

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

    return {
        "updated_at": scrape_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cinemas": {
            "st-austell": {
                "name": "White River Cinema, St Austell",
                "url": WTW_WHATS_ON_URL,
                "films": films,
            }
        },
    }


def compute_fingerprint(data: Dict[str, Any]) -> str:
    """Stable hash of data (film titles + showtime counts + dates) for change detection."""
    canonical = []
    for cinema in (data.get("cinemas") or {}).values():
        for film in cinema.get("films") or []:
            canonical.append(film.get("title", ""))
            for st in film.get("showtimes") or []:
                canonical.append(f"{st.get('date')}_{st.get('time')}_{st.get('screen')}")
    return hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()


def build_html(data: Dict[str, Any]) -> str:
    """Generate single self-contained index.html with Web3 style and date filtering."""
    cinema = (data.get("cinemas") or {}).get("st-austell") or {}
    films = cinema.get("films") or []

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

        showtimes_by_date: Dict[str, List[Dict]] = {}
        for st in f.get("showtimes") or []:
            d = st.get("date", "")
            if d not in showtimes_by_date:
                showtimes_by_date[d] = []
            showtimes_by_date[d].append(st)

        rows = []
        for d in sorted(showtimes_by_date.keys()):
            times = showtimes_by_date[d]
            time_parts = []
            for st in times:
                t = st.get("time", "")
                screen = st.get("screen", 0)
                booking = st.get("booking_url", "")
                tags = st.get("tags") or []
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
                tag_span = " ".join(tag_html(tag) for tag in tags[:4])
                time_el = f'<a href="{booking}">{t}</a>' if booking else f'<span class="past">{t}</span>'
                time_parts.append(
                    f'<div class="st-row">'
                    f'<span class="st-time">{time_el}</span>'
                    f'<span class="st-screen">Screen {screen}</span>'
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
        showtimes_html = "\n".join(rows)

        has_3d = any("3D" in (st.get("tags") or []) for st in (f.get("showtimes") or []))
        if poster_url:
            poster_inner = f'<img src="{poster_url}" alt="" loading="lazy"/>'
            if has_3d:
                poster_inner += '<i class="icon--hints icon--3d" aria-hidden="true"></i>'
            poster_div = f'<div class="poster">{poster_inner}</div>'
        else:
            poster_div = ""
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
        if director_esc:
            meta_lines.append(f'<p class="crew"><strong>Director:</strong> {director_esc}</p>')
        if writer_esc:
            meta_lines.append(f'<p class="crew"><strong>Writer(s):</strong> {writer_esc}</p>')
        if cast_first_esc or cast_rest_esc:
            cast_rest_html = f'<span class="cast-rest" hidden>, {cast_rest_esc}</span>' if cast_rest_esc else ""
            more_btn = f' <button type="button" class="cast-more-btn">More</button>' if cast_rest_esc else ""
            meta_lines.append(f'<p class="cast"><strong>Starring:</strong> {cast_first_esc}{cast_rest_html}{more_btn}</p>')
        crew_html = "\n      ".join(meta_lines)

        return f"""
<article class="film-card" data-dates="{",".join(showtimes_by_date.keys())}">
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
  <div class="showtimes">{showtimes_html}</div>
</article>"""

    cards_html = "\n".join(film_card(f) for f in films)

    # Date filter tabs
    today_iso = datetime.utcnow().date().isoformat()
    tabs = ['<button type="button" class="tab active" data-date="all">All</button>']
    for d in sorted_dates[:14]:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            label = dt.strftime("%a %d")
            if d == today_iso:
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
  <title>What's on at WTW St Austell — ratings, trailers &amp; links</title>
  <link rel="preconnect" href="https://fonts.googleapis.com"/>
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
  <style>
    :root {{
      --bg: #0a0a0f;
      --card-bg: #12121a;
      --cyan: #00d4ff;
      --purple: #a855f7;
      --text: #e2e8f0;
      --muted: #94a3b8;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: 'Space Grotesk', sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }}
    .wrap {{ max-width: 1400px; margin: 0 auto; padding: 1rem; }}
    header {{ text-align: center; padding: 2rem 0; border-bottom: 1px solid rgba(0,212,255,0.2); }}
    header h1 {{ font-size: 1.75rem; font-weight: 700; background: linear-gradient(90deg, var(--cyan), var(--purple)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }}
    header p {{ color: var(--muted); font-size: 0.95rem; margin-top: 0.25rem; }}
    .tabs {{ display: flex; flex-wrap: wrap; gap: 0.5rem; justify-content: center; padding: 1rem 0; }}
    .tab {{ font-family: inherit; background: var(--card-bg); border: 1px solid rgba(168,85,247,0.3); color: var(--text); padding: 0.5rem 0.75rem; border-radius: 8px; cursor: pointer; font-size: 0.9rem; }}
    .tab:hover {{ border-color: var(--cyan); }}
    .tab.active {{ background: linear-gradient(135deg, rgba(0,212,255,0.15), rgba(168,85,247,0.15)); border-color: var(--cyan); }}
    #films {{ display: grid; grid-template-columns: 1fr; gap: 1.5rem; }}
    @media (min-width: 900px) {{ #films {{ grid-template-columns: repeat(2, 1fr); }} }}
    .film-card {{ background: var(--card-bg); border: 1px solid rgba(168,85,247,0.25); border-radius: 12px; padding: 1.25rem; transition: box-shadow 0.2s, border-color 0.2s; }}
    .film-card:hover {{ border-color: rgba(0,212,255,0.4); box-shadow: 0 0 24px rgba(0,212,255,0.08); }}
    .film-header {{ display: flex; gap: 1.25rem; flex-wrap: wrap; }}
    .poster {{ position: relative; flex-shrink: 0; }}
    .poster img {{ width: 210px; height: 315px; object-fit: cover; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }}
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
    .cert-fallback {{ background: #333; color: #fff; font-size: 0.65rem; font-weight: 700; display: inline-flex; align-items: center; justify-content: center; border-radius: 4px; }}
    .meta-line {{ color: var(--muted); font-size: 0.9rem; margin-bottom: 0.5rem; display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem 1rem; }}
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
    .trailer-lightbox-inner {{ position: relative; width: 100%; max-width: 90vw; max-height: 90vh; aspect-ratio: 16/9; background: #000; border-radius: 8px; box-shadow: 0 0 40px rgba(0,212,255,0.2); overflow: hidden; }}
    .trailer-lightbox-inner iframe {{ position: absolute; top: 0; left: 0; width: 100%; height: 100%; border: none; }}
    .trailer-lightbox-close {{ position: absolute; top: -2.5rem; right: 0; background: var(--card-bg); border: 1px solid rgba(168,85,247,0.3); color: var(--text); width: 2rem; height: 2rem; border-radius: 6px; cursor: pointer; font-size: 1.25rem; line-height: 1; display: flex; align-items: center; justify-content: center; z-index: 1; }}
    .trailer-lightbox-close:hover {{ border-color: var(--cyan); color: var(--cyan); }}
    .trailer-lightbox-fallback {{ position: absolute; bottom: 0.5rem; left: 0.5rem; font-size: 0.85rem; color: var(--cyan); }}
    .trailer-lightbox-fallback:hover {{ color: var(--purple); }}
    .film-meta .crew {{ font-size: 0.9rem; color: var(--muted); margin: 0; padding: 0.5rem 0; border-bottom: 1px solid rgba(255,255,255,0.1); }}
    .film-meta .crew:first-of-type {{ padding-top: 0; }}
    .film-meta .cast {{ font-size: 0.9rem; color: var(--muted); margin: 0; padding: 0.5rem 0; border-bottom: 1px solid rgba(255,255,255,0.1); }}
    .film-meta .synopsis {{ font-size: 0.9rem; color: var(--muted); margin: 0; padding: 0.75rem 0 0.5rem; line-height: 1.5; max-width: 56em; border-top: 1px solid rgba(255,255,255,0.1); }}
    .links {{ margin-top: 0.75rem; display: flex; flex-wrap: wrap; gap: 0.5rem; align-items: center; }}
    .links a {{ display: inline-flex; align-items: center; gap: 0.35rem; padding: 0.5rem 0.75rem; border-radius: 8px; font-size: 0.9rem; text-decoration: none; }}
    .links .btn {{ background: linear-gradient(135deg, var(--cyan), var(--purple)); color: var(--bg); font-weight: 600; border: none; }}
    .links .link {{ color: var(--cyan); background: rgba(255,255,255,0.06); border: 1px solid rgba(0,212,255,0.35); }}
    .links .link:hover {{ background: rgba(0,212,255,0.12); border-color: var(--cyan); }}
    .ext-logo {{ width: 18px; height: 18px; flex-shrink: 0; }}
    .showtimes {{ margin-top: 1rem; padding-top: 1rem; border-top: 1px solid rgba(255,255,255,0.08); font-size: 0.9rem; }}
    .day-group {{ margin-bottom: 0.75rem; }}
    .day-group:last-child {{ margin-bottom: 0; }}
    .st-date {{ font-weight: 600; margin-bottom: 0.25rem; color: var(--text); }}
    .st-row {{ display: grid; grid-template-columns: 4.5rem 6rem 1fr; gap: 0 1rem; align-items: center; margin-bottom: 0.2rem; }}
    .st-row:last-child {{ margin-bottom: 0; }}
    .st-time {{ font-variant-numeric: tabular-nums; }}
    .st-time a, .showtime a {{ color: var(--cyan); }}
    .st-time .past {{ color: var(--muted); }}
    .st-screen {{ color: var(--muted); }}
    .st-tags {{ display: flex; align-items: center; flex-wrap: wrap; gap: 0.25rem; }}
    .cast-more-btn {{ background: none; border: none; color: var(--cyan); cursor: pointer; font-size: 0.85em; padding: 0 0.25rem; font-family: inherit; }}
    .cast-more-btn:hover {{ text-decoration: underline; }}
    .tag {{ font-size: 0.75rem; color: var(--muted); margin-left: 0.25rem; display: inline-flex; align-items: center; gap: 0.25rem; }}
    .tag-icon {{ width: 14px; height: 14px; flex-shrink: 0; vertical-align: middle; }}
    .cal-link {{ color: var(--purple); text-decoration: none; margin-left: 0.25rem; }}
    footer {{ text-align: center; padding: 2rem; color: var(--muted); font-size: 0.85rem; border-top: 1px solid rgba(255,255,255,0.06); }}
    footer a {{ color: var(--cyan); }}
  </style>
</head>
<body>
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
  <div class="wrap">
    <header>
      <h1>What's on at WTW St Austell</h1>
      <p>Ratings, trailers &amp; links to IMDb, RT and Trakt</p>
    </header>
    <div class="tabs">{tabs_html}</div>
    <main id="films">{cards_html}</main>
    <footer>
      <a href="{WTW_WHATS_ON_URL}">WTW Cinemas</a>
    </footer>
  </div>
  <script>
    document.querySelectorAll('.tab').forEach(function(btn) {{
      btn.addEventListener('click', function() {{
        document.querySelectorAll('.tab').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        var date = btn.getAttribute('data-date');
        document.querySelectorAll('.film-card').forEach(function(card) {{
          var dates = (card.getAttribute('data-dates') || '').split(',');
          var show = date === 'all' || dates.indexOf(date) !== -1;
          card.style.display = show ? 'block' : 'none';
        }});
      }});
    }});
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
    scrape_date = datetime.utcnow()
    data = scrape_whats_on(scrape_date)

    api_key = os.environ.get("TMDB_API_KEY")
    tmdb_cache = load_tmdb_cache()
    if api_key:
        for film in data["cinemas"]["st-austell"]["films"]:
            film["film_slug"] = film.get("film_slug") or slug_from_film_url(film.get("film_url", ""))
            enrich_film_tmdb(film, api_key, tmdb_cache)
        save_tmdb_cache(tmdb_cache)
    else:
        logger.info("TMDB_API_KEY not set; skipping TMDb enrichment")
        # Preserve poster and other TMDb fields from last run so posters don't disappear
        if Path(DATA_FILE).exists():
            try:
                with open(DATA_FILE, encoding="utf-8") as f:
                    old_data = json.load(f)
                old_films = {f.get("film_url"): f for f in (old_data.get("cinemas", {}).get("st-austell", {}).get("films") or []) if f.get("film_url")}
                for film in data["cinemas"]["st-austell"]["films"]:
                    old = old_films.get(film.get("film_url"))
                    if old:
                        for key in ("poster_url", "trailer_url", "vote_average", "genres", "imdb_id", "overview", "director", "writer", "cast"):
                            if film.get(key) in (None, "", []) and old.get(key):
                                film[key] = old[key]
            except Exception as e:
                logger.warning("Could not merge previous TMDb data: %s", e)

    # Ensure search_title and default enrichment keys
    for film in data["cinemas"]["st-austell"]["films"]:
        film.setdefault("search_title", extract_search_title(film.get("title", "")))
        film.setdefault("poster_url", film.get("poster_url") or "")
        film.setdefault("trailer_url", "")
        film.setdefault("vote_average", None)
        film.setdefault("genres", [])
        film.setdefault("imdb_id", "")
        film.setdefault("overview", "")
        film.setdefault("director", "")
        film.setdefault("writer", "")

    # Download TMDb posters locally (they are proper portrait posters; WTW listing images are landscape cards)
    for film in data["cinemas"]["st-austell"]["films"]:
        poster_url = film.get("poster_url") or ""
        if poster_url.startswith("http"):
            slug = film.get("film_slug") or _tmdb_cache_key(film)
            local = _download_poster(poster_url, slug)
            if local:
                film["poster_url"] = local

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %s", DATA_FILE)

    fingerprint = compute_fingerprint(data)
    prev_fingerprint = ""
    if Path(FINGERPRINT_FILE).exists():
        prev_fingerprint = Path(FINGERPRINT_FILE).read_text(encoding="utf-8").strip()

    _download_cert_images()
    _download_3d_icon()
    html = build_html(data)
    Path("index.html").write_text(html, encoding="utf-8")
    logger.info("Wrote index.html")
    Path(FINGERPRINT_FILE).write_text(fingerprint, encoding="utf-8")
    if fingerprint == prev_fingerprint:
        logger.info("Fingerprint unchanged; nothing new to commit.")
    else:
        logger.info("Fingerprint updated; commit and push to publish.")


if __name__ == "__main__":
    main()
