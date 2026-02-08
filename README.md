# WTW What's On

What's on at **White River Cinema, St Austell** (WTW Cinemas) — ratings, trailers, posters and links to IMDb, RT and Trakt.

## Features

- **Listings** — Films and showtimes scraped from the [St Austell whats-on page](https://wtwcinemas.co.uk/st-austell/whats-on/), with date tabs to filter.
- **TMDb enrichment** (optional) — Posters, trailers, ratings (with visual bar), genres, cast, director, writer and synopsis when `TMDB_API_KEY` is set.
- **Age ratings** — BBFC-style cert icons (U, PG, 12A, 15, 18) and 3D badge on posters where applicable.
- **Trailer lightbox** — Play trailers in a dimmed overlay without leaving the page.
- **Accessibility tags** — AD, Subs, WA, 2D, Strobe etc. with tooltips; aligned showtime grid (date, time, screen, tags).
- **Links** — Book at WTW, IMDb, RT (Rotten Tomatoes), Trakt in a single chip-style row.

## Run locally

```bash
pip install -r requirements.txt
python whats_on_scraper.py
```

- Writes `whats_on_data.json` and regenerates `site/index.html` (and assets in `site/`) every run.
- **Optional:** set `TMDB_API_KEY` (from [themoviedb.org](https://www.themoviedb.org/settings/api)) for posters, trailers, ratings and genres. Without it, existing TMDb data in the repo is preserved.

## Production (GitHub Pages)

1. **Settings → Pages** — Source: **Deploy from a branch** → Branch: **main** → **/site** → Save.  
   Site: [https://evenwebb.github.io/wtw-whats-on](https://evenwebb.github.io/wtw-whats-on).

2. **TMDb (optional)** — **Settings → Secrets and variables → Actions** → New repository secret: **TMDB_API_KEY**. The workflow uses it to refresh posters and metadata.

3. **Workflow** — `.github/workflows/whats_on_html.yml` runs daily (09:00 UTC) and on manual trigger. It commits `whats_on_data.json`, `.tmdb_cache.json`, `.whats_on_fingerprint` and `site/` only when the data fingerprint changes.

## Files

| Path | Purpose |
|------|--------|
| `whats_on_scraper.py` | Scrape listings, TMDb enrich, build HTML |
| `whats_on_data.json` | Cinema data (films, showtimes); committed when changed |
| `site/` | Generated site: `index.html`, `posters/`, `certs/`, `icons/` |
| `.tmdb_cache.json` | TMDb cache; committed to limit API use |
| `.whats_on_fingerprint` | Hash used to decide whether to commit after a run |

## License

[LICENSE](LICENSE).
