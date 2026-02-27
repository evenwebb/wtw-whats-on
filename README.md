<div align="center">

# 🎬 WTW What's On

Automatically scrapes WTW Cinemas St Austell listings, optionally enriches films with TMDb metadata, and publishes an updated static site in `docs/` for GitHub Pages.

</div>

---

## 📚 Table of Contents

- [⚡ Quick Start](#-quick-start)
- [✨ Features](#-features)
- [📦 Installation](#-installation)
- [🚀 Usage](#-usage)
- [⚙️ Configuration](#️-configuration)
- [🤖 GitHub Actions Automation](#-github-actions-automation)
- [🌐 GitHub Pages Deployment](#-github-pages-deployment)
- [🧩 Dependencies](#-dependencies)
- [🛠️ Troubleshooting](#️-troubleshooting)
- [⚠️ Known Limitations](#️-known-limitations)
- [📄 License](#-license)

---

## ⚡ Quick Start

```bash
git clone https://github.com/evenwebb/wtw-whats-on.git
cd wtw-whats-on
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 whats_on_scraper.py
```

Generated outputs:

- `whats_on_data.json`
- `docs/index.html`
- `docs/posters/` and related assets

---

## ✨ Features

| Feature | Description |
|---|---|
| `🎬 Listings + Showtimes` | Scrapes current films and grouped showtimes from WTW St Austell. |
| `🏷️ Rich Showtime Metadata` | Preserves screen labels and accessibility/format tags in generated output. |
| `🧩 TMDb Enrichment` | Adds posters, trailers, ratings, cast, crew, and genres (required for CI publishing). |
| `💾 Smart Caching` | Reuses TMDb cache data to reduce API requests and runtime cost. |
| `🧮 Fingerprint-Based Commits` | Uses deterministic fingerprinting to avoid unnecessary repository commits. |
| `🌐 Static Site Output` | Regenerates `docs/index.html` and assets for GitHub Pages publishing. |
| `🤖 Automated Daily Updates` | GitHub Actions runs on schedule/manual trigger with retries and optional failure issue creation. |

---

## 📦 Installation

```bash
git clone https://github.com/evenwebb/wtw-whats-on.git
cd wtw-whats-on
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 🚀 Usage

```bash
python3 whats_on_scraper.py
```

The script refreshes listings, updates `whats_on_data.json`, and regenerates the `docs/` site output.

---

## ⚙️ Configuration

Configuration is set in `whats_on_scraper.py` and via environment variables.

| Variable/Option | Default | Description |
|---|---|---|
| `TMDB_API_KEY` (env) | unset | Enables TMDb enrichment. Required in CI workflow. |
| `POSTER_MISSING_FAIL_THRESHOLD` (env) | unset locally | Optional quality gate. If set, scraper fails when missing-poster count exceeds threshold. |
| `HTTP_TIMEOUT` | `60` | Request timeout in seconds. |
| `HTTP_RETRIES` | `3` | Number of HTTP retries per request. |
| `HTTP_RETRY_DELAY` | `1` | Initial backoff delay in seconds. |
| `HTTP_RETRY_MULTIPLIER` | `2` | Retry delay multiplier. |
| `TMDB_CACHE_FILE` | `.tmdb_cache.json` | TMDb cache storage file. |
| `TMDB_CACHE_DAYS` | `30` | Cache retention window in days. |
| `DATA_FILE` | `whats_on_data.json` | Scraped data output file. |
| `FINGERPRINT_FILE` | `.whats_on_fingerprint` | Hash file used to detect meaningful changes. |
| `SITE_DIR` | `docs` | Generated static site output directory. |

---

## 🤖 GitHub Actions Automation

This repo includes `.github/workflows/whats_on_html.yml`:

- `⏰` Runs daily at `09:00 UTC`
- `🖱️` Supports manual runs (`workflow_dispatch`)
- `🔁` Retries scraper runs before failing (`SCRAPER_RUN_ATTEMPTS`, default `2`)
- `📝` Commits output files only when changed
- `🚨` Optionally opens or updates a GitHub issue on failure (`CREATE_FAILURE_ISSUE=true`)

Configure these repository secrets as needed:

- `TMDB_API_KEY` (required)
- `CREATE_FAILURE_ISSUE` (`true`/`false`)
- `SCRAPER_RUN_ATTEMPTS` (integer)

Optional repository variable:

- `POSTER_MISSING_FAIL_THRESHOLD` (default workflow value: `8`)

---

## 🌐 GitHub Pages Deployment

1. In GitHub, open **Settings -> Pages**.
2. Set source to **Deploy from a branch**.
3. Select branch `main` and folder `/docs`.
4. Save.

After each workflow update, the published site refreshes from the latest committed `docs/` output.

---

## 🧩 Dependencies

| Package | Purpose |
|---|---|
| `requests` | HTTP requests for listings, metadata, and assets |
| `beautifulsoup4` | HTML parsing for listing extraction |

---

## 🛠️ Troubleshooting

- `🧱` If listings fail to parse, the source HTML structure may have changed.
- `🔑` If posters/trailers are missing, verify `TMDB_API_KEY` and API quota.
- `🖼️` Missing posters render a local fallback image (`docs/posters/placeholder.svg`) instead of blank space.
- `🚦` If updates are not appearing, check workflow logs and Pages build status.

---

## ⚠️ Known Limitations

- `🌐` Site parsing depends on current WTW page markup.
- `🎯` TMDb matches are best-effort and may occasionally select imperfect title matches.

---

## 📄 License

[GPL-3.0](LICENSE)
