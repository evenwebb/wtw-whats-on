<div align="center">

# 🎬 WTW What's On

[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Workflow](https://img.shields.io/badge/Workflow-GitHub%20Actions-2088FF?logo=github-actions&logoColor=white)](https://github.com/evenwebb/wtw-whats-on/actions)
[![Pages](https://img.shields.io/badge/Deploy-GitHub%20Pages-222222?logo=githubpages&logoColor=white)](https://evenwebb.github.io/wtw-whats-on/)

Automated scraper and static-site generator for WTW Cinemas listings across **St Austell, Newquay, Truro, and Wadebridge**.
It enriches films with TMDb metadata and publishes a production-ready `docs/` site via GitHub Actions.

</div>

---

## 📚 Table of Contents

- [⚡ Quick Start](#-quick-start)
- [✨ Features](#-features)
- [📦 Installation](#-installation)
- [🚀 Usage](#-usage)
- [⚙️ Configuration](#️-configuration)
- [🩺 Health Checks](#-health-checks)
- [🤖 GitHub Actions](#-github-actions)
- [🚨 Failure Handling](#-failure-handling)
- [🌐 GitHub Pages Deployment](#-github-pages-deployment)
- [🧩 Dependencies](#-dependencies)
- [🛠️ Troubleshooting](#️-troubleshooting)
- [📄 License](#-license)

---

## ⚡ Quick Start

```bash
git clone https://github.com/evenwebb/wtw-whats-on.git
cd wtw-whats-on
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
TMDB_API_KEY=your_key_here python3 whats_on_scraper.py
```

Outputs:

- `whats_on_data.json`
- `docs/index.html`
- `docs/posters/`, `docs/certs/`, `docs/icons/`
- `.tmdb_cache.json`
- `.whats_on_fingerprint`
- `.cinema_failure_state.json`

---

## ✨ Features

| Feature | Description |
|---|---|
| `🏢 Multi-cinema scraping` | Scrapes all configured WTW cinemas concurrently. |
| `🧠 Markup drift diagnostics` | Primary selector + fallback selector mode tracking with per-cinema health metrics. |
| `🩺 Health-gated deploys` | Configurable minimum film/showtime/cinema thresholds to fail bad runs before publish. |
| `🧯 Outage tolerance` | Single-cinema failures can restore prior data and only fail after consecutive threshold breaches. |
| `🧩 TMDb enrichment` | Adds posters, trailers, ratings, cast/crew, and genres using `TMDB_API_KEY`. |
| `⚡ Fast no-change path` | Fingerprint skip avoids expensive enrichment/render when nothing changed. |
| `🖼️ Poster optimization` | Dedupe by movie key, local caching, and parallel poster download pass. |
| `🚨 Failure issue automation` | Structured failure issues with log/artifact links and signature-based dedupe. |
| `✅ Auto-close stale incidents` | Failure issues auto-close after 2 consecutive successful runs. |

---

## 📦 Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 🚀 Usage

Run locally:

```bash
TMDB_API_KEY=your_key_here python3 whats_on_scraper.py
```

Run with selected cinemas only:

```bash
WTW_ENABLED_CINEMAS="st-austell,truro" TMDB_API_KEY=your_key_here python3 whats_on_scraper.py
```

Force rebuild even if fingerprint unchanged:

```bash
FORCE_REBUILD=true TMDB_API_KEY=your_key_here python3 whats_on_scraper.py
```

---

## ⚙️ Configuration

### Core

| Variable | Default | Purpose |
|---|---|---|
| `TMDB_API_KEY` | unset | Required for TMDb enrichment in CI and recommended locally. |
| `WTW_ENABLED_CINEMAS` | all enabled | Comma-separated cinema slugs to scrape. |
| `WTW_INITIAL_SHOWINGS_VISIBLE` | `10` | Initial showings rendered per film card before "Show more". |
| `FORCE_REBUILD` | unset | If true, bypasses fingerprint skip. |
| `POSTER_MISSING_FAIL_THRESHOLD` | unset | Optional hard fail if too many unique films lack posters. |
| `MAX_CONSECUTIVE_CINEMA_FAILURES` | `2` | Fail run when a cinema keeps failing across runs. |

### Backward-compatible gates

| Variable | Default |
|---|---|
| `WTW_MIN_TOTAL_FILMS` | `1` |
| `WTW_MIN_TOTAL_SHOWTIMES` | `1` |
| `WTW_MIN_FILMS_PER_CINEMA` | `0` |
| `WTW_FAIL_ON_MARKUP_DRIFT` | `false` |

---

## 🩺 Health Checks

This project now supports explicit health gates for safer deploys.

| Variable | Default in CI | Meaning |
|---|---|---|
| `HEALTHCHECK_ENFORCE` | `true` | Fail run when any health gate is breached. |
| `HEALTH_MIN_TOTAL_FILMS` | `8` | Minimum total parsed films across non-excluded cinemas. |
| `HEALTH_MIN_TOTAL_SHOWTIMES` | `20` | Minimum total parsed showtimes across non-excluded cinemas. |
| `HEALTH_MIN_CINEMAS_WITH_FILMS` | `3` | Minimum cinemas with at least one parsed film. |
| `HEALTH_MIN_NOW_SHOWING_FILMS` | `3` | Minimum films classified as now showing. |
| `HEALTH_MAX_MARKUP_SUSPECT_CINEMAS` | `1` | Maximum cinemas allowed in fallback/none parser mode. |
| `HEALTH_EXCLUDED_CINEMAS` | `st-ives` | Comma list excluded from gating calculations. |

Health summary is logged on every run, including:

- total cinemas, films, showtimes
- cinemas with films
- now-showing films
- markup-suspect cinema count

---

## 🤖 GitHub Actions

Workflow file: [`.github/workflows/whats_on_html.yml`](.github/workflows/whats_on_html.yml)

It runs daily and on manual trigger, then:

1. Installs dependencies with pip cache.
2. Masks TMDb secret in logs.
3. Writes config snapshot to `logs/health_env.txt`.
4. Runs scraper and captures full output to `logs/scraper.log`.
5. Uploads `logs/` artifacts on failure.
6. Commits updated outputs only when changed.

Required repository secrets:

- `TMDB_API_KEY`
- `CREATE_FAILURE_ISSUE` (`true`/`false`)

Recommended repository variables (optional overrides):

- `HEALTH_MIN_TOTAL_FILMS`
- `HEALTH_MIN_TOTAL_SHOWTIMES`
- `HEALTH_MIN_CINEMAS_WITH_FILMS`
- `HEALTH_MIN_NOW_SHOWING_FILMS`
- `HEALTH_MAX_MARKUP_SUSPECT_CINEMAS`
- `HEALTH_EXCLUDED_CINEMAS`
- `MAX_CONSECUTIVE_CINEMA_FAILURES`
- `POSTER_MISSING_FAIL_THRESHOLD`

---

## 🚨 Failure Handling

When workflow fails and `CREATE_FAILURE_ISSUE=true`:

- Uses a stable failure signature derived from step context + normalized error excerpt.
- Opens/updates a single issue thread (`What's On workflow failures`).
- Adds one comment per **new signature** only (prevents spam).
- Reopens the issue if it was closed and failure recurs.
- Includes run link, artifacts link, config snapshot, and log excerpt.
- Tracks success streak and auto-closes after 2 consecutive successful runs.

---

## 🌐 GitHub Pages Deployment

1. Go to **Settings -> Pages**.
2. Set source: **Deploy from branch**.
3. Select branch `main` and folder `/docs`.
4. Save.

Published site updates after workflow commits.

---

## 🧩 Dependencies

| Package | Purpose |
|---|---|
| `requests` | HTTP requests and API calls |
| `beautifulsoup4` | HTML parsing |

---

## 🛠️ Troubleshooting

<details>
<summary><strong>Scraper suddenly returns very few films/showtimes</strong></summary>

- Check latest workflow run logs and `logs/health_env.txt` artifact.
- Look for `parser_mode=fallback` or selector warnings.
- Adjust `HEALTH_*` thresholds temporarily only if source data is genuinely low.

</details>

<details>
<summary><strong>No posters/trailers in output</strong></summary>

- Ensure `TMDB_API_KEY` exists in repo secrets.
- Confirm API key has valid TMDb access and is not rate-limited.
- Verify workflow logs for TMDb warnings.

</details>

<details>
<summary><strong>One cinema keeps failing</strong></summary>

- The scraper restores prior data for that cinema when possible.
- Check `.cinema_failure_state.json` counters.
- Increase `MAX_CONSECUTIVE_CINEMA_FAILURES` only if needed.

</details>

---

## 📄 License

This project is licensed under [GPL-3.0](LICENSE).

---

<div align="center">

Built by [evenwebb](https://github.com/evenwebb) • If this helps, star the repo ⭐

</div>
