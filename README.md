# 0DTE Straddle Dashboard

Live dashboard showing historical 0DTE ATM straddle analysis for NIFTY and SENSEX.  
Auto-refreshed nightly via GitHub Actions. Hosted on GitHub Pages.

**View the dashboard →** `https://YOUR_USERNAME.github.io/YOUR_REPO/`

---

## What it shows

For each of the last 10 completed expiries (NIFTY + SENSEX):

| Column | Description |
|---|---|
| Straddle 9:16 AM | ATM CE + PE premium at market open; re-evaluates ATM from spot at that minute |
| Straddle 10:00 AM | Same at 10:00 AM |
| ↑ / ↓ / ↔ | H-O, O-L, H-L from each checkpoint to EOD — in pts and as straddle multiples |
| Gap | Previous day's official close → today's 9:15 AM open (pts + %) |
| Spot Chart | Inline sparkline of the index for the full expiry day; click to expand |
| Day Type | Trending / Volatile / Consolidating based on 9:16 AM movement metrics |

**UI controls:** Filter by index (Combined / Nifty / Sensex) and number of expiries (5 / 10 / 15 / Custom). Average row updates dynamically with every filter change.

---

## How it updates

A GitHub Actions cron job runs every weekday at **9:30 PM IST** (16:00 UTC):

1. Restores cached market data (spot CSVs + option contract files)
2. Downloads only the incremental delta since the last run
3. Generates `straddle_dashboard.html`
4. Pushes the HTML to the `gh-pages` branch (served by GitHub Pages)
5. Saves the updated data back to cache

First run is slower (~5–10 min); subsequent runs are fast (~1–2 min).

A **manual trigger** is also available in the Actions tab if you need an immediate refresh.

---

## Repository structure

```
├── .github/workflows/deploy.yml   GitHub Actions workflow
├── straddle_dashboard.py          Dashboard generator
├── download_spot.py               Incremental NIFTY + SENSEX spot downloader
├── download_0dte.py               0DTE options contract downloader
├── requirements.txt
├── CHANGELOG.md
└── README.md
```

Data files (`Data/`, `NIFTY/`, `SENSEX/`) are downloaded at runtime and cached — never committed.

---

## Setup (first time)

### 1. Fork or clone this repo

### 2. Add GitHub Secrets

In your repo: **Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Value |
|---|---|
| `GROWW_API_KEY` | Your Groww API JWT |
| `GROWW_TOTP_SECRET` | Your Groww TOTP secret string |

### 3. Enable GitHub Pages

**Settings → Pages → Source → GitHub Actions**

### 4. Trigger the first run

**Actions tab → Update Dashboard → Run workflow**

---

## Development workflow

| Branch | Purpose |
|---|---|
| `main` | Production — every push triggers a deployment |
| `dev` | Staging — test changes here before merging to `main` |

Changes are developed in the separate sandbox directory (`Backtesting/`) and  
promoted to this repo when stable. See `CHANGELOG.md` for version history.

---

## Versioning

This project uses [Semantic Versioning](https://semver.org/):

- `MAJOR` — breaking change to dashboard output or schema
- `MINOR` — new column, new index, new feature
- `PATCH` — bug fix, styling tweak, performance improvement

Releases are tagged in git (`v1.0.0`, `v1.1.0`, etc.) and recorded in `CHANGELOG.md`.
