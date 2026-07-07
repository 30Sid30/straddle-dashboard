# Changelog

All notable changes to the hosted dashboard are recorded here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).  
Versioning follows [Semantic Versioning](https://semver.org/) — `MAJOR.MINOR.PATCH`.

> **Sandbox vs hosted:** Changes are developed in the main `Backtesting/` directory  
> and promoted here when stable. This repo is production-only.

---

## [Unreleased]

---

## [1.3.0] — 2026-07-07

### Fixed
- `download_0dte.py`: stale-file bug where a contract file downloaded before
  expiry (by `download_data.py`) would be skipped even though it contained no
  expiry-day candles. Skip logic now checks `_has_expiry_day_data()` — only
  skips if the file already contains at least one candle on the expiry date.
  Previously, NIFTY Jun 30 PE files (data: Apr-Jun 12) were blocking the
  correct expiry-day download indefinitely.

---

## [1.2.0] — 2026-07-06

### Fixed
- SENSEX expiries with illiquid ATM strikes now fall back to the nearest
  liquid adjacent strike (±1, ±2, ±3 × interval) instead of showing
  "No contract data" error

### Changed
- Workflow now runs `download_0dte.py` before `straddle_dashboard.py`,
  pre-fetching all ATM ± 15 contracts for all 90 expiries. Dashboard HTML
  generation reads from disk entirely — no inline API calls, faster output.

---

## [1.1.0] — 2026-07-06

### Changed
- `N_EXPIRIES` increased from 10 → 90, covering all NIFTY and SENSEX expiries  
  from Jan 2025 to present (79 NIFTY + 83 SENSEX past expiries)
- Expiry cache lookback extended from 12 → 24 months so Jan–May 2025 expiries  
  are included in the calendar (previously the 12-month window missed them)

### Notes
- The dashboard UI filter (5 / 10 / 15 / Custom) still controls how many rows  
  are displayed — 90 is the maximum available, default view remains 10

---

## [1.0.0] — 2026-07-06

### Added
- `straddle_dashboard.py` — Bootstrap 5 HTML dashboard for 0DTE NIFTY + SENSEX  
  straddle premiums, ATM strikes, movement metrics (H-O / O-L / H-L in pts and  
  straddle multiples), gap from previous close, inline sparklines with click-to-expand  
  Chart.js modal, day classification (Trending / Volatile / Consolidating)
- `download_spot.py` — Incremental 1-min and daily spot downloader (NIFTY + SENSEX)
- `download_0dte.py` — Targeted 0DTE contract downloader with expiry cache,  
  ATM ± 15 strikes, expiry-day-only window (avoids full contract history)
- `.github/workflows/deploy.yml` — GitHub Actions workflow:  
  cron at 21:30 IST daily + manual trigger; data cached between runs via  
  `actions/cache`; deploys `straddle_dashboard.html` to GitHub Pages
- `requirements.txt`, `.gitignore`, `README.md`

### Security
- All credentials (`GROWW_API_KEY`, `GROWW_TOTP_SECRET`) stored exclusively as  
  GitHub Secrets — never committed to the repository

---

[Unreleased]: https://github.com/YOUR_USERNAME/YOUR_REPO/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/YOUR_USERNAME/YOUR_REPO/releases/tag/v1.0.0
