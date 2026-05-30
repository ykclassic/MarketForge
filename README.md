# ProfitForge: Multi-Timeframe Signal Execution Pipeline

An institutional-grade, automated cryptocurrency signal pipeline. Engineered strictly for **quality over quantity**, this system filters market noise by calculating consensus across multiple timeframes (15m, 1h, 4h) using a vectorized indicator matrix.

High-probability alerts are dispatched directly to Discord via a headless, low-latency webhook bridge.

## Core Architecture
1. **Secure Data Ingestion:** Asynchronous k-line data fetching from XT.com API.
2. **Consensus Engine:** Pandas-TA vectorized calculations mapping Ichimoku Cloud, MACD, RSI, and Bollinger Bands.
3. **Headless Webhook Bridge:** No visual dashboard. Institutional "Rich Embeds" are delivered straight to Discord communities.
4. **Automated CI/CD:** 24/7 market monitoring cycles powered by GitHub Actions.

## Security & Performance
* **Zero Hardcoded Secrets:** All execution relies on environment variables and GitHub Secrets.
* **SQL Injection Prevention:** Strict typed schemas and parameterized SQLite queries (`signals.db`).
* **Non-Blocking I/O:** `aiohttp` and `aiosqlite` ensure network and disk operations never bottleneck the engine.

## Quick Start
Refer to the `GUIDE.md` file for comprehensive local setup, B2B client onboarding, and GitHub deployment instructions.
