# SYS-01: Business & Predictive Analytics Dashboard

SYS-01 is the primary front-facing analytical interface of the **Financial Analytics & Fraud Detection Enterprise Platform**.

It provides financial traders, risk analysts, and executive stakeholders with unified real-time market visualization, predictive AI price overlays, anomaly detection pinpoints, and transaction fraud monitoring.

## 📌 Architecture Reference
- Official Master Specification: `Plan/systems_docs/01_Business_Predictive_Analytics_Dashboard.md`
- Master Plan v2.0: `Plan/master_plan(update_2026_08_07).html`

## 🚀 Key Responsibilities
1. **Interactive Real-Time Candlestick Charting**: Sub-second rendering of OHLCV candles, SMA-20, RSI-14, and live Binance ticker updates.
2. **AI Predictive Trajectory (SYS-04 Integration)**: Visual overlay of 15-minute and 60-minute price forecast paths from LSTM models.
3. **Market Anomaly Markers**: Pin red 🔴 markers on candlesticks displaying abnormal volatility or volume bursts flagged by Isolation Forest.
4. **Fraud Risk Monitoring**: Real-time table tracking transactions evaluated by LightGBM model with fraud risk scoring and status (`BLOCKED`, `REVIEW`).
5. **Operational Alert Dispatch (SYS-06 Integration)**: Automatic alert payload forwarding to SYS-06 when trader observes manual anomaly or system triggers critical threshold.

## 🧭 Directory Structure & Documentation
- [CONTEXT.md](CONTEXT.md): Ubiquitous domain language, entity definitions, and SLA constraints.
- 🇻🇳 [docs/BUILD_PLAN_VI.md](docs/BUILD_PLAN_VI.md): Kế hoạch xây dựng chi tiết từ A-Z (Bản Tiếng Việt - Next.js + TradingView + FastAPI).
- 🇬🇧 [docs/BUILD_PLAN_EN.md](docs/BUILD_PLAN_EN.md): Detailed Engineering Build Plan from A-Z (English Edition - Next.js + TradingView + FastAPI).
- Legacy draft: [docs/BUILD_PLAN.md](docs/BUILD_PLAN.md).

Start with [docs/BUILD_PLAN_VI.md](docs/BUILD_PLAN_VI.md) or [docs/BUILD_PLAN_EN.md](docs/BUILD_PLAN_EN.md).
