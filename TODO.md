# Energiespel: NL Power Price Prediction – TODO

This project aims to predict NL day-ahead hourly power prices (assumed bidding zone NL, CET/CEST) to compete on `https://jeroen.nl/energiespel`.

If any assumptions below conflict with the game’s rules, adjust before implementation.

## Open decisions to confirm (blocking)

- [ ] Target variable and horizon: NL EPEX day-ahead hourly price (D-1 for D, 24 hours)?
- [ ] Submission cadence and deadline (local time) and scoring metric used in the game
- [ ] Timezone policy: all data aligned to CET/CEST with DST handling
- [ ] Allowed data timing: only information available before DA gate closure (typically 12:00 CET/CEST D-1)
- [ ] Historical price file schema (path, columns, timezone)

## Prioritized drivers with API sources (Phase by impact)

Tier 0 – Must-have (highest impact first)

1) NL load (demand) day-ahead forecast
   - API: ENTSO-E Transparency Platform (requires token)
   - Why: Direct driver of price via demand side

2) NL wind and solar generation day-ahead forecast
   - API: ENTSO-E Transparency Platform; TenneT Data Platform (open data)
   - Why: Variable RES materially shifts marginal units and prices

3) German and Belgian wind/solar day-ahead forecasts (neighbors)
   - API: ENTSO-E Transparency Platform
   - Why: Cross-border coupling links NL price to DE/BE supply conditions

4) Natural gas benchmark (TTF) – spot/front contracts
   - API: TradingEconomics API (free key) as a proxy; paid: ICE/EEX
   - Why: Sets marginal cost for gas-fired generation (dominant in NL)

5) EU ETS carbon price (EUA)
   - API: TradingEconomics API (free key); paid: ICE/EEX
   - Why: Adds to variable cost of fossil generation

6) Weather forecast for NL (and bordering regions)
   - API: Open-Meteo (free) or KNMI Data Platform
   - Key vars: 2m temperature, wind speed at 100m, GHI/DNI, cloud cover
   - Why: Drives both load and RES production

7) Cross-border transmission capacity and planned outages
   - API: ENTSO-E (capacity/ATC, outages); TenneT Data Platform (NL grid)
   - Why: Interconnection limits influence net position and price convergence

Tier 1 – Strong next additions

8) Gas system fundamentals (supply availability)
   - API: GIE AGSI+ (gas storage levels), GIE ALSI (LNG sendout), ENTSOG Transparency (pipeline flows)
   - Why: Strong determinants of TTF dynamics when prices are stressed

9) Generation unit unavailability (planned/unplanned)
   - API: ENTSO-E outages; ACER REMIT UMM feed
   - Why: Large thermal/nuclear unit outages create scarcity and price spikes

10) Public holidays and calendar effects (NL/DE/BE)
   - API: Nager.Date
   - Why: Systematic demand pattern shifts by weekday/holiday

11) Hydrology where relevant (DE/NO via DE coupling)
   - API: National sources; ENTSO-E hydro reservoir levels
   - Why: Affects hydro availability and neighboring prices (secondary in NL)

Tier 2 – Nice-to-have/longer term

12) Coal ARA benchmark
   - API: TradingEconomics (proxy), paid vendor otherwise

13) FX (EURUSD) for fuel import costs (minor effect short-term)
   - API: ECB/FRED

14) EV charging/DSR signals (if available)
   - API: Operator/DSO data (often not public)

## Data sources – quick links

- ENTSO-E Transparency: `https://transparency.entsoe.eu/` (API key required)
- TenneT Data Platform: `https://data.tennet.eu/`
- TradingEconomics API: `https://api.tradingeconomics.com/` (free dev key)
- Open-Meteo API: `https://open-meteo.com/`
- KNMI Data Platform: `https://dataplatform.knmi.nl/`
- GIE AGSI+ (storage): `https://agsi.gie.eu/`
- GIE ALSI (LNG): `https://alsi.gie.eu/`
- ENTSOG Transparency: `https://transparency.entsog.eu/`
- Nager.Date: `https://date.nager.at/`

## File/data standards (to decide)

- [ ] Time index: 1h frequency, timezone CET/CEST; include both UTC and local columns
- [ ] Price target column: `price_eur_mwh`
- [ ] Source parity: store raw pulls in `data/raw/<source>/` and curated in `data/processed/`
- [ ] Schema for each dataset with data dictionary under `docs/data_schema.md`

## Phase plan

Phase 1 – Baseline and Tier 0 features

- [ ] Import historical NL day-ahead prices (user provides)
- [ ] Implement ENTSO-E client and fetch:
  - [ ] NL load DA forecast
  - [ ] NL wind/solar DA forecast
  - [ ] DE/BE wind/solar DA forecasts
- [ ] Implement Open-Meteo client for NL (and border regions)
- [ ] Implement TradingEconomics client for TTF and EUA
- [ ] Implement TenneT grid/asset availability (if endpoints open)
- [ ] Align all series to unified calendar and timezone
- [ ] Feature engineering v1 (lags, rolling stats, forecast deltas, calendar/holiday)
- [ ] Baseline models: persistence, linear ridge/lasso, LightGBM/XGBoost
- [ ] Backtesting: expanding-window time series CV; metrics MAE/RMSE and game metric
- [ ] Packaging: reproducible scripts for ingest, features, train, forecast

Phase 2 – Tier 1 features and robustness

- [ ] Add gas fundamentals (AGSI+, ALSI, ENTSOG)
- [ ] Add outages: ENTSO-E generation unavailability; ACER REMIT UMM
- [ ] Add public holidays via Nager.Date for NL/DE/BE
- [ ] Improve feature set (nonlinear interactions, weather-to-load/RES transforms)
- [ ] Candidate deep model: Temporal Fusion Transformer or N-BEATS
- [ ] Hyperparameter tuning with time-aware CV

Phase 3 – Ops and competition workflow

- [ ] Decide daily run schedule before DA gate closure; cron/GitHub Actions
- [ ] Secrets management for API keys (`.env`, 1Password, or GitHub Secrets)
- [ ] Generate submission file in the exact format the game requires
- [ ] Manual review dashboard (quick plots) and fallback strategy
- [ ] Monitoring: prediction error tracking; data freshness alerts

## Repo structure (proposed)

- `data/` – raw and processed datasets (git-ignored except small samples)
- `src/` – Python package with modules: `ingest/`, `features/`, `models/`, `utils/`
- `notebooks/` – EDA and experiments (light, synced to scripts)
- `config/` – YAML for sources, credentials, and settings
- `docs/` – data schema, API notes, and competition notes

## Immediate next actions

- [ ] Confirm game rules and submission/scoring specifics
- [ ] Define historical price file layout and place it in `data/raw/prices/`
- [ ] Register for ENTSO-E API key and TradingEconomics key
- [ ] Draft minimal ingestion scripts for Tier 0 sources
- [ ] Build baseline and first backtest to establish a benchmark

## Data folder audit and normalization

- [ ] Audit `data/` contents and unify filenames (e.g., duplicate `.json.json` extensions)
- [ ] Validate schemas of `nl_energy_prices_*.json` and `historical_gas_prices_nl.json`
- [ ] Move raw user-provided files to `data/raw/` with source-specific subfolders
- [ ] Create `docs/data_schema.md` describing each dataset and columns

## KNMI ingestion tasks

- [ ] Implement KNMI hourly fetch via POST for `uurgegevens`
  - Endpoint: `https://www.daggegevens.knmi.nl/klimatologie/uurgegevens` (CSV/JSON)
  - Params: `start=YYYYMMDDHH`, `end=YYYYMMDDHH`, `stns=ALL or list`, `vars=sets`
- [ ] Implement KNMI daily fetch via POST for `daggegevens`
  - Endpoint: `https://www.daggegevens.knmi.nl/klimatologie/daggegevens`
  - Params: `start=YYYYMMDD`, `end=YYYYMMDD`, `stns`, `vars`
- [ ] Map KNMI variables to modeling features (TEMP, WIND, SUNR, PRCP, VICL)
- [ ] Timezone reconcile: convert KNMI timestamps to CET/CEST and add UTC
- [ ] Store raw responses under `data/raw/knmi/` and curated under `data/processed/knmi/`
- [ ] Add source citations in `docs/data_sources.md` with KNMI page reference


