# Nexergy

Rust workspace for NL energy price modeling with a Parquet lake.

## Build

```bash
cargo build
```

## Ingestion CLI

- Prices JSON → Parquet
```bash
./target/debug/nexergy-cli ingest-prices \
  --input data/nl_energy_prices_2014.json \
  --out-dir data/curated/prices
```

- Gas JSON → Parquet
```bash
./target/debug/nexergy-cli ingest-gas \
  --input data/nl_gas_prices_2018-2025.json \
  --out-dir data/curated/fuels_gas
```

- KNMI hourly (uurgegevens) → Parquet
```bash
./target/debug/nexergy-cli ingest-knmi-hourly \
  --start 2024010101 --end 2024010124 \
  --stns 260 --vars TEMP:WIND:SUNR:PRCP:VICL \
  --out-dir data/curated/knmi_hourly
```

- KNMI daily (daggegevens) → Parquet
- ENTSO-E TimeSeries (XML) → Parquet
```bash
./target/debug/nexergy-cli ingest-entsoe \
  --base_url "https://transparency.entsoe.eu/api" \
  --security_token "$ENTSOE_TOKEN" \
  --params "documentType=A44" "processType=A01" "outBiddingZone_Domain=10YNL----------L" "periodStart=202401010000" "periodEnd=202401012300" \
  --out-dir data/curated/entsoe
```
```bash
./target/debug/nexergy-cli ingest-knmi-daily \
  --start 20240101 --end 20240107 \
  --stns 260 --vars TEMP:WIND:SUNR:PRCP:VICL \
  --out-dir data/curated/knmi_daily
```

## Train and Evaluate (train <= 2024, test = 2025)

1) Build release binary
```bash
cargo build --release --locked --all-features
```

2) Ingest prices JSONs into curated Parquet (one-time)
```bash
for y in 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025; do \
  ./target/release/nexergy-cli ingest-prices \
    --input data/nl_energy_prices_${y}.json \
    --out-dir data/curated/prices; \
done
```

3) Train on data up to and including 2024 and evaluate on 2025
```bash
./target/release/nexergy-cli train-eval \
  --prices-dir data/curated/prices \
  --cutoff-year 2024 \
  --lags 24 \
  --learning-rate 0.05 \
  --epochs 5000
```

Notes
- The model is a standardized linear regression trained with gradient descent using lag features of the target.
- Output prints train/test sizes, MAE/RMSE on 2025, and a persistence baseline for reference.

Notes
- KNMI script endpoints: https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script
- Output is partitioned by `dt=YYYY-MM-DD/part-0001.parquet`.
