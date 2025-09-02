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

Notes
- KNMI script endpoints: https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script
- Output is partitioned by `dt=YYYY-MM-DD/part-0001.parquet`.
