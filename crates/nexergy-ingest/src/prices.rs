use anyhow::{anyhow, Result};
use polars::prelude::*;
use serde::Deserialize;
use std::{fs, path::Path};

#[derive(Debug, Deserialize)]
struct PriceRowRaw {
    datum_nl: String,
    datum_utc: String,
    prijs_excl_belastingen: String,
}

fn parse_price_eur_mwh(s: &str) -> Result<f64> {
    let s = s.trim().replace(',', ".");
    let v: f64 = s.parse().map_err(|e| anyhow!("parse price '{s}': {e}"))?;
    Ok(v * 1000.0)
}

pub fn read_price_json_array_to_df<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let data = fs::read_to_string(path.as_ref())?;
    let rows: Vec<PriceRowRaw> = serde_json::from_str(&data)?;

    let mut datum_local: Vec<String> = Vec::with_capacity(rows.len());
    let mut datum_utc: Vec<String> = Vec::with_capacity(rows.len());
    let mut price_eur_mwh: Vec<f64> = Vec::with_capacity(rows.len());

    for r in rows.into_iter() {
        datum_local.push(r.datum_nl);
        datum_utc.push(r.datum_utc);
        price_eur_mwh.push(parse_price_eur_mwh(&r.prijs_excl_belastingen)?);
    }

    let df = df!(
        "datetime_local" => datum_local,
        "datetime_utc" => datum_utc,
        "price_eur_mwh" => price_eur_mwh,
    )?;
    Ok(df)
}

pub fn write_partitioned_by_date<P: AsRef<Path>>(df: &DataFrame, out_dir: P) -> Result<()> {
    let out_dir = out_dir.as_ref();
    fs::create_dir_all(out_dir)?;

    let s = df.column("datetime_utc")?;
    let mut by_date: std::collections::BTreeMap<String, Vec<usize>> = Default::default();
    for idx in 0..df.height() {
        let av = s.get(idx)?;
        let val = match av {
            AnyValue::String(v) => v,
            AnyValue::StringOwned(ref v) => v.as_str(),
            _ => continue,
        };
        if val.len() >= 10 {
            let date = &val[0..10];
            by_date.entry(date.to_string()).or_default().push(idx);
        }
    }

    for (date, indices) in by_date.into_iter() {
        let idx_vec: Vec<u32> = indices.into_iter().map(|i| i as u32).collect();
        let take_idx = UInt32Chunked::from_vec("idx".into(), idx_vec);
        let mut part = df.take(&take_idx)?;
        let path = out_dir.join(format!("dt={}/part-0001.parquet", date));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::File::create(&path)?;
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut part)?;
    }
    Ok(())
}
