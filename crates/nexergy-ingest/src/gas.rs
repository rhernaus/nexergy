use anyhow::{anyhow, Result};
use polars::prelude::*;
use serde::Deserialize;
use std::{fs, path::Path};

#[derive(Debug, Deserialize)]
struct GasRowRaw {
    datum: String,
    prijs_excl_belastingen: String,
}

fn parse_price_eur_mwh(s: &str) -> Result<f64> {
    let s = s.trim().replace(',', ".");
    let v: f64 = s.parse().map_err(|e| anyhow!("parse price '{s}': {e}"))?;
    Ok(v * 1000.0)
}

pub fn read_gas_json_array_to_df<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let data = fs::read_to_string(path.as_ref())?;
    let rows: Vec<GasRowRaw> = serde_json::from_str(&data)?;

    let mut datetime_local: Vec<String> = Vec::with_capacity(rows.len());
    let mut price_eur_mwh: Vec<f64> = Vec::with_capacity(rows.len());

    for r in rows.into_iter() {
        datetime_local.push(r.datum);
        price_eur_mwh.push(parse_price_eur_mwh(&r.prijs_excl_belastingen)?);
    }

    let df = df!(
        "datetime_local" => datetime_local,
        "price_eur_mwh" => price_eur_mwh,
    )?;
    Ok(df)
}

pub fn write_partitioned_by_date<P: AsRef<Path>>(df: &DataFrame, out_dir: P) -> Result<()> {
    let out_dir = out_dir.as_ref();
    std::fs::create_dir_all(out_dir)?;

    let s = df.column("datetime_local")?;
    let mut by_date: std::collections::BTreeMap<String, Vec<usize>> = Default::default();
    for idx in 0..df.height() {
        let v = s.str_value(idx)?;
        let date = &v[0..10];
        by_date.entry(date.to_string()).or_default().push(idx);
    }

    for (date, indices) in by_date.into_iter() {
        let idx_vec: Vec<u32> = indices.into_iter().map(|i| i as u32).collect();
        let take_idx = UInt32Chunked::from_vec("idx".into(), idx_vec);
        let mut part = df.take(&take_idx)?;
        let path = out_dir.join(format!("dt={}/part-0001.parquet", date));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::File::create(&path)?;
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut part)?;
    }
    Ok(())
}
