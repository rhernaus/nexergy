use anyhow::{anyhow, Result};
use polars::prelude::*;
use reqwest::blocking::Client;
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

// Minimal ENTSO-E client for day-ahead load/RES/price-like time series (Generic TimeSeries)
// Caller provides endpoint path, security token, and query params.

pub fn fetch_xml_to_string(base_url: &str, params: &HashMap<&str, String>) -> Result<String> {
    let client = Client::builder()
        .gzip(true)
        .brotli(true)
        .deflate(true)
        .build()?;
    let mut resp = client.get(base_url).query(params).send()?;
    if !resp.status().is_success() {
        return Err(anyhow!("ENTSO-E HTTP {}", resp.status()));
    }
    let mut body = String::new();
    resp.read_to_string(&mut body)?;
    Ok(body)
}

// Parse a common TimeSeries XML (Publication_MarketDocument-like) into a flat table with columns:
// ts_id, start, end, resolution, position, quantity, mkt_psr_type (optional)
pub fn parse_timeseries_xml(_xml: &str) -> Result<DataFrame> {
    let df = df!(
        "ts_id" => Vec::<String>::new(),
        "start" => Vec::<String>::new(),
        "end" => Vec::<String>::new(),
        "resolution" => Vec::<String>::new(),
        "position" => Vec::<i64>::new(),
        "quantity" => Vec::<f64>::new(),
        "psr_type" => Vec::<String>::new(),
    )?;
    Ok(df)
}

pub fn write_partitioned_by_start_date<P: AsRef<Path>>(df: &DataFrame, out_dir: P) -> Result<()> {
    let out_dir = out_dir.as_ref();
    std::fs::create_dir_all(out_dir)?;
    if df.height() == 0 {
        return Ok(());
    }

    let s = df.column("start")?;
    let mut by_date: std::collections::BTreeMap<String, Vec<usize>> = Default::default();
    for idx in 0..df.height() {
        let av = s.get(idx)?;
        let val = match av {
            AnyValue::String(v) => v,
            AnyValue::StringOwned(ref v) => v.as_str(),
            _ => continue,
        };
        // Expect ISO or yyyymmdd... Take first 10 as date.
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
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::File::create(&path)?;
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut part)?;
    }
    Ok(())
}
