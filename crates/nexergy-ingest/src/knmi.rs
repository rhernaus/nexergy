use anyhow::{anyhow, Result};
use polars::prelude::*;
use reqwest::blocking::Client;
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

const KNMI_HOURLY_URL: &str = "https://www.daggegevens.knmi.nl/klimatologie/uurgegevens";
const KNMI_DAILY_URL: &str = "https://www.daggegevens.knmi.nl/klimatologie/daggegevens";

fn post_csv(url: &str, form: &HashMap<&str, String>) -> Result<String> {
    let client = Client::builder()
        .gzip(true)
        .brotli(true)
        .deflate(true)
        .build()?;
    let mut resp = client.post(url).form(form).send()?;
    let mut body = String::new();
    resp.read_to_string(&mut body)?;
    Ok(body)
}

fn csv_without_comments(s: &str) -> String {
    s.lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn fetch_knmi_hourly_to_df(
    start: &str,
    end: &str,
    stns: &str,
    vars: &str,
) -> Result<DataFrame> {
    let mut form = HashMap::new();
    form.insert("start", start.to_string());
    form.insert("end", end.to_string());
    form.insert("stns", stns.to_string());
    form.insert("vars", vars.to_string());
    form.insert("fmt", "csv".to_string());

    let csv = post_csv(KNMI_HOURLY_URL, &form)?;
    let csv = csv_without_comments(&csv);
    let df = CsvReader::new(std::io::Cursor::new(csv)).finish()?;
    Ok(df)
}

pub fn fetch_knmi_daily_to_df(start: &str, end: &str, stns: &str, vars: &str) -> Result<DataFrame> {
    let mut form = HashMap::new();
    form.insert("start", start.to_string());
    form.insert("end", end.to_string());
    form.insert("stns", stns.to_string());
    form.insert("vars", vars.to_string());
    form.insert("fmt", "csv".to_string());

    let csv = post_csv(KNMI_DAILY_URL, &form)?;
    let csv = csv_without_comments(&csv);
    let df = CsvReader::new(std::io::Cursor::new(csv)).finish()?;
    Ok(df)
}

fn normalize_date_str(df: &DataFrame) -> Result<(DataFrame, String)> {
    // Try to find the date column (usually "YYYYMMDD") allowing whitespace
    let mut found: Option<String> = None;
    for name in df.get_column_names() {
        if name.trim() == "YYYYMMDD" {
            found = Some(name.to_string());
            break;
        }
    }
    let date_col = found.ok_or_else(|| {
        anyhow!(
            "KNMI date column 'YYYYMMDD' not present; columns: {:?}",
            df.get_column_names()
        )
    })?;

    let s = df.column(&date_col)?;
    let date_series: Series = match s.dtype() {
        DataType::String => {
            let mut out: Vec<String> = Vec::with_capacity(s.len());
            for idx in 0..s.len() {
                let v = s.str_value(idx)?;
                let v = v.trim();
                if v.len() >= 8 {
                    out.push(format!("{}-{}-{}", &v[0..4], &v[4..6], &v[6..8]));
                } else {
                    out.push(v.to_string());
                }
            }
            Series::new("date_str".into(), out)
        }
        DataType::Int32 => {
            let ca = s.i32()?;
            let mut out: Vec<String> = Vec::with_capacity(ca.len());
            for opt in ca.into_iter() {
                if let Some(v) = opt {
                    out.push(format!(
                        "{:04}-{:02}-{:02}",
                        v / 10000,
                        (v / 100) % 100,
                        v % 100
                    ));
                } else {
                    out.push(String::new());
                }
            }
            Series::new("date_str".into(), out)
        }
        DataType::Int64 => {
            let ca = s.i64()?;
            let mut out: Vec<String> = Vec::with_capacity(ca.len());
            for opt in ca.into_iter() {
                if let Some(v) = opt {
                    out.push(format!(
                        "{:04}-{:02}-{:02}",
                        v / 10000,
                        (v / 100) % 100,
                        v % 100
                    ));
                } else {
                    out.push(String::new());
                }
            }
            Series::new("date_str".into(), out)
        }
        _ => {
            return Err(anyhow!(
                "Unsupported dtype for date column: {:?}",
                s.dtype()
            ))
        }
    };

    let mut df2 = df.clone();
    df2.hstack_mut(&[date_series])?;
    Ok((df2, "date_str".to_string()))
}

pub fn write_partitioned_by_date<P: AsRef<Path>>(
    df: &DataFrame,
    date_col: &str,
    out_dir: P,
) -> Result<()> {
    let out_dir = out_dir.as_ref();
    std::fs::create_dir_all(out_dir)?;

    // Normalize/ensure a YYYY-MM-DD string column exists
    let (df2, date_col_norm) =
        if date_col == "date_str" && df.get_column_names().iter().any(|n| n == &"date_str") {
            (df.clone(), date_col.to_string())
        } else {
            normalize_date_str(df)?
        };

    let s = df2.column(&date_col_norm)?;
    let mut by_date: std::collections::BTreeMap<String, Vec<usize>> = Default::default();
    for idx in 0..df2.height() {
        let v = s.str_value(idx)?;
        let date = &v[0..10];
        by_date.entry(date.to_string()).or_default().push(idx);
    }

    for (date, indices) in by_date.into_iter() {
        let idx_vec: Vec<u32> = indices.into_iter().map(|i| i as u32).collect();
        let take_idx = UInt32Chunked::from_vec("idx".into(), idx_vec);
        let mut part = df2.take(&take_idx)?;
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
