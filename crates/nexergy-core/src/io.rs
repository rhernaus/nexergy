use anyhow::Result;
use polars::prelude::*;
use std::path::Path;

pub fn write_parquet<P: AsRef<Path>>(df: &mut DataFrame, path: P) -> Result<()> {
    let mut file = std::fs::File::create(path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(df)?;
    Ok(())
}
