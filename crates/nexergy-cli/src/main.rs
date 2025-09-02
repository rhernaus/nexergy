use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(name = "ingest-prices")]
    Prices {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, name = "out-dir")]
        out_dir: PathBuf,
    },
    #[command(name = "ingest-gas")]
    Gas {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, name = "out-dir")]
        out_dir: PathBuf,
    },
    #[command(name = "ingest-knmi-hourly")]
    KnmiHourly {
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: String,
        #[arg(long)]
        stns: String,
        #[arg(long)]
        vars: String,
        #[arg(long, name = "out-dir")]
        out_dir: PathBuf,
    },
    #[command(name = "ingest-knmi-daily")]
    KnmiDaily {
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: String,
        #[arg(long)]
        stns: String,
        #[arg(long)]
        vars: String,
        #[arg(long, name = "out-dir")]
        out_dir: PathBuf,
    },
    #[command(name = "ingest-entsoe")]
    Entsoe {
        #[arg(long)]
        base_url: String,
        #[arg(long)]
        security_token: String,
        #[arg(long)]
        params: Vec<String>,
        #[arg(long, name = "out-dir")]
        out_dir: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Prices { input, out_dir } => {
            let df = nexergy_ingest::prices::read_price_json_array_to_df(&input)?;
            nexergy_ingest::prices::write_partitioned_by_date(&df, &out_dir)?;
        }
        Commands::Gas { input, out_dir } => {
            let df = nexergy_ingest::gas::read_gas_json_array_to_df(&input)?;
            nexergy_ingest::gas::write_partitioned_by_date(&df, &out_dir)?;
        }
        Commands::KnmiHourly {
            start,
            end,
            stns,
            vars,
            out_dir,
        } => {
            let df = nexergy_ingest::knmi::fetch_knmi_hourly_to_df(&start, &end, &stns, &vars)?;
            nexergy_ingest::knmi::write_partitioned_by_date(&df, "YYYYMMDD", &out_dir)?;
        }
        Commands::KnmiDaily {
            start,
            end,
            stns,
            vars,
            out_dir,
        } => {
            let df = nexergy_ingest::knmi::fetch_knmi_daily_to_df(&start, &end, &stns, &vars)?;
            nexergy_ingest::knmi::write_partitioned_by_date(&df, "YYYYMMDD", &out_dir)?;
        }
        Commands::Entsoe {
            base_url,
            security_token,
            params,
            out_dir,
        } => {
            // Parse key=value pairs
            let mut q: std::collections::HashMap<&str, String> = std::collections::HashMap::new();
            q.insert("securityToken", security_token);
            for kv in params {
                if let Some((k, v)) = kv.split_once('=') {
                    q.insert(Box::leak(k.to_string().into_boxed_str()), v.to_string());
                }
            }
            let xml = nexergy_ingest::entsoe::fetch_xml_to_string(&base_url, &q)?;
            let df = nexergy_ingest::entsoe::parse_timeseries_xml(&xml)?;
            nexergy_ingest::entsoe::write_partitioned_by_start_date(&df, &out_dir)?;
        }
    }
    Ok(())
}
