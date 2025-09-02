use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::Path;
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
    #[command(name = "train-eval")]
    TrainEval {
        #[arg(long, name = "prices-dir")]
        prices_dir: PathBuf,
        #[arg(long, default_value_t = 2024)]
        cutoff_year: i32,
        #[arg(long, default_value_t = 24)]
        lags: usize,
        #[arg(long, default_value_t = 0.01)]
        learning_rate: f64,
        #[arg(long, default_value_t = 2000)]
        epochs: usize,
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
        Commands::TrainEval {
            prices_dir,
            cutoff_year,
            lags,
            learning_rate,
            epochs,
        } => {
            let res = nexergy_model::train_eval_from_curated(
                Path::new(&prices_dir),
                "price_eur_mwh",
                "datetime_utc",
                lags,
                cutoff_year,
                learning_rate,
                epochs,
            )?;
            if let Some(model) = res.model {
                println!(
                    "train_n={}, test_n={}, MAE={:.3}, RMSE={:.3}, baseline_MAE={:?}, baseline_RMSE={:?}, features={}, weights={:?}",
                    res.train_n,
                    res.test_n,
                    res.mae,
                    res.rmse,
                    res.baseline_mae,
                    res.baseline_rmse,
                    model.feature_names.len(),
                    model.weights
                );
            } else {
                println!(
                    "train_n={}, test_n={}, MAE={:.3}, RMSE={:.3}, baseline_MAE={:?}, baseline_RMSE={:?}",
                    res.train_n,
                    res.test_n,
                    res.mae,
                    res.rmse,
                    res.baseline_mae,
                    res.baseline_rmse,
                );
            }
        }
    }
    Ok(())
}
