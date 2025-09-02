use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::path::Path;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct LinearModel {
    pub feature_names: Vec<String>,
    pub weights: Vec<f64>, // length = features + 1 (bias)
    pub feature_means: Vec<f64>,
    pub feature_stds: Vec<f64>,
    pub target_mean: f64,
    pub target_std: f64,
}

impl LinearModel {
    pub fn predict_row(&self, x_raw: &[f64]) -> f64 {
        let mut y_std = self.weights[0];
        for (j, v_raw) in x_raw.iter().enumerate() {
            let x_std = if self.feature_stds[j] > 0.0 {
                (v_raw - self.feature_means[j]) / self.feature_stds[j]
            } else {
                0.0
            };
            y_std += self.weights[j + 1] * x_std;
        }
        self.target_mean + self.target_std * y_std
    }
}

pub fn make_lag_features(df: &DataFrame, target_col: &str, num_lags: usize) -> Result<DataFrame> {
    let mut df2 = df.clone();
    let target = df2.column(target_col)?.f64()?.clone();
    for lag in 1..=num_lags {
        let mut vals: Vec<Option<f64>> = Vec::with_capacity(target.len());
        for i in 0..target.len() {
            if i >= lag {
                vals.push(target.get(i - lag));
            } else {
                vals.push(None);
            }
        }
        let s = Series::new(format!("lag_{}", lag).into(), vals);
        df2.hstack_mut(&[s.into()])?;
    }
    Ok(df2)
}

pub fn drop_nulls_by_cols(df: &DataFrame, cols: &[String]) -> Result<DataFrame> {
    Ok(df.clone().drop_nulls(Some(cols))?)
}

fn reorder_by_string_column(df: &DataFrame, col: &str) -> Result<DataFrame> {
    let s = df.column(col)?;
    let mut pairs: Vec<(usize, String)> = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let av = s.get(i)?;
        let v = match av {
            AnyValue::String(v) => v.to_string(),
            AnyValue::StringOwned(ref v) => v.as_str().to_string(),
            _ => String::new(),
        };
        pairs.push((i, v));
    }
    pairs.sort_by(|a, b| a.1.cmp(&b.1));
    let idx: Vec<u32> = pairs.into_iter().map(|(i, _)| i as u32).collect();
    let take_idx = UInt32Chunked::from_vec("idx".into(), idx);
    Ok(df.take(&take_idx)?)
}

pub fn fit_linear_gd(
    df: &DataFrame,
    target_col: &str,
    feature_cols: &[String],
    learning_rate: f64,
    epochs: usize,
) -> Result<LinearModel> {
    if feature_cols.is_empty() {
        return Err(anyhow!("no features provided"));
    }
    let mut w = vec![0.0_f64; feature_cols.len() + 1];
    let y_ca = df
        .column(target_col)?
        .as_series()
        .ok_or_else(|| anyhow!("expected Series for target column"))?
        .f64()?;
    let n = y_ca.len();
    let mut feats: Vec<ChunkedArray<Float64Type>> = Vec::with_capacity(feature_cols.len());
    for c in feature_cols.iter() {
        let ca = df
            .column(c)?
            .as_series()
            .ok_or_else(|| anyhow!("expected Series for feature column"))?
            .f64()?
            .clone();
        feats.push(ca);
    }

    // Compute standardization parameters on train
    let mut feat_means: Vec<f64> = Vec::with_capacity(feature_cols.len());
    let mut feat_stds: Vec<f64> = Vec::with_capacity(feature_cols.len());
    for ca in feats.iter() {
        let mut sum = 0.0_f64;
        let mut cnt = 0usize;
        for i in 0..n {
            if let Some(v) = ca.get(i) {
                sum += v;
                cnt += 1;
            }
        }
        let mean = if cnt > 0 { sum / (cnt as f64) } else { 0.0 };
        let mut ssd = 0.0_f64;
        for i in 0..n {
            if let Some(v) = ca.get(i) {
                let d = v - mean;
                ssd += d * d;
            }
        }
        let std = if cnt > 1 {
            (ssd / (cnt as f64)).sqrt()
        } else {
            1.0
        };
        feat_means.push(mean);
        feat_stds.push(if std > 0.0 { std } else { 1.0 });
    }
    let mut y_sum = 0.0_f64;
    let mut y_cnt = 0usize;
    for i in 0..n {
        if let Some(v) = y_ca.get(i) {
            y_sum += v;
            y_cnt += 1;
        }
    }
    let y_mean = if y_cnt > 0 {
        y_sum / (y_cnt as f64)
    } else {
        0.0
    };
    let mut y_ssd = 0.0_f64;
    for i in 0..n {
        if let Some(v) = y_ca.get(i) {
            let d = v - y_mean;
            y_ssd += d * d;
        }
    }
    let y_std = if y_cnt > 1 {
        (y_ssd / (y_cnt as f64)).sqrt()
    } else {
        1.0
    };
    let y_std = if y_std > 0.0 { y_std } else { 1.0 };

    for _ in 0..epochs {
        let mut g0 = 0.0_f64; // bias grad
        let mut g = vec![0.0_f64; feature_cols.len()];
        for i in 0..n {
            let yi = if let Some(v) = y_ca.get(i) {
                (v - y_mean) / y_std
            } else {
                0.0
            };
            let mut xi: Vec<f64> = Vec::with_capacity(feature_cols.len());
            for (j, f) in feats.iter().enumerate() {
                let raw = f.get(i).unwrap_or(0.0);
                let std = (raw - feat_means[j]) / feat_stds[j];
                xi.push(std);
            }
            let yhat = w[0]
                + xi.iter()
                    .enumerate()
                    .map(|(j, v)| w[j + 1] * v)
                    .sum::<f64>();
            let err = yhat - yi;
            g0 += err;
            for j in 0..g.len() {
                g[j] += err * xi[j];
            }
        }
        let scale = 1.0 / (n as f64);
        w[0] -= learning_rate * g0 * scale;
        for j in 0..g.len() {
            w[j + 1] -= learning_rate * g[j] * scale;
        }
    }
    Ok(LinearModel {
        feature_names: feature_cols.to_vec(),
        weights: w,
        feature_means: feat_means,
        feature_stds: feat_stds,
        target_mean: y_mean,
        target_std: y_std,
    })
}

pub fn predict_df(model: &LinearModel, df: &DataFrame) -> Result<Series> {
    let mut out: Vec<f64> = Vec::with_capacity(df.height());
    let mut feats: Vec<ChunkedArray<Float64Type>> = Vec::with_capacity(model.feature_names.len());
    for c in model.feature_names.iter() {
        let ca = df
            .column(c)?
            .as_series()
            .ok_or_else(|| anyhow!("expected Series for feature column"))?
            .f64()?
            .clone();
        feats.push(ca);
    }
    for i in 0..df.height() {
        let mut xi: Vec<f64> = Vec::with_capacity(feats.len());
        for f in &feats {
            xi.push(f.get(i).unwrap_or(0.0));
        }
        out.push(model.predict_row(&xi));
    }
    Ok(Series::new("yhat".into(), out))
}

pub fn train_test_split_by_year(
    df: &DataFrame,
    date_col: &str,
    cutoff_year: i32,
) -> Result<(DataFrame, DataFrame)> {
    let s = df.column(date_col)?;
    let mut train_idx: Vec<u32> = Vec::new();
    let mut test_idx: Vec<u32> = Vec::new();
    for i in 0..df.height() {
        let av = s.get(i)?;
        let v = match av {
            AnyValue::String(v) => v,
            AnyValue::StringOwned(ref v) => v.as_str(),
            _ => continue,
        };
        // Expect YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
        if v.len() >= 4 {
            if let Ok(y) = v[0..4].parse::<i32>() {
                if y <= cutoff_year {
                    train_idx.push(i as u32);
                } else if y == cutoff_year + 1 {
                    test_idx.push(i as u32);
                }
            }
        }
    }
    let train = df.take(&UInt32Chunked::from_vec("idx".into(), train_idx))?;
    let test = df.take(&UInt32Chunked::from_vec("idx".into(), test_idx))?;
    Ok((train, test))
}

pub fn mean_absolute_error(y_true: &Series, y_pred: &Series) -> Result<f64> {
    let yt = y_true.f64()?;
    let yp = y_pred.f64()?;
    let n = yt.len().min(yp.len());
    let mut sum = 0.0_f64;
    let mut cnt = 0usize;
    for i in 0..n {
        if let (Some(a), Some(b)) = (yt.get(i), yp.get(i)) {
            sum += (a - b).abs();
            cnt += 1;
        }
    }
    Ok(if cnt > 0 {
        sum / (cnt as f64)
    } else {
        f64::NAN
    })
}

pub fn root_mean_squared_error(y_true: &Series, y_pred: &Series) -> Result<f64> {
    let yt = y_true.f64()?;
    let yp = y_pred.f64()?;
    let n = yt.len().min(yp.len());
    let mut sum = 0.0_f64;
    let mut cnt = 0usize;
    for i in 0..n {
        if let (Some(a), Some(b)) = (yt.get(i), yp.get(i)) {
            let d = a - b;
            sum += d * d;
            cnt += 1;
        }
    }
    Ok(if cnt > 0 {
        (sum / (cnt as f64)).sqrt()
    } else {
        f64::NAN
    })
}

pub fn read_partitioned_parquet(dir: &Path) -> Result<DataFrame> {
    let mut dfs: Vec<DataFrame> = Vec::new();
    for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            let df = ParquetReader::new(std::fs::File::open(path)?).finish()?;
            dfs.push(df);
        }
    }
    if dfs.is_empty() {
        return Err(anyhow!("no parquet files found under {}", dir.display()));
    }
    let mut it = dfs.into_iter();
    let mut acc = it.next().unwrap();
    for df in it {
        acc.vstack_mut(&df)?;
    }
    Ok(acc)
}

fn drop_non_finite_by_cols(df: &DataFrame, cols: &[String]) -> Result<DataFrame> {
    let n = df.height();
    let mut keep: Vec<bool> = vec![true; n];
    for c in cols.iter() {
        let ca = df
            .column(c)?
            .as_series()
            .ok_or_else(|| anyhow!("expected Series for numeric column"))?
            .f64()?;
        for i in 0..n {
            if let Some(v) = ca.get(i) {
                if !v.is_finite() {
                    keep[i] = false;
                }
            } else {
                keep[i] = false;
            }
        }
    }
    let mask = BooleanChunked::from_iter_values("mask".into(), keep.into_iter());
    Ok(df.filter(&mask)?)
}

pub struct TrainEvalResult {
    pub model: Option<LinearModel>,
    pub mae: f64,
    pub rmse: f64,
    pub train_n: usize,
    pub test_n: usize,
    pub baseline_mae: Option<f64>,
    pub baseline_rmse: Option<f64>,
}

pub fn train_eval_from_curated(
    prices_dir: &Path,
    target_col: &str,
    date_col: &str,
    lags: usize,
    cutoff_year: i32,
    learning_rate: f64,
    epochs: usize,
) -> Result<TrainEvalResult> {
    let df_prices = read_partitioned_parquet(prices_dir)?;
    let df_prices = reorder_by_string_column(&df_prices, date_col)?;
    let mut df_feat = make_lag_features(&df_prices, target_col, lags)?;
    let mut cols_needed: Vec<String> = vec![target_col.to_string(), date_col.to_string()];
    let mut lag_cols: Vec<String> = Vec::new();
    for lag in 1..=lags {
        let name = format!("lag_{}", lag);
        cols_needed.push(name.clone());
        lag_cols.push(name);
    }
    df_feat = drop_nulls_by_cols(&df_feat, &cols_needed)?;
    // Filter non-finite only on numeric columns (target + lags)
    let mut numeric_cols = vec![target_col.to_string()];
    numeric_cols.extend(lag_cols.into_iter());
    df_feat = drop_non_finite_by_cols(&df_feat, &numeric_cols)?;

    let (train, test) = train_test_split_by_year(&df_feat, date_col, cutoff_year)?;
    let feature_cols: Vec<String> = (1..=lags).map(|i| format!("lag_{}", i)).collect();

    let train_n = train.height();
    let test_n = test.height();

    // Baseline: persistence (yhat = lag_1)
    let baseline = if test_n > 0 {
        let y_true = test
            .column(target_col)?
            .as_series()
            .ok_or_else(|| anyhow!("expected Series for target column"))?
            .clone();
        let yhat = test
            .column("lag_1")?
            .as_series()
            .ok_or_else(|| anyhow!("expected Series for lag_1"))?
            .clone();
        Some((
            mean_absolute_error(&y_true, &yhat)?,
            root_mean_squared_error(&y_true, &yhat)?,
        ))
    } else {
        None
    };

    if train_n == 0 || test_n == 0 {
        return Ok(TrainEvalResult {
            model: None,
            mae: f64::NAN,
            rmse: f64::NAN,
            train_n,
            test_n,
            baseline_mae: baseline.map(|b| b.0),
            baseline_rmse: baseline.map(|b| b.1),
        });
    }

    let model = fit_linear_gd(&train, target_col, &feature_cols, learning_rate, epochs)?;
    let y_true = test
        .column(target_col)?
        .as_series()
        .ok_or_else(|| anyhow!("expected Series for target column"))?
        .clone();
    let y_pred = predict_df(&model, &test)?;
    let mae = mean_absolute_error(&y_true, &y_pred)?;
    let rmse = root_mean_squared_error(&y_true, &y_pred)?;
    Ok(TrainEvalResult {
        model: Some(model),
        mae,
        rmse,
        train_n,
        test_n,
        baseline_mae: baseline.map(|b| b.0),
        baseline_rmse: baseline.map(|b| b.1),
    })
}
