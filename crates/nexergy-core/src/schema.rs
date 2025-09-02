use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceRecord {
    pub datum_nl: String,
    pub datum_utc: String,
    pub prijs_excl_belastingen: String,
}
