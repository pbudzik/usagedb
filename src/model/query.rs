use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuerySource {
    Auto,
    RollupsOnly,
    RawOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupKey {
    Account,
    Subscription,
    Product,
    Meter,
    Model,
    Day,
    Hour,
    Dimension(String),
}
