use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SmallDimensions {
    pub inner: BTreeMap<String, String>,
}

impl SmallDimensions {
    pub fn new() -> Self {
        Self { inner: BTreeMap::new() }
    }
}
