use std::collections::HashMap;

// Simple dictionary encoding for strings/IDs
#[derive(Debug, Default)]
pub struct DictionaryEncoder {
    dict: HashMap<String, u32>,
    reverse_dict: Vec<String>,
}

impl DictionaryEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn encode(&mut self, value: &str) -> u32 {
        if let Some(&id) = self.dict.get(value) {
            id
        } else {
            let id = self.reverse_dict.len() as u32;
            self.dict.insert(value.to_string(), id);
            self.reverse_dict.push(value.to_string());
            id
        }
    }

    pub fn get_dictionary(&self) -> &[String] {
        &self.reverse_dict
    }
}

// Simple delta encoding for timestamps
pub fn encode_deltas(values: &[i64]) -> Vec<i64> {
    let mut deltas = Vec::with_capacity(values.len());
    let mut last = 0;
    for &v in values {
        deltas.push(v - last);
        last = v;
    }
    deltas
}

pub fn decode_deltas(deltas: &[i64]) -> Vec<i64> {
    let mut values = Vec::with_capacity(deltas.len());
    let mut last = 0;
    for &d in deltas {
        let v = last + d;
        values.push(v);
        last = v;
    }
    values
}

// Zigzag encoding for i128 quantities
pub fn zigzag_encode(n: i128) -> u128 {
    ((n << 1) ^ (n >> 127)) as u128
}

pub fn zigzag_decode(n: u128) -> i128 {
    ((n >> 1) as i128) ^ (-((n & 1) as i128))
}
