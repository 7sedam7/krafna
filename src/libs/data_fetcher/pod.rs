use serde::{Deserialize, Serialize};

use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Pod {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Array(Vec<Pod>),
    Hash(HashMap<String, Pod>),
}

impl Pod {
    pub fn nested_get(&self, key: &str) -> Option<&Pod> {
        let mut current = self;
        for subkey in key.split('.') {
            match current {
                Pod::Hash(hash) => match hash.get(subkey) {
                    Some(pod) => current = pod,
                    None => return None,
                },
                _ => return None,
            }
        }
        Some(current)
    }

    pub fn new_hash() -> Pod {
        Pod::Hash(HashMap::new())
    }

    pub fn new_array() -> Pod {
        Pod::Array(Vec::new())
    }

    pub fn insert<T>(&mut self, key: String, value: T) -> Result<(), String>
    where
        T: Into<Pod>,
    {
        if let Pod::Hash(hash) = self {
            hash.insert(key, value.into());
            Ok(())
        } else {
            Err("Not a hash".to_string())
        }
    }

    pub fn push<T>(&mut self, value: T) -> Result<(), String>
    where
        T: Into<Pod>,
    {
        if let Pod::Array(array) = self {
            array.push(value.into());
            Ok(())
        } else {
            Err("Not an array".to_string())
        }
    }

    pub fn to_untagged_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(
            &self
                .to_gray_matter_pod()
                .deserialize::<serde_json::Value>()?,
        )
    }

    // TODO: Figure out how to better deal with untagged so i don't have to do this crazy
    // conversion hack
    pub fn to_gray_matter_pod(&self) -> gray_matter::Pod {
        match self {
            Pod::Array(array) => {
                gray_matter::Pod::Array(array.iter().map(|x| x.to_gray_matter_pod()).collect())
            }
            Pod::Hash(hash) => gray_matter::Pod::Hash(
                hash.iter()
                    .map(|(k, v)| (k.clone(), v.to_gray_matter_pod()))
                    .collect(),
            ),
            Pod::String(s) => gray_matter::Pod::String(s.clone()),
            Pod::Integer(i) => gray_matter::Pod::Integer(*i),
            Pod::Float(f) => gray_matter::Pod::Float(*f),
            Pod::Boolean(b) => gray_matter::Pod::Boolean(*b),
            Pod::Null => gray_matter::Pod::Null,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match *self {
            Pod::String(ref value) => Some(value.clone()),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match *self {
            Pod::Integer(ref value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match *self {
            Pod::Float(ref value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match *self {
            Pod::Boolean(ref value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_vec(&self) -> Option<Vec<Pod>> {
        match *self {
            Pod::Array(ref value) => Some(value.clone()),
            _ => None,
        }
    }

    pub fn as_hashmap(&self) -> Option<HashMap<String, Pod>> {
        match *self {
            Pod::Hash(ref value) => Some(value.clone()),
            _ => None,
        }
    }
}

impl std::fmt::Display for Pod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pod::Null => write!(f, "NULL"),
            Pod::String(ref value) => write!(f, "{}", value),
            Pod::Integer(ref value) => write!(f, "{}", value),
            Pod::Float(ref value) => write!(f, "{}", value),
            Pod::Boolean(ref value) => write!(f, "{}", value),
            _ => write!(f, "{}", self.to_untagged_json_string().unwrap()),
        }
    }
}

//impl ToString for Pod {
//    fn to_string(&self) -> String {
//        match self {
//            Pod::Null => "NULL".to_string(),
//            Pod::String(ref value) => value.clone(),
//            Pod::Integer(ref value) => value.to_string(),
//            Pod::Float(ref value) => value.to_string(),
//            Pod::Boolean(ref value) => value.to_string(),
//            _ => self.to_json_string().unwrap(),
//        }
//    }
//}

impl From<Pod> for String {
    fn from(val: Pod) -> Self {
        val.as_string().unwrap()
    }
}

impl From<Pod> for i64 {
    fn from(val: Pod) -> Self {
        val.as_i64().unwrap()
    }
}

impl From<Pod> for f64 {
    fn from(val: Pod) -> Self {
        val.as_f64().unwrap()
    }
}

impl From<Pod> for bool {
    fn from(val: Pod) -> Self {
        val.as_bool().unwrap()
    }
}

impl From<Pod> for Vec<Pod> {
    fn from(val: Pod) -> Self {
        val.as_vec().unwrap()
    }
}

impl From<Pod> for HashMap<String, Pod> {
    fn from(val: Pod) -> Self {
        val.as_hashmap().unwrap()
    }
}

impl From<i64> for Pod {
    fn from(val: i64) -> Self {
        Pod::Integer(val)
    }
}

impl From<f64> for Pod {
    fn from(val: f64) -> Self {
        Pod::Float(val)
    }
}

impl From<String> for Pod {
    fn from(val: String) -> Self {
        Pod::String(val)
    }
}

impl From<bool> for Pod {
    fn from(val: bool) -> Self {
        Pod::Boolean(val)
    }
}

impl From<Vec<Pod>> for Pod {
    fn from(val: Vec<Pod>) -> Self {
        Pod::Array(val)
    }
}

impl From<HashMap<String, Pod>> for Pod {
    fn from(val: HashMap<String, Pod>) -> Self {
        Pod::Hash(val)
    }
}
