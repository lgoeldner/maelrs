use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Address {
    Node { id: u32 },
    Client { id: u32 },
}

impl Address {
    pub fn id(&self) -> u32 {
        match self {
            Address::Node { id } | Address::Client { id } => *id,
        }
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Address::Node { id } => write!(f, "n{id}"),
            Address::Client { id } => write!(f, "c{id}"),
        }
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let (ch, num) = s.split_at(1);
        let id: u32 = num.parse().map_err(serde::de::Error::custom)?;
        match ch {
            "c" => Ok(Address::Client { id }),
            "n" => Ok(Address::Node { id }),
            _ => Err(serde::de::Error::custom(format!(
                "{ch} is not a valid Address type!"
            ))),
        }
    }
}

impl Serialize for Address {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}
