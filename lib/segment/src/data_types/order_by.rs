use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::types::{Payload, Range, RangeInterface};

const INTERNAL_KEY_OF_ORDER_BY_VALUE: &str = "____ordered_with____";

#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Asc,
    Desc,
}

impl Direction {
    pub fn as_range_from<T>(&self, from: T) -> Range<T> {
        match self {
            Direction::Asc => Range {
                gte: Some(from),
                gt: None,
                lte: None,
                lt: None,
            },
            Direction::Desc => Range {
                lte: Some(from),
                gt: None,
                gte: None,
                lt: None,
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub enum StartFrom {
    Float(f64),
    Datetime(DateTime<Utc>),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub struct OrderBy {
    /// Payload key to order by
    pub key: String,

    /// Direction of ordering: `asc` or `desc`. Default is ascending.
    pub direction: Option<Direction>,

    /// Which payload value to start scrolling from. Default is the lowest value for `asc` and the highest for `desc`
    pub start_from: Option<StartFrom>,
}

impl OrderBy {
    /// If there is a start value, returns a range representation of OrderBy.
    pub fn as_range(&self) -> RangeInterface {
        self.start_from
            .as_ref()
            .map(|start_from| match start_from {
                StartFrom::Float(f) => RangeInterface::Float(self.direction().as_range_from(*f)),
                StartFrom::Datetime(dt) => {
                    RangeInterface::DateTime(self.direction().as_range_from(*dt))
                }
            })
            .unwrap_or_else(|| RangeInterface::Float(Range::default()))
    }

    pub fn direction(&self) -> Direction {
        self.direction.unwrap_or_default()
    }

    pub fn insert_order_value_in_payload(payload: Option<Payload>, value: f64) -> Payload {
        let mut new_payload = payload.unwrap_or_default();
        new_payload
            .0
            .insert(INTERNAL_KEY_OF_ORDER_BY_VALUE.to_string(), value.into());
        new_payload
    }

    pub fn remove_order_value_from_payload(&self, payload: Option<&mut Payload>) -> f64 {
        payload
            .and_then(|payload| payload.0.remove(INTERNAL_KEY_OF_ORDER_BY_VALUE))
            .and_then(|v| v.as_f64())
            .unwrap_or_else(|| match self.direction() {
                Direction::Asc => std::f64::MAX,
                Direction::Desc => std::f64::MIN,
            })
    }
}
