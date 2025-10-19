use chrono::{DateTime, Utc};
use serde::ser::{Serialize, Serializer, Error as _};

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct DateTimeParam(pub DateTime<Utc>);

impl DateTimeParam {
    pub fn new(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }
}

impl Serialize for DateTimeParam {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            let formatted = self.0.format("%Y-%m-%d %H:%M:%S").to_string();
            serializer.serialize_str(&formatted)
        } else {
            let ts = self.0.timestamp();
            u32::try_from(ts)
                .map_err(|_| S::Error::custom(format!("{} out of DateTime range", self.0)))?
                .serialize(serializer)
        }
    }
}

impl From<DateTime<Utc>> for DateTimeParam {
    fn from(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }
}
