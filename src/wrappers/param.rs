use serde::ser::{Serialize, Serializer};

pub trait ClickHouseParam: Serialize {
    fn serialize_param<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.serialize(serializer)
    }
}

impl<T: Serialize> ClickHouseParam for T {}
