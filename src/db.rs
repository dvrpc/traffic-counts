//! Interact with database.
use log::error;

use crate::{
    CountError, FifteenMinuteSpeedRangeCount, FifteenMinuteVehicleClassCount, NonNormalVolCount,
};

pub trait Insert {
    type Item;
    fn insert(item: Self::Item) -> Result<(), CountError<'static>>;
}

impl Insert for FifteenMinuteVehicleClassCount {
    type Item = FifteenMinuteVehicleClassCount;
    fn insert(_: FifteenMinuteVehicleClassCount) -> Result<(), CountError<'static>> {
        Ok(())
    }
}

impl Insert for FifteenMinuteSpeedRangeCount {
    type Item = FifteenMinuteSpeedRangeCount;
    fn insert(_: FifteenMinuteSpeedRangeCount) -> Result<(), CountError<'static>> {
        Ok(())
    }
}

impl Insert for NonNormalVolCount {
    type Item = NonNormalVolCount;
    fn insert(_: NonNormalVolCount) -> Result<(), CountError<'static>> {
        Ok(())
    }
}
