mod change_log;
mod field_update;
mod product_change;
mod sender;

pub use crate::mapper::ChangeLogMapper;
pub use change_log::ItemChange;
pub use product_change::ProductChange;
pub use sender::ChangeLogSender;

pub use field_update::FieldUpdate;

pub trait IntoChangeLog<T> {
    fn into_change_log(self) -> Option<T>;
}
