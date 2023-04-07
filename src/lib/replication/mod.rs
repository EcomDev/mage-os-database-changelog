use error::ReplicationError;
mod error;
mod reader;

pub use reader::ReplicationReader;

pub trait ReplicationObserver {
    fn update_row(&mut self);
    fn write_row(&mut self);
    fn delete_row(&mut self);
    fn update_partial_row(&mut self);
}