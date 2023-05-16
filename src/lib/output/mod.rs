mod json;
mod msgpack;

use crate::aggregate::ChangeAggregate;
use crate::error::Error;
pub use json::*;
pub use msgpack::*;

use tokio::io::AsyncWrite;

pub trait Output {
    async fn write<T: AsyncWrite + Unpin>(
        &self,
        writer: &mut T,
        aggregate: ChangeAggregate,
    ) -> Result<(), Error>;
}
