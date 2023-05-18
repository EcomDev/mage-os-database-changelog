use crate::aggregate::ChangeAggregate;
use crate::error::Error;
use crate::output::{JsonOutput, MessagePack, Output};
use tokio::io::AsyncWrite;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum ApplicationOutput {
    Json,
    Binary,
}

impl Default for ApplicationOutput {
    fn default() -> Self {
        ApplicationOutput::Json
    }
}

impl Output for ApplicationOutput {
    async fn write<T: AsyncWrite + Unpin>(
        &self,
        writer: &mut T,
        aggregate: ChangeAggregate,
    ) -> Result<(), Error> {
        match self {
            Self::Json => JsonOutput.write(writer, aggregate).await,
            Self::Binary => MessagePack.write(writer, aggregate).await,
        }
    }
}
