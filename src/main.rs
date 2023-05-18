use mage_os_database_changelog::app::{command_from_cli, Application};
use mage_os_database_changelog::error::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    Application::new().run(command_from_cli()).await
}
