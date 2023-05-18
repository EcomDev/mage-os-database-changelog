
use crate::database::Database;

use mysql_async::{Opts, OptsBuilder};
use serde::de::{Error, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt::Formatter;

#[derive(PartialEq, Debug, Clone)]
pub struct ConnectionOpts(Opts);

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct ApplicationConfig {
    database: String,
    #[serde(default)]
    table_prefix: String,
    #[serde(default = "ApplicationConfig::default_batch_size")]
    batch_size: usize,
    connection: ConnectionOpts,
}

impl ApplicationConfig {
    fn default_batch_size() -> usize {
        10000
    }

    pub fn new(database: impl Into<String>, connection: impl Into<ConnectionOpts>) -> Self {
        Self {
            database: database.into(),
            table_prefix: Default::default(),
            batch_size: Self::default_batch_size(),
            connection: connection.into(),
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn create_database(&self) -> Database {
        Database::new(self.connection.0.clone())
    }

    pub fn database(&self) -> &str {
        self.database.as_str()
    }

    pub fn table_prefix(&self) -> &str {
        self.table_prefix.as_str()
    }
}

impl<'de> Deserialize<'de> for ConnectionOpts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ConnectionVisitor;

        enum ConnectionKey {
            Socket,
            User,
            Pass,
            Host,
            Port,
            StmtCacheSize,
            MaxAllowedPacket,
        }

        impl<'de> Deserialize<'de> for ConnectionKey {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct ConnectionKeyVisitor;
                impl<'de> Visitor<'de> for ConnectionKeyVisitor {
                    type Value = ConnectionKey;

                    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                        formatter.write_str("expecting a valid string key for connection")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: Error,
                    {
                        Ok(match value {
                            "socket" => ConnectionKey::Socket,
                            "user" => ConnectionKey::User,
                            "pass" => ConnectionKey::Pass,
                            "host" => ConnectionKey::Host,
                            "port" => ConnectionKey::Port,
                            "stmt_cache_size" => ConnectionKey::StmtCacheSize,
                            "max_allowed_packet" => ConnectionKey::MaxAllowedPacket,
                            other => {
                                return Err(E::custom(format!(
                                    "unknown connection key \"{other}\""
                                )))
                            }
                        })
                    }
                }

                deserializer.deserialize_identifier(ConnectionKeyVisitor)
            }
        }

        impl<'de> Visitor<'de> for ConnectionVisitor {
            type Value = ConnectionOpts;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("a valid URL or connection options for MySQL")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                match Opts::from_url(v) {
                    Ok(opts) => Ok(ConnectionOpts(opts)),
                    Err(err) => Err(E::custom(err.to_string())),
                }
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: Error,
            {
                match Opts::from_url(&v) {
                    Ok(opts) => Ok(ConnectionOpts(opts)),
                    Err(err) => Err(E::custom(err.to_string())),
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut builder = OptsBuilder::default();

                while let Some(key) = map.next_key::<ConnectionKey>()? {
                    builder = match key {
                        ConnectionKey::Socket => builder.socket(Some(map.next_value::<String>()?)),
                        ConnectionKey::User => builder.user(Some(map.next_value::<String>()?)),
                        ConnectionKey::Pass => builder.pass(Some(map.next_value::<String>()?)),
                        ConnectionKey::Host => builder.ip_or_hostname(map.next_value::<String>()?),
                        ConnectionKey::Port => builder.tcp_port(map.next_value()?),
                        ConnectionKey::StmtCacheSize => {
                            builder.stmt_cache_size(Some(map.next_value()?))
                        }
                        ConnectionKey::MaxAllowedPacket => {
                            builder.max_allowed_packet(Some(map.next_value()?))
                        }
                    }
                }

                Ok(ConnectionOpts(builder.into()))
            }
        }

        deserializer.deserialize_any(ConnectionVisitor)
    }
}

impl From<Opts> for ConnectionOpts {
    fn from(value: Opts) -> Self {
        ConnectionOpts(value)
    }
}

impl From<OptsBuilder> for ConnectionOpts {
    fn from(value: OptsBuilder) -> Self {
        ConnectionOpts(value.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::OptsBuilder;

    #[test]
    fn creates_configuration_from_toml_with_url_database_connection() {
        let config: ApplicationConfig = toml::from_str(
            r#"
            connection = "mysql://root:root@localhost/"
            database = "magento"
            "#,
        )
        .unwrap();

        assert_eq!(
            config,
            ApplicationConfig::new(
                "magento",
                Opts::from_url("mysql://root:root@localhost/").unwrap()
            )
        )
    }

    #[test]
    fn creates_configuration_from_toml_with_connection_options_for_socket() {
        let config: ApplicationConfig = toml::from_str(
            r#"
            database = "magento"
            
            [connection]
            socket = "/var/mysql.sock"
            user = "root"
            pass = "root"
            "#,
        )
        .unwrap();

        assert_eq!(
            config,
            ApplicationConfig::new(
                "magento",
                OptsBuilder::default()
                    .socket(Some("/var/mysql.sock"))
                    .user(Some("root"))
                    .pass(Some("root"))
            )
        )
    }

    #[test]
    fn creates_configuration_from_toml_with_tcp_connection_and_common_options() {
        let config: ApplicationConfig = toml::from_str(
            r#"
            database = "magento"
            
            [connection]
            host = "localhost"
            port = 9090
            stmt_cache_size = 100
            max_allowed_packet = 100
            "#,
        )
        .unwrap();

        assert_eq!(
            config,
            ApplicationConfig::new(
                "magento",
                OptsBuilder::default()
                    .ip_or_hostname("localhost")
                    .tcp_port(9090)
                    .stmt_cache_size(100)
                    .max_allowed_packet(Some(100))
            )
        )
    }
}
