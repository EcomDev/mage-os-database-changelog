# Mage-OS Database Changelog Generator

Connects to MySQL as a replication service and aggregates updates into database tables into easy to use event aggregates like this:

```json
{"entity":"product","global":{"@created":[1,2,3,4]},"metadata":{"file":"d18ce2081821-bin.000020","position":1003,"timestamp":1684421292}}
{"entity":"product","global":{"has_options":[1,2,3,4]},"metadata":{"file":"d18ce2081821-bin.000020","position":2127,"timestamp":1684421408}}
{"entity":"product","global":{"has_options":[2,4],"type_id":[2,4]},"metadata":{"file":"d18ce2081821-bin.000020","position":2617,"timestamp":1684421448}}
```

Application has the following configuration file structure in both JSON and TOML formats:

* **database** Name of the database to limit number.
* **table_prefix** Table prefix for table name match in mapper.
* **connection** MySQL connection URL or list of options.
* **batch_size** Size of the changelog buffer. Defaults to `10000` rows.
* **batch_duration** Time in seconds after which aggregate is going to be printed if batch size does not come first. Defaults to `60` seconds. 

Here is an example of minimal configuration in JSON:
```json
{
  "database": "magento2",
  "connection": "mysql://root:root@127.0.0.1:3306"
}
```

## Usage examples

Each command requires config file with MySQL connection details 

### Binlog Position

Print current position of binlog in the database. Can be used to establish initial sync point for changelog to work from.

```database-changelog --config ./config.json position```

### Dump Binlog Since Position in JSON

Dumps current binlog since provided position in arguments as JSON event lines. It is good for human-readable inspection of the data.

`database-changelog --config ./config.json dump <FILE> <POSITION>`

### Dump Binlog Since Position in Binary 

Dumps current binlog since provided position in arguments as binary event stream (mspack). 
Works best for processing by another application as serialization and deserialization is much faster.

`database-changelog --config ./config.json dump <FILE> <POSITION>`


## Requirements
- MySQL/MariaDB with binary log in `ROW` format enabled
- User for connection via replication protocol with such permissions

    ```sql
    GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '[USER]'@'%';
    ```
    If your Magento user already has all privileges like this:
    ```
    mysql> SHOW GRANTS
    -> ;
    +-------------------------------------------------------------------------------------------------------------------------------+
    | Grants for [USER]@%                                                                                                              |
    +-------------------------------------------------------------------------------------------------------------------------------+
    | GRANT ALL PRIVILEGES ON *.* TO '[USER]'@'%' IDENTIFIED BY PASSWORD '[YOUR_PASS]' WITH GRANT OPTION |
    +-------------------------------------------------------------------------------------------------------------------------------+
    1 row in set (0.00 sec)

    ```
    You do not need any changes, although it is a good practice to create custom replication user.

## Dev Build

To build own binary you should have [Rust toolchain](https://www.rust-lang.org/learn/get-started). 
At the moment project relies on unstable feature in nightly rust that is going to be stabilized this July, so in the mean time you have to use nightly build of rust.

To get a working binary just run the following:
```bash
cargo build --release
cp target/release/database-changelog ./to/your/path/
```

``````


## Compatibility
- MySQL 5.7+, 8.0+ (or MariaDB equivalent)
- Mage-OS 2.4 and later
- Magento 2.4 and later

## Roadmap
- Compatibility with Adobe Commerce Content Staging
- Compatibility with Magento 1 / OpenMage