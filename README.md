# Mage-OS Database Changelog Generator

Connects to MySQL as a replication service and aggregates updates into database tables into easy to use event aggregates like this:

```json
{
  "entity": "product",
  "updates": {
    "name": [1, 2, 3, 5],
    "status": [1, 2, 3],
    "sku": [1],
    "category_ids": [2, 4]
  }
}
```

Then module in the ecommerce store is going to update 

## Requirements
- MySQL/MariaDB with binary log in `ROW` format enabled
- User for connection via replication protocol with such permissions

    ```sql
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '[USER]'@'%';
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


## Compatibility
- MySQL 5.7+, 8.0+ (or MariaDB equivalent)
- Mage-OS 2.4 and later
- Magento 2.4 and later

## Roadmap
- Compatibility with Adobe Commerce Content Staging
- Compatibility with Magento 1 / OpenMage