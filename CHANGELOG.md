# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `Database` as a building block for connection management in application.
- `schema::TableSchema` for mapping binary row into actual column names
- `replication::BinaryRow` and `replication::BinaryRowIter` for making working with rows based binary events easier to work with
- `replication::Event` to make easier processing of similar table changes
- `mapper::ChangeLogMapper` to transform `replication::Event` into business domain change item
- `log::ItemChange` to store multiple domain model specific changes in an enum
- `aggregate::Aggregate` and `aggregate::AsyncAggregate` to process `log::ItemChange` in batches and product output of `aggregate::ChangeAggregate`
- `app::Application` to build easily your own changelog customized apps

[unreleased]: https://github.com/EcomDev/mage-os-database-changelog/compare/0a7c672...HEAD