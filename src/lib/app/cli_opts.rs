use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use crate::app::{ApplicationCommand, ApplicationConfig, ApplicationOutput};
use crate::replication::BinlogPosition;
use clap::builder::Str;
use clap::{arg, command, value_parser, ArgAction, Command};
use mysql_common::frunk::labelled::IntoLabelledGeneric;

fn parse_output_format(value: &str) -> Result<ApplicationOutput, String> {
    match value {
        "json" => Ok(ApplicationOutput::Json),
        "binary" => Ok(ApplicationOutput::Binary),
        _ => Err(format!(
            "Unknown format {value}, only json and binary is allowed"
        )),
    }
}

fn parse_config_file_location(value: &str) -> Result<ApplicationConfig, String> {
    let path: PathBuf = match value.parse() {
        Ok(path) => path,
        _ => return Err(format!("Cannot parse {value} as file path")),
    };

    if !path.exists() || !path.is_file() {
        return return Err(format!("Configuration file {value} does not exists"));
    }
    let extension_error = format!("Configuration {value} file does not have extension");

    let extension = path
        .extension()
        .ok_or(extension_error.clone())?
        .to_str()
        .ok_or(extension_error)?;

    match extension {
        "ini" | "toml" | "cnf" => {
            let mut value = String::new();

            File::open(path)
                .map_err(|e| e.to_string())?
                .read_to_string(&mut value)
                .map_err(|e| e.to_string())?;

            toml::from_str(&value).map_err(|e| e.to_string())
        }
        "json" => serde_json::from_reader(File::open(path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string()),
        _ => Err(format!("Configuration file {value} is not supported")),
    }
}

fn cli_command() -> Command {
    command!()
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(arg!(--"config" <CONFIG>).value_parser(parse_config_file_location))
        .subcommand(Command::new("position").about("Current binary log position"))
        .subcommand(
            Command::new("dump")
                .about("Dumps changelog since specified position and exists")
                .arg(
                    arg!(--"output" <FORMAT>)
                        .required(false)
                        .value_parser(parse_output_format),
                )
                .arg(arg!(<FILE>).value_parser(value_parser!(String)))
                .arg(arg!(<POSITION>).value_parser(value_parser!(u32))),
        )
        .subcommand(
            Command::new("watch")
                .about("Dumps changelog since specified position and waits for new events")
                .arg(
                    arg!(--"output" <FORMAT>)
                        .required(false)
                        .value_parser(parse_output_format),
                )
                .arg(arg!(<FILE>).value_parser(value_parser!(String)))
                .arg(arg!(<POSITION>).value_parser(value_parser!(u32))),
        )
}

pub fn command_from_cli() -> ApplicationCommand {
    let command = cli_command().get_matches();

    let configuration = command
        .get_one::<ApplicationConfig>("config")
        .unwrap()
        .clone();

    match command.subcommand().unwrap() {
        ("position", _) => ApplicationCommand::Position(configuration),
        ("dump", args) => ApplicationCommand::Dump(
            configuration,
            args.get_one::<ApplicationOutput>("output")
                .copied()
                .unwrap_or(ApplicationOutput::Json),
            BinlogPosition::new(
                args.get_one::<String>("FILE").unwrap().as_str(),
                *args.get_one::<u32>("POSITION").unwrap(),
            ),
        ),
        ("watch", args) => ApplicationCommand::Watch(
            configuration,
            args.get_one::<ApplicationOutput>("output")
                .copied()
                .unwrap_or(ApplicationOutput::Json),
            BinlogPosition::new(
                args.get_one::<String>("FILE").unwrap().as_str(),
                *args.get_one::<u32>("POSITION").unwrap(),
            ),
        ),
        _ => unreachable!(),
    }
}
