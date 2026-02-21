#![forbid(unsafe_code)]

use std::env;
use std::path::PathBuf;

use rafka_codegen::{
    generate_message_structs_source, generate_registry_source, load_message_specs,
    write_registry_file,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("rafka-codegen failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = None::<PathBuf>;
    let mut registry_output = None::<PathBuf>;
    let mut messages_output = None::<PathBuf>;
    let mut targets = vec![
        "ApiVersionsRequest".to_string(),
        "ApiVersionsResponse".to_string(),
        "ProduceRequest".to_string(),
        "OffsetCommitRequest".to_string(),
        "OffsetCommitResponse".to_string(),
        "OffsetFetchRequest".to_string(),
        "OffsetFetchResponse".to_string(),
        "JoinGroupRequest".to_string(),
        "JoinGroupResponse".to_string(),
        "SyncGroupRequest".to_string(),
        "SyncGroupResponse".to_string(),
        "HeartbeatRequest".to_string(),
        "HeartbeatResponse".to_string(),
        "LeaveGroupRequest".to_string(),
        "LeaveGroupResponse".to_string(),
        "SaslHandshakeRequest".to_string(),
        "SaslHandshakeResponse".to_string(),
        "SaslAuthenticateRequest".to_string(),
        "SaslAuthenticateResponse".to_string(),
    ];

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => {
                let value = args.next().ok_or("missing value for --input")?;
                input = Some(PathBuf::from(value));
            }
            "--output" => {
                let value = args.next().ok_or("missing value for --output")?;
                registry_output = Some(PathBuf::from(value));
            }
            "--messages-output" => {
                let value = args.next().ok_or("missing value for --messages-output")?;
                messages_output = Some(PathBuf::from(value));
            }
            "--targets" => {
                let value = args.next().ok_or("missing value for --targets")?;
                let parsed = value
                    .split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>();
                if parsed.is_empty() {
                    return Err("empty --targets list".into());
                }
                targets = parsed;
            }
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}").into()),
        }
    }

    let input = input.ok_or("missing required --input")?;
    if registry_output.is_none() && messages_output.is_none() {
        return Err("at least one output must be provided (--output or --messages-output)".into());
    }
    let specs = load_message_specs(&input)?;
    if let Some(output) = registry_output {
        let source = generate_registry_source(&specs);
        write_registry_file(&output, &source)?;
        println!(
            "generated protocol registry: {} specs -> {}",
            specs.len(),
            output.display()
        );
    }
    if let Some(output) = messages_output {
        let refs = targets.iter().map(String::as_str).collect::<Vec<_>>();
        let source = generate_message_structs_source(&specs, &refs)?;
        write_registry_file(&output, &source)?;
        println!(
            "generated protocol message structs: {} targets -> {}",
            refs.len(),
            output.display()
        );
    }
    Ok(())
}

fn print_help() {
    println!("Usage: rafka-codegen --input <message-spec-dir> [--output <generated-registry-file>] [--messages-output <generated-messages-file>] [--targets <comma-separated-message-names>]");
}
