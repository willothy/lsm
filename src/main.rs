use clap::Parser;

#[derive(Debug, Clone, clap::Parser)]
pub struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum CliCommand {
    Get {
        key: String,
    },

    Put {
        #[arg(requires = "value_input")]
        key: String,
        #[clap(group = "value_input")]
        value: Option<String>,
        #[arg(long, group = "value_input")]
        stdin: bool,
    },

    #[command(name = "del")]
    Delete {
        key: String,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let mut db = mintdb::Database::open("example_wal".into())?;

    match args.command {
        CliCommand::Get { key } => {
            println!("{:?}", db.get(&key.into()));
        }
        CliCommand::Put { key, value, stdin } => {
            db.put(
                key,
                value.unwrap_or_else(|| {
                    if stdin {
                        let mut buffer = String::new();
                        std::io::stdin().read_line(&mut buffer).unwrap();
                        buffer.trim_end().to_string()
                    } else {
                        panic!("Value must be provided either as an argument or via stdin");
                    }
                }),
            )?;
        }
        CliCommand::Delete { key } => {
            db.delete(key)?;
        }
    }

    let r = db.debug_replay_wal()?;
    println!("{:?} ({})", r, r.len());

    Ok(())
}
