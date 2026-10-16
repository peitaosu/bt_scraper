use anyhow::Context;
use bt_scraper::commands;
use bt_scraper::config::Config;
use bt_scraper::app;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "bt-scraper")]
#[command(about = "BitTorrent DHT indexer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Run {
        #[arg(short, long, default_value = "config/default.toml")]
        config: PathBuf,
    },
    Stats {
        #[arg(short, long, default_value = "config/default.toml")]
        config: PathBuf,
    },
    Reindex {
        #[arg(short, long, default_value = "config/default.toml")]
        config: PathBuf,
    },
    Backfill {
        #[arg(short, long, default_value = "config/default.toml")]
        config: PathBuf,
    },
    Export {
        #[arg(short, long, default_value = "config/default.toml")]
        config: PathBuf,
        #[arg(short, long)]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("bt_scraper=info".parse()?))
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Run { config } => {
            let cfg = Config::load(&config).context("load config")?;
            info!(config = %config.display(), "starting");
            app::run(cfg).await?;
        }
        Commands::Stats { config } => commands::stats(&config).await?,
        Commands::Reindex { config } => commands::reindex(&config).await?,
        Commands::Backfill { config } => commands::backfill(&config).await?,
        Commands::Export { config, output } => commands::export(&config, &output).await?,
    }
    Ok(())
}
