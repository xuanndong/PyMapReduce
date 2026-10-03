use clap::{Parser, Subcommand};
use mapreduce::cluster::head::HeadNode;
use mapreduce::fault::wal::WriteAheadLog;
use mapreduce::gcs::store::Gcs;
use mapreduce::scheduler::dispatcher::Dispatcher;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "mapreduce", about = "MapReduce Distributed Framework (Rust Core Engine)")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a HeadNode
    Serve {
        #[arg(long, default_value_t = 7777)]
        port: u16,

        #[arg(long, value_enum, default_value_t = mapreduce::scheduler::strategy::SchedulerType::RoundRobin)]
        strategy: mapreduce::scheduler::strategy::SchedulerType,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve { port, strategy } => {
            let addr = format!("0.0.0.0:{}", port);
            println!("Starting HeadNode on {}", addr);
            println!("Using Scheduler Strategy: {:?}", strategy);

            let gcs = Arc::new(Gcs::new());
            let strategy_impl = strategy.into_strategy();
            let dispatcher = Arc::new(Dispatcher::new(strategy_impl));
            let wal = WriteAheadLog::new(PathBuf::from("/tmp/pymapreduce/wal")).await?;

            let head = HeadNode::new(gcs, dispatcher, wal, addr);
            head.run().await?;
        }
    }

    Ok(())
}

