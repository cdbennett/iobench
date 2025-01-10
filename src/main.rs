use std::{path::PathBuf, time::Instant};

use clap::{Parser, Subcommand};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand, Debug, Clone)]
enum CliCommand {
    ReadTree {
        /// The directory to read instead of CWD.
        #[arg(short, long)]
        dir: Option<String>,
        /// Number of concurrent threads to use.
        #[arg(short = 'j', long, default_value_t = 16)]
        threads: u32,
    },
}

fn main() {
    let options = Cli::parse();

    match options.command {
        CliCommand::ReadTree { dir, threads } => read_tree(
            dir.map(|s| PathBuf::from(&s))
                .unwrap_or_else(|| std::env::current_dir().unwrap()),
            threads,
        ),
    }
}

fn read_tree(dir: PathBuf, threads: u32) {
    println!("-- reading {dir:?} using {threads} threads");
    let t1 = Instant::now();
    let all_files: Vec<_> = walkdir::WalkDir::new(dir)
        .sort_by_file_name()
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.unwrap();
            if !entry.file_type().is_dir() {
                Some(entry)
            } else {
                None
            }
        })
        .collect();
    let t2 = Instant::now();
    let dur_s = (t2 - t1).as_secs_f64();
    println!(
        "-- list: {:.0} files/s  ({} files in {} s)",
        all_files.len() as f64 / dur_s,
        all_files.len(),
        dur_s,
    );

    let chunk_size = all_files.len() / threads as usize;
    let chunks: Vec<_> = all_files.chunks(chunk_size).collect();

    let t1 = Instant::now();
    let mut all_stats = ReadFilesStats::default();
    std::thread::scope(|scope| {
        let results: Vec<_> = chunks
            .iter()
            .map(|chunk| scope.spawn(|| read_files(chunk)))
            .collect();

        for h in results {
            let r = h.join();
            let stats = r.expect("return value from thread");
            all_stats.update(&stats);
        }
    });
    let t2 = Instant::now();
    let dur_s = (t2 - t1).as_secs_f64();
    let total_size_mb = all_stats.bytes as f64 / 1_000_000.0;
    println!(
        "-- read: {:.0} MB/s   {:.0} files/s  ({} MB in {} s)",
        total_size_mb / dur_s,
        all_files.len() as f64 / dur_s,
        total_size_mb,
        dur_s,
    );
}

#[derive(Default)]
struct ReadFilesStats {
    bytes: u64,
    file_count: u64,
}

impl ReadFilesStats {
    fn update(&mut self, other: &ReadFilesStats) {
        self.bytes += other.bytes;
        self.file_count += other.file_count;
    }
}

fn read_files(chunk: &[walkdir::DirEntry]) -> ReadFilesStats {
    let mut stats = ReadFilesStats::default();
    for file in chunk {
        let path = file.path();
        match std::fs::read(path) {
            Ok(content) => {
                stats.bytes += content.len() as u64;
                stats.file_count += 1;
            }
            Err(err) => {
                eprintln!("note: failed to read file {path:?}: {err}");
            }
        }
    }

    stats
}
