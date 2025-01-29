use std::{path::PathBuf, time::Instant};

use clap::{Parser, Subcommand};
use jwalk::{
    rayon::{
        self,
        iter::{IntoParallelRefIterator, ParallelIterator},
    },
    DirEntry, WalkDir,
};

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
    let all_files: Vec<_> = WalkDir::new(dir)
        .parallelism(jwalk::Parallelism::RayonNewPool(threads as usize))
        .skip_hidden(false)
        .sort(true)
        // .process_read_dir(|depth, path, read_dir_state, children| {
        //     children.retain(|dir_entry_result| {
        //         dir_entry_result.as_ref().map(|dir_entry| dir_entry.file_type.is_file()).unwrap_or(false)
        //     })
        // })
        .into_iter()
        .filter_map(|result| result.ok().filter(|entry| entry.file_type.is_file()))
        .collect();
    let t2 = Instant::now();
    let dur_s = (t2 - t1).as_secs_f64();
    println!(
        "-- list: {:.0} files/s  ({} files in {} s)",
        all_files.len() as f64 / dur_s,
        all_files.len(),
        dur_s,
    );

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads as usize)
        .build()
        .expect("thread pool");

    let t1 = Instant::now();

    let all_stats = pool.install(|| {
        all_files
            .par_iter()
            .map(read_file)
            .reduce(ReadFilesStats::default, |a, b| a.combine(&b))
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
    fn combine(&self, other: &Self) -> Self {
        Self {
            bytes: self.bytes + other.bytes,
            file_count: self.file_count + other.file_count,
        }
    }
}

fn read_file(entry: &DirEntry<((), ())>) -> ReadFilesStats {
    let mut stats = ReadFilesStats::default();
    let path = &entry.path();
    match std::fs::read(path) {
        Ok(content) => {
            stats.bytes += content.len() as u64;
            stats.file_count += 1;
        }
        Err(err) => {
            eprintln!("note: failed to read file {path:?}: {err}");
        }
    }

    stats
}
