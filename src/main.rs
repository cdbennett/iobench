//! Disk I/O benchmark test. Measure the read performance of filesystem access.
//! Multithreaded with configurable concurrency.

use std::{env, io::Read, os::unix::fs::MetadataExt, path::PathBuf, time::Instant};

use clap::{Parser, Subcommand};
use jwalk::{
    rayon::{
        self,
        iter::{IntoParallelRefIterator, ParallelIterator},
    },
    DirEntry, WalkDir,
};
use tracing::{debug, trace};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter};

/// Disk I/O benchmark performance test
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand, Debug, Clone)]
enum CliCommand {
    /// Read a filesystem directory tree recursively.
    ReadTree {
        /// The directory to read instead of CWD.
        #[arg(short, long)]
        dir: Option<String>,
        /// Number of concurrent threads to use.
        #[arg(short = 'j', long, default_value_t = 16)]
        threads: u32,

        /// Filesystem paths to read (alternative to -d/--dir DIR)
        paths: Vec<String>,
    },
}

fn main() {
    init_logging();
    let options = Cli::parse();

    match options.command {
        CliCommand::ReadTree {
            dir,
            paths,
            threads,
        } => {
            let mut paths = paths.clone();
            if let Some(d) = dir {
                paths.push(d);
            }
            let mut paths = paths
                .into_iter()
                .map(|s| PathBuf::from(s))
                .collect::<Vec<PathBuf>>();
            if paths.is_empty() {
                paths.push(std::env::current_dir().unwrap());
            }
            read_tree(paths, threads);
        }
    }
}

fn read_tree(dirs: Vec<PathBuf>, threads: u32) {
    println!("-- reading {dirs:?} using {threads} threads");
    let t1 = Instant::now();
    let mut all_files = Vec::new();
    for dir in dirs {
        let files = WalkDir::new(dir)
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
            .collect::<Vec<_>>();
        all_files.extend(files);
    }
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
    let path = entry.path();

    match do_read_file(&entry, &mut stats) {
        Ok(()) => {
            trace!("done reading file {}", path.to_string_lossy());
        }
        Err(err) => {
            debug!("error reading file {}: {err}", path.to_string_lossy());
        }
    }

    stats
}

const BUF_SIZE: usize = 65536;

fn do_read_file(
    entry: &DirEntry<((), ())>,
    stats: &mut ReadFilesStats,
) -> Result<(), std::io::Error> {
    let path = entry.path();
    let pathstr = path.to_string_lossy();
    trace!("open file: {}", pathstr);
    let mut f = std::fs::File::open(&path)?;
    stats.file_count += 1;
    let size = entry.metadata()?.size();
    let mb = size as f64 / 1e6;
    trace!("begin reading file: {}", pathstr);
    let mut buf = [0; BUF_SIZE];
    loop {
        let n = f.read(&mut buf[..])?;
        if n == 0 {
            if stats.bytes != size {
                debug!("file must have been truncated, size was {size} but only read {} before getting empty read: {pathstr}", stats.bytes);
            }
            break;
        }
        stats.bytes += n as u64;
        trace!(
            "read chunk of {n} bytes ({:.1}% of {mb:.3} MB) from: {pathstr}",
            100.0 * stats.bytes as f64 / size as f64,
        );
        if stats.bytes > size {
            debug!("file must have been extended, size was {size} but we've read {}, stopping to avoid unbounded reading: {pathstr}", stats.bytes);
            break;
        }
    }

    Ok(())
}

const DEFAULT_LOGGING_DIRECTIVES: &str = "info,iobench=debug";

fn init_logging() {
    // Rust defaults to no backtraces on panic. Enable backtraces by default.
    set_env_var_default("RUST_BACKTRACE", "1");

    // Default logging level configuration if not set in the environment.
    set_env_var_default("RUST_LOG", DEFAULT_LOGGING_DIRECTIVES);

    tracing_subscriber::Registry::default()
        // Use the RUST_LOG environment variable to configure logging levels.
        // We disable 'ansi' so the color codes don't interfere with logs stored in files.
        .with(EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_thread_ids(true)
                .with_thread_names(true),
        )
        .init();
}

fn set_env_var_default(name: &str, value: &str) {
    if var_missing_or_blank(name) {
        env::set_var(name, value);
    }
}

fn var_missing_or_blank(name: &str) -> bool {
    env::var(name).unwrap_or("".to_string()).trim().is_empty()
}
