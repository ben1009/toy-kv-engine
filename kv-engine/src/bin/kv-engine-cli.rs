mod wrapper;

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use kv_engine_wrapper::{
    FutureResultExt,
    compact::{
        CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
        TieredCompactionOptions,
    },
    iterators::StorageIterator,
    lsm_storage::{KvEngine, LsmStorageOptions, PrefixBloomOptions},
};
use rustyline::DefaultEditor;
use wrapper::kv_engine_wrapper;

#[derive(Debug, Clone, ValueEnum)]
enum CompactionStrategy {
    Simple,
    Leveled,
    Tiered,
    None,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "lsm.db")]
    path: PathBuf,
    #[arg(long, default_value = "leveled")]
    compaction: CompactionStrategy,
    #[arg(long)]
    enable_wal: bool,
    #[arg(long)]
    serializable: bool,
    #[arg(long, default_value = "1024")]
    block_cache_capacity: u64,
}

struct ReplHandler {
    epoch: u64,
    lsm: Arc<KvEngine>,
}

impl ReplHandler {
    fn handle(&mut self, command: &Command) -> Result<()> {
        match command {
            Command::Fill { begin, end } => {
                for i in *begin..=*end {
                    self.lsm
                        .put(
                            format!("{}", i).as_bytes(),
                            format!("value{}@{}", i, self.epoch).as_bytes(),
                        )
                        .into_result()?;
                }

                println!(
                    "{} values filled with epoch {}",
                    end - begin + 1,
                    self.epoch
                );
            }
            Command::Del { key } => {
                self.lsm.delete(key.as_bytes()).into_result()?;
                println!("{} deleted", key);
            }
            Command::Delrange { start, end } => {
                if start >= end {
                    return Err(anyhow::anyhow!(
                        "invalid range: start must be less than end"
                    ));
                }
                self.lsm
                    .delete_range(start.as_bytes(), end.as_bytes())
                    .into_result()?;
                println!("range deleted: [{}, {})", start, end);
            }
            Command::Get { key } => {
                if let Some(value) = self.lsm.get(key.as_bytes()).into_result()? {
                    println!("{}={:?}", key, value);
                } else {
                    println!("{} not exist", key);
                }
            }
            Command::Scan { begin, end } => match (begin, end) {
                (None, None) => {
                    let mut iter = self
                        .lsm
                        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
                        .into_result()?;
                    let mut cnt = 0;
                    while iter.is_valid() {
                        println!(
                            "{:?}={:?}",
                            Bytes::copy_from_slice(iter.key()),
                            Bytes::copy_from_slice(iter.value()),
                        );
                        iter.next()?;
                        cnt += 1;
                    }
                    println!();
                    println!("{} keys scanned", cnt);
                }
                (Some(begin), Some(end)) => {
                    let mut iter = self
                        .lsm
                        .scan(
                            std::ops::Bound::Included(begin.as_bytes()),
                            std::ops::Bound::Included(end.as_bytes()),
                        )
                        .into_result()?;
                    let mut cnt = 0;
                    while iter.is_valid() {
                        println!(
                            "{:?}={:?}",
                            Bytes::copy_from_slice(iter.key()),
                            Bytes::copy_from_slice(iter.value()),
                        );
                        iter.next()?;
                        cnt += 1;
                    }
                    println!();
                    println!("{} keys scanned", cnt);
                }
                _ => {
                    println!("invalid command");
                }
            },
            Command::Prefix { prefix } => {
                let mut iter = self.lsm.prefix_scan(prefix.as_bytes()).into_result()?;
                let mut cnt = 0;
                while iter.is_valid() {
                    println!(
                        "{:?}={:?}",
                        Bytes::copy_from_slice(iter.key()),
                        Bytes::copy_from_slice(iter.value()),
                    );
                    iter.next()?;
                    cnt += 1;
                }
                println!();
                println!("{} keys scanned", cnt);
            }
            Command::Dump => {
                self.lsm.dump_structure();
                println!("dump success");
            }
            Command::Flush => {
                self.lsm.force_flush().into_result()?;
                println!("flush success");
            }
            Command::FullCompaction => {
                self.lsm.force_full_compaction().into_result()?;
                println!("full compaction success");
            }
            Command::Quit | Command::Close => {
                self.lsm.close().into_result()?;
                std::process::exit(0);
            }
            Command::Stats => {
                let cs = self.lsm.cache_stats();
                println!("Block cache:");
                println!("  entries:    {}", cs.block_cache_entry_count);
                if cs.value_cache_hit_count > 0 || cs.value_cache_miss_count > 0 {
                    println!("Value cache:");
                    println!("  hits:       {}", cs.value_cache_hit_count);
                    println!("  misses:     {}", cs.value_cache_miss_count);
                    let vc_total = cs.value_cache_hit_count + cs.value_cache_miss_count;
                    println!(
                        "  hit rate:   {:.1}%",
                        cs.value_cache_hit_count as f64 / vc_total as f64 * 100.0
                    );
                } else {
                    println!("Value cache: disabled or no activity");
                }
            }
            Command::RtStats => {
                let rs = self.lsm.range_tombstone_stats();
                println!("Range tombstones:");
                println!("  active:          {}", rs.active_count);
                println!("  immutable:       {}", rs.immutable_count);
                println!("  SSTs:            {}", rs.sst_count);
                println!("  fragments:       {}", rs.total_sst_fragment_count);
                println!("  metadata bytes:  {}", rs.metadata_bytes);
                println!("  covering lookups: {}", rs.covering_lookups);
                println!("  covering hits:    {}", rs.covering_hits);
                println!("  covered drops:    {}", rs.covered_point_drops);
                println!("  tombstone drops:  {}", rs.tombstone_drops);
            }
        };

        self.epoch += 1;

        Ok(())
    }
}

#[derive(Debug)]
enum Command {
    Fill {
        begin: u64,
        end: u64,
    },
    Del {
        key: String,
    },
    Delrange {
        start: String,
        end: String,
    },
    Get {
        key: String,
    },
    Scan {
        begin: Option<String>,
        end: Option<String>,
    },
    Prefix {
        prefix: String,
    },

    Dump,
    Flush,
    FullCompaction,
    Quit,
    Close,
    Stats,
    RtStats,
}

impl Command {
    pub fn parse(input: &str) -> Result<Self> {
        use nom::{
            branch::*, bytes::complete::*, character::complete::*, combinator::*, sequence::*,
        };

        let uint = |i| {
            map_res(digit1::<&str, nom::error::Error<_>>, |s: &str| {
                s.parse()
                    .map_err(|_| nom::error::Error::new(s, nom::error::ErrorKind::Digit))
            })(i)
        };

        let string = |i| {
            map(take_till1(|c: char| c.is_whitespace()), |s: &str| {
                s.to_string()
            })(i)
        };

        let fill = |i| {
            map(
                tuple((tag_no_case("fill"), space1, uint, space1, uint)),
                |(_, _, key, _, value)| Command::Fill {
                    begin: key,
                    end: value,
                },
            )(i)
        };

        let del = |i| {
            map(
                tuple((tag_no_case("del"), space1, string)),
                |(_, _, key)| Command::Del { key },
            )(i)
        };

        let delrange = |i| {
            map(
                tuple((tag_no_case("delrange"), space1, string, space1, string)),
                |(_, _, start, _, end)| Command::Delrange { start, end },
            )(i)
        };

        let get = |i| {
            map(
                tuple((tag_no_case("get"), space1, string)),
                |(_, _, key)| Command::Get { key },
            )(i)
        };

        let scan = |i| {
            map(
                tuple((
                    tag_no_case("scan"),
                    opt(tuple((space1, string, space1, string))),
                )),
                |(_, opt_args)| {
                    let (begin, end) = opt_args
                        .map_or((None, None), |(_, begin, _, end)| (Some(begin), Some(end)));
                    Command::Scan { begin, end }
                },
            )(i)
        };

        let prefix = |i| {
            map(
                tuple((tag_no_case("prefix"), space1, string)),
                |(_, _, prefix)| Command::Prefix { prefix },
            )(i)
        };

        let command = |i| {
            alt((
                fill,
                delrange,
                del,
                get,
                scan,
                prefix,
                map(tag_no_case("dump"), |_| Command::Dump),
                map(tag_no_case("flush"), |_| Command::Flush),
                map(tag_no_case("full_compaction"), |_| Command::FullCompaction),
                map(tag_no_case("quit"), |_| Command::Quit),
                map(tag_no_case("close"), |_| Command::Close),
                map(tag_no_case("stats"), |_| Command::Stats),
                map(tag_no_case("rt_stats"), |_| Command::RtStats),
            ))(i)
        };

        command(input)
            .map(|(_, c)| c)
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

struct Repl {
    app_name: String,
    description: String,
    prompt: String,

    handler: ReplHandler,

    editor: DefaultEditor,
}

impl Repl {
    pub fn run(mut self) -> Result<()> {
        self.bootstrap()?;

        loop {
            let readline = self.editor.readline(&self.prompt)?;
            if readline.trim().is_empty() {
                // Skip noop
                continue;
            }
            let command = Command::parse(&readline)?;
            self.handler.handle(&command)?;
            self.editor.add_history_entry(readline)?;
        }
    }

    fn bootstrap(&mut self) -> Result<()> {
        println!("Welcome to {}!", self.app_name);
        println!("{}", self.description);
        println!();
        Ok(())
    }
}

struct ReplBuilder {
    app_name: String,
    description: String,
    prompt: String,
}

impl ReplBuilder {
    pub fn new() -> Self {
        Self {
            app_name: "kv-engine-cli".to_string(),
            description: "A CLI for kv-engine".to_string(),
            prompt: "kv-engine-cli> ".to_string(),
        }
    }

    pub fn app_name(mut self, app_name: &str) -> Self {
        self.app_name = app_name.to_string();
        self
    }

    pub fn description(mut self, description: &str) -> Self {
        self.description = description.to_string();
        self
    }

    pub fn prompt(mut self, prompt: &str) -> Self {
        self.prompt = prompt.to_string();
        self
    }

    pub fn build(self, handler: ReplHandler) -> Result<Repl> {
        Ok(Repl {
            app_name: self.app_name,
            description: self.description,
            prompt: self.prompt,
            editor: DefaultEditor::new()?,
            handler,
        })
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let lsm = KvEngine::open(
        args.path,
        LsmStorageOptions {
            block_size: 4096,
            target_sst_size: 2 << 20, // 2MB
            num_memtable_limit: 3,
            prefix_bloom: PrefixBloomOptions::default(),
            compaction_options: match args.compaction {
                CompactionStrategy::None => CompactionOptions::NoCompaction,
                CompactionStrategy::Simple => {
                    CompactionOptions::Simple(SimpleLeveledCompactionOptions {
                        size_ratio_percent: 200,
                        level0_file_num_compaction_trigger: 2,
                        max_levels: 4,
                    })
                }
                CompactionStrategy::Tiered => CompactionOptions::Tiered(TieredCompactionOptions {
                    num_tiers: 3,
                    max_size_amplification_percent: 200,
                    size_ratio: 1,
                    min_merge_width: 2,
                    max_merge_width: None,
                }),
                CompactionStrategy::Leveled => {
                    CompactionOptions::Leveled(LeveledCompactionOptions {
                        level0_file_num_compaction_trigger: 2,
                        max_levels: 4,
                        base_level_size_mb: 128,
                        level_size_multiplier: 2,
                    })
                }
            },
            enable_wal: args.enable_wal,
            serializable: args.serializable,
            value_separation: None,
            manifest_snapshot_threshold_bytes: 4 << 20, // 4MB
            block_cache_capacity: args.block_cache_capacity,
            enable_cache_backfill: true,
        },
    )
    .into_result()?;

    let repl = ReplBuilder::new()
        .app_name("kv-engine-cli")
        .description("A CLI for kv-engine")
        .prompt("kv-engine-cli> ")
        .build(ReplHandler { epoch: 0, lsm })?;

    repl.run()?;
    Ok(())
}
