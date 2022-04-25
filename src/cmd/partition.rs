use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::Path;

use csv;
use regex::Regex;

use config::{Config, Delimiter};
use select::SelectColumns;
use util::{self, FilenameTemplate};
use CliResult;

static USAGE: &'static str = "
Partitions the given CSV data into chunks based on the value of a column

The files are written to the output directory with filenames based on the
values in the partition column and the `--filename` flag.

Usage:
    xsv partition [options] <column> <outdir> [<input>]
    xsv partition --help

partition options:
    --filename <filename>  A filename template to use when constructing
                           the names of the output files.  The string '{}'
                           will be replaced by a value based on the value
                           of the field, but sanitized for shell safety.
                           [default: {}.csv]
    -p, --prefix-length <n>  Truncate the partition column after the
                           specified number of bytes when creating the
                           output file.
    --drop                 Drop the partition column from results.
    --max-open-files <n>   Maximum number of files to keep open.
                           [default: 512]

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. Otherwise, the first row will
                           appear in all chunks as the header row.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Clone, Deserialize)]
struct Args {
    arg_column: SelectColumns,
    arg_input: Option<String>,
    arg_outdir: String,
    flag_max_open_files: usize,
    flag_filename: FilenameTemplate,
    flag_prefix_length: Option<usize>,
    flag_drop: bool,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    fs::create_dir_all(&args.arg_outdir)?;

    // It would be nice to support efficient parallel partitions, but doing
    // do would involve more complicated inter-thread communication, with
    // multiple readers and writers, and some way of passing buffers
    // between them.
    args.sequential_partition()
}

impl Args {
    /// Configuration for our reader.
    fn rconfig(&self) -> Config {
        Config::new(&self.arg_input)
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
            .select(self.arg_column.clone())
    }

    /// Get the column to use as a key.
    fn key_column(&self, rconfig: &Config, headers: &csv::ByteRecord) -> CliResult<usize> {
        let select_cols = rconfig.selection(headers)?;
        if select_cols.len() == 1 {
            Ok(select_cols[0])
        } else {
            fail!("can only partition on one column")
        }
    }

    /// A basic sequential partition.
    fn sequential_partition(&self) -> CliResult<()> {
        let rconfig = self.rconfig();
        let mut rdr = rconfig.reader()?;
        let headers = rdr.byte_headers()?.clone();
        let key_col = self.key_column(&rconfig, &headers)?;

        let seen_keys = HashSet::new();
        let mut writers = lru::LruCache::new(self.flag_max_open_files);
        let gen = WriterGenerator::new(self.flag_filename.clone());

        let mut pool = WriterPool {
            seen_keys,
            gen,
            rconfig,
            outdir: self.arg_outdir.clone(),
            flag_drop: self.flag_drop,
            headers,
            key_col,
        };

        let mut row = csv::ByteRecord::new();

        while rdr.read_byte_record(&mut row)? {
            // Decide what file to put this in.
            let column = &row[key_col];
            let key = match self.flag_prefix_length {
                // We exceed --prefix-length, so ignore the extra bytes.
                Some(len) if len < column.len() => &column[0..len],
                _ => &column[..],
            };
            let wtr = pool.writer(&mut writers, key)?;
            if self.flag_drop {
                wtr.write_record(row.iter().enumerate().filter_map(|(i, e)| {
                    if i != key_col {
                        Some(e)
                    } else {
                        None
                    }
                }))?;
            } else {
                wtr.write_byte_record(&row)?;
            }
        }
        Ok(())
    }
}

type BoxedWriter = csv::Writer<Box<io::Write + 'static>>;

/// Generates unique filenames based on CSV values.
struct WriterGenerator {
    template: FilenameTemplate,
    counter: usize,
    used: HashSet<String>,
    non_word_char: Regex,
}

impl WriterGenerator {
    fn new(template: FilenameTemplate) -> WriterGenerator {
        WriterGenerator {
            template,
            counter: 1,
            used: HashSet::new(),
            non_word_char: Regex::new(r"\W").unwrap(),
        }
    }

    /// Create a CSV writer for `key`.  Does not add headers.
    fn writer<P>(&mut self, path: P, key: &[u8]) -> io::Result<BoxedWriter>
    where
        P: AsRef<Path>,
    {
        let unique_value = self.unique_value(key);
        self.template.writer(path.as_ref(), &unique_value)
    }

    /// Create a CSV writer for `key`.  Does not add headers. Does not truncate file.
    fn re_open<P>(&mut self, path: P, key: &[u8]) -> io::Result<BoxedWriter>
    where
        P: AsRef<Path>,
    {
        let unique_value = self.unique_value(key);
        self.template.writer(path.as_ref(), &unique_value)
    }

    /// Generate a unique value for `key`, suitable for use in a
    /// "shell-safe" filename.  If you pass `key` twice, you'll get two
    /// different values.
    fn unique_value(&mut self, key: &[u8]) -> String {
        // Sanitize our key.
        let utf8 = String::from_utf8_lossy(key);
        let safe = self.non_word_char.replace_all(&*utf8, "").into_owned();
        let base = if safe.is_empty() {
            "empty".to_owned()
        } else {
            safe
        };

        // Now check for collisions.
        if !self.used.contains(&base) {
            self.used.insert(base.clone());
            base
        } else {
            loop {
                let candidate = format!("{}_{}", &base, self.counter);
                self.counter = self.counter.checked_add(1).unwrap_or_else(|| {
                    // We'll run out of other things long before we ever
                    // reach this, but we'll check just for correctness and
                    // completeness.
                    panic!("Cannot generate unique value")
                });
                if !self.used.contains(&candidate) {
                    self.used.insert(candidate.clone());
                    return candidate;
                }
            }
        }
    }
}

struct WriterPool {
    seen_keys: HashSet<Vec<u8>>,
    outdir: String,
    gen: WriterGenerator,
    rconfig: Config,
    flag_drop: bool,
    headers: csv::ByteRecord,
    key_col: usize,
}

impl WriterPool {
    fn new_writer(&mut self, key: &[u8]) -> io::Result<BoxedWriter> {
        if self.seen_keys.contains(key) {
            // We have seen this file before; just re-open it.
            self.gen.re_open(&*self.outdir, key)
        } else {
            self.seen_keys.insert(key.to_vec());
            // Need a new writer
            let mut wtr = self.gen.writer(&*self.outdir, key)?;
            if !self.rconfig.no_headers {
                if self.flag_drop {
                    wtr.write_record(self.headers.iter().enumerate().filter_map(|(i, e)| {
                        if i != self.key_col {
                            Some(e)
                        } else {
                            None
                        }
                    }))?;
                } else {
                    wtr.write_record(&self.headers)?;
                }
            }
            Ok(wtr)
        }
    }

    fn writer<'a>(
        &mut self,
        writers: &'a mut lru::LruCache<Vec<u8>, BoxedWriter>,
        key: &[u8],
    ) -> io::Result<&'a mut BoxedWriter> {
        // TODO: LRU doesn't currently have an Entry API to avoid computing hash 3 times.
        if !writers.contains(key) {
            writers.put(key.to_vec(), self.new_writer(key)?);
        }

        return Ok(writers.get_mut(key).unwrap());
    }
}
