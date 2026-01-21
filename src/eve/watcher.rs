// SPDX-FileCopyrightText: (C) 2023 Jason Ish <jason@codemonkey.net>
// SPDX-License-Identifier: MIT

use tracing::{info, warn};

use super::{filters::EveFilter, EveReader, Processor, ZmqProcessor};
use crate::{eve::filters::EveBoxMetadataFilter, importer::EventSink};
use std::time::Duration;
use std::{collections::HashSet, path::PathBuf, sync::Arc};

/// Watches a collection of filename patterns and starts a new EVE
/// pipeline when a new file is found.
pub struct EvePatternWatcher {
    patterns: Vec<String>,
    filenames: HashSet<PathBuf>,
    endpoints: HashSet<String>,
    sink: EventSink,
    filters: Vec<EveFilter>,
    end: bool,
    bookmark_directory: Option<String>,
    data_directory: Option<String>,
}

impl EvePatternWatcher {
    pub fn new(
        patterns: Vec<String>,
        sink: EventSink,
        filters: Vec<EveFilter>,
        end: bool,
        bookmark_directory: Option<String>,
        data_directory: Option<String>,
    ) -> Self {
        Self {
            patterns,
            filenames: HashSet::new(),
            endpoints: HashSet::new(),
            sink,
            filters,
            end,
            bookmark_directory,
            data_directory,
        }
    }

    pub fn check(&mut self) {
        for pattern in &self.patterns {
            // pattern 是以 tcp/pgm/ipc 开头，则是zmq的endpoint
            if pattern.starts_with("tcp://")
                || pattern.starts_with("pgm://")
                || pattern.starts_with("ipc://")
            {
                if !self.endpoints.contains(pattern) {
                    info!("Found EVE input endpoint {}", pattern);
                    self.start_zmq(pattern);
                    self.endpoints.insert(pattern.to_string());
                }
                continue;
            }
            // This is for error reporting to the user, in the case
            // where the parent directory of the log files is not
            // readable by EveBox.
            if let Some(p) = PathBuf::from(pattern).parent() {
                if let Err(err) = std::fs::read_dir(p) {
                    warn!(
                        "Failed to read directory {}, EVE log files are likely unreadable: {}",
                        p.display(),
                        err
                    );
                }
            }
            if let Ok(paths) = crate::path::expand(pattern) {
                for path in paths {
                    if !self.filenames.contains(&path) {
                        info!("Found EVE input file {}", path.display());
                        self.start_file(&path);
                        self.filenames.insert(path);
                    }
                }
            }
        }
    }

    fn start_file(&self, filename: &PathBuf) {
        let reader = EveReader::new(filename.clone());
        let mut processor = Processor::new(reader, self.sink.clone());
        let mut filters = self.filters.clone();
        filters.push(
            EveBoxMetadataFilter {
                filename: Some(filename.display().to_string()),
            }
            .into(),
        );

        let bookmark_filename = crate::server::main::get_bookmark_filename(
            filename,
            self.bookmark_directory.as_deref(),
            self.data_directory.as_deref(),
        );

        processor.filters = Arc::new(filters);
        if bookmark_filename.is_none() && !self.end {
            warn!(
                "Failed to create bookmark file for {}, will read from end of file",
                filename.display()
            );
            processor.end = false;
        } else {
            processor.end = self.end;
        }
        processor.report_interval = Duration::from_secs(60);
        processor.bookmark_filename = bookmark_filename;
        info!("Starting EVE processor for {}", filename.display());
        tokio::spawn(async move {
            processor.run().await;
        });
    }

    fn start_zmq(&self, endpoint: &str) {
        let mut zmq_processor = ZmqProcessor::new(endpoint, self.sink.clone());
        zmq_processor.filters = Arc::new(self.filters.clone());
        info!("Starting EVE processor for {}", endpoint);
        tokio::spawn(async move {
            zmq_processor.run().await;
        });
    }

    pub fn run(mut self) {
        tokio::spawn(async move {
            loop {
                self.check();
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            }
        });
    }
}
