use crate::eve::filters::EveFilter;
use crate::importer::EventSink;
use std::sync::Arc;

const DEFAULT_BATCH_SIZE: usize = 10;

pub struct ZmqProcessor {
    endpoint: String,
    pub importer: EventSink,
    pub filters: Arc<Vec<EveFilter>>,
}

impl ZmqProcessor {
    pub fn new(endpoint: &str, importer: EventSink) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            importer,
            filters: Arc::new(Vec::new()),
        }
    }

    pub async fn run(&mut self) {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PULL).unwrap();
        socket.bind(&self.endpoint).unwrap();

        let mut msg = zmq::Message::new();
        loop {
            if socket.recv(&mut msg, 0).is_err() {
                continue;
            }
            let s = String::from_utf8_lossy(&msg);
            let mut record: serde_json::Value = serde_json::from_str(&s).unwrap();

            for filter in &*self.filters {
                filter.run(&mut record);
            }
            let commit = self.importer.submit(record).await.unwrap();
            if commit || self.importer.pending() >= DEFAULT_BATCH_SIZE {
                let _ = self.importer.commit().await;
            }
        }
    }
}
