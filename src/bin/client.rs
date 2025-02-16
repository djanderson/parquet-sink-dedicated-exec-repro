use std::cell::RefCell;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, RecordBatch};
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandStatementIngest;
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt as _;
use rand::rngs::ThreadRng;
use rand::Rng;
use tonic::transport::Endpoint;

thread_local! {
    static RNG: RefCell<ThreadRng> = RefCell::new(rand::rng());
}

#[tokio::main]
async fn main() {
    // Read command-line args
    let args: Vec<String> = env::args().collect();
    let n_batches = args
        .get(1)
        .map(|s| {
            s.parse::<usize>()
                .expect("Failed to parse argument as an integer")
        })
        .unwrap_or(500); // Default to 500 if no argument is given

    let endpoint = Endpoint::new("http://localhost:50051")
        .unwrap()
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(20))
        .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);

    let channel = endpoint.connect().await.unwrap();

    let mut client = FlightSqlServiceClient::new(channel);
    let command = CommandStatementIngest {
        table: String::from("test"),
        ..Default::default()
    };

    println!("Sending {} record batches of data...", n_batches);
    let batch_cols = 10;
    let batch_rows = 1000;
    let stream = futures::stream::repeat_with(move || Ok(record_batch(batch_cols, batch_rows)))
        .take(n_batches);

    match client.execute_ingest(command, stream).await {
        Ok(rows) => println!("wrote {rows} rows"),
        Err(e) => eprintln!("error: {e}"),
    }
}

// Generate a record batch with `cols` columns named "colN" (1-indexed), with random f64 values.
fn record_batch(cols: usize, rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        (0..cols)
            .map(|i| Arc::new(Field::new(&format!("col{}", i), DataType::Float64, false)))
            .collect::<Vec<_>>(),
    ));

    let columns: Vec<ArrayRef> = (0..cols)
        .map(|_| {
            let values: Vec<f64> = RNG.with(|rng| {
                (0..rows)
                    .map(|_| rng.borrow_mut().random::<f64>())
                    .collect()
            });
            Arc::new(Float64Array::from(values)) as ArrayRef
        })
        .collect();

    RecordBatch::try_new(schema, columns).unwrap()
}
