use std::sync::Arc;

use arrow::array as arrow_array;
use datafusion::common::record_batch;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::{FileSink, FileSinkConfig};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use dotenvy::dotenv;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;

use crate::dedicated_executor::DedicatedExecutorBuilder;

mod dedicated_executor;

#[tokio::main]
async fn main() {
    dotenv().unwrap();
    let exec = DedicatedExecutorBuilder::new().build();
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://localhost:9000"))
            .with_allow_http(true)
            .with_bucket_name(env("BUCKET_NAME"))
            .with_access_key_id(env("ACCESS_KEY"))
            .with_secret_access_key(env("SECRET_KEY"))
            .build()
            .unwrap(),
    );
    let store = exec.wrap_object_store_for_io(store);

    let batch = record_batch!(("col", Int32, vec![1, 2, 3])).unwrap();
    let schema = batch.schema();
    let stream_adapter =
        RecordBatchStreamAdapter::new(batch.schema(), futures::stream::iter(vec![Ok(batch)]));
    let stream: SendableRecordBatchStream = Box::pin(stream_adapter);

    let ctx = SessionContext::new();
    let object_store_url = ObjectStoreUrl::local_filesystem();
    ctx.register_object_store(object_store_url.as_ref(), store);

    // Configure sink
    let file_sink_config = FileSinkConfig {
        object_store_url,
        file_groups: vec![],
        table_paths: vec![ListingTableUrl::parse("/test.parquet").unwrap()],
        output_schema: schema,
        table_partition_cols: vec![],
        insert_op: InsertOp::Overwrite,
        keep_partition_by_columns: false,
        file_extension: String::from("parquet"),
    };
    let table_options = Default::default();
    let data_sink = ParquetSink::new(file_sink_config, table_options);

    // Execute write on dedicated runtime
    exec.spawn(async move { data_sink.write_all(stream, &ctx.task_ctx()).await.unwrap() })
        .await
        .unwrap();

    exec.join().await;

    println!("Test completed successfully.");
}

// Get an env var or panic.
fn env(var: &str) -> String {
    std::env::var(var).expect(&format!("{var} env var must be set"))
}
