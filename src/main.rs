use std::fs;
use std::path::Path;
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

mod dedicated_executor;

use dedicated_executor::DedicatedExecutorBuilder;
use object_store::ObjectStore;

#[tokio::main]
async fn main() {
    let exec = DedicatedExecutorBuilder::new().build();
    let store_prefix = Path::new("data");
    fs::create_dir(store_prefix).unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(store_prefix).unwrap());
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
