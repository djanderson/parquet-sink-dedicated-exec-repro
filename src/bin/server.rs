use std::sync::Arc;

use arrow::array as arrow_array;
use datafusion::common::record_batch;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::{FileSink, FileSinkConfig};
use datafusion::execution::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry as _, ObjectStoreUrl,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SendableRecordBatchStream, SessionState, SessionStateBuilder};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{SessionConfig, SessionContext};
use dedicated_executor::{DedicatedExecutor, DedicatedExecutorBuilder};
use dotenvy::dotenv;
use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

pub struct Service {
    session: SessionState,
    exec: DedicatedExecutor,
}

#[tonic::async_trait]
impl Greeter for Service {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let ctx = SessionContext::new_with_state(self.session.clone());

        let batch = record_batch!(("col", Int32, vec![1, 2, 3])).unwrap();
        let schema = batch.schema();
        let stream_adapter =
            RecordBatchStreamAdapter::new(batch.schema(), futures::stream::iter(vec![Ok(batch)]));
        let stream: SendableRecordBatchStream = Box::pin(stream_adapter);

        // Configure sink
        let file_sink_config = FileSinkConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
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
        self.exec
            .spawn(async move { data_sink.write_all(stream, &ctx.task_ctx()).await.unwrap() })
            .await
            .unwrap();

        self.exec.join().await;

        println!("Test completed successfully.");

        let reply = hello_world::HelloReply {
            message: format!("Hello {}!", request.into_inner().name),
        };
        Ok(Response::new(reply))
    }
}

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
    let io_store = exec.wrap_object_store_for_io(store);

    let config =
        SessionConfig::new().set_str("datafusion.execution.parquet.compression", "zstd(19)");

    let object_store_registery = Arc::new(DefaultObjectStoreRegistry::default());
    object_store_registery.register_store(ObjectStoreUrl::local_filesystem().as_ref(), io_store);

    let runtime_env = RuntimeEnvBuilder::new()
        .with_object_store_registry(object_store_registery)
        .build_arc()
        .unwrap();

    let session = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .build();

    let addr = "[::1]:50051".parse().unwrap();
    let svc = Service { session, exec };

    println!("Service listening on {}", addr);

    Server::builder()
        .add_service(GreeterServer::new(svc))
        .serve(addr)
        .await
        .unwrap();
}

// Get an env var or panic.
fn env(var: &str) -> String {
    std::env::var(var).expect(&format!("{var} env var must be set"))
}
