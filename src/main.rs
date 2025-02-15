use std::sync::Arc;

use arrow::ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{CommandStatementIngest, SqlInfo};
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::{FileSink, FileSinkConfig};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry as _, ObjectStoreUrl,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SendableRecordBatchStream, SessionState, SessionStateBuilder};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{SessionConfig, SessionContext};
use dotenvy::dotenv;
use futures::TryStreamExt as _;
use tonic::transport::Server;
use tonic::{Request, Status};

use crate::dedicated_executor::{DedicatedExecutor, DedicatedExecutorBuilder};
use crate::mock_store::MockStore;

mod dedicated_executor;
mod mock_store;

pub struct FlightSql {
    session: SessionState,
    exec: DedicatedExecutor,
}

#[tonic::async_trait]
impl FlightSqlService for FlightSql {
    type FlightService = Self;

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        unimplemented!()
    }

    async fn do_put_statement_ingest(
        &self,
        ticket: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> tonic::Result<i64> {
        println!("Got a request from {:?}", request.remote_addr());

        let ctx = SessionContext::new_with_state(self.session.clone());
        let mut flight_data_stream = request.into_inner();

        // Extract flight descriptor out of first FlightData
        let first_flight_data = flight_data_stream
            .peek()
            .await
            .expect("first flight data available at this point")
            .as_ref()
            .map_err(|e| Status::failed_precondition(format!("Failed to read FlightData: {e}")))?;

        let schema = Arc::new(
            try_schema_from_flatbuffer_bytes(&first_flight_data.data_header).map_err(|e| {
                Status::invalid_argument(format!("Missing schema in first message: {e}"))
            })?,
        );

        let path = format!(
            "{}/{}/{}.parquet",
            ticket.catalog(),
            ticket.schema(),
            ticket.table
        );
        let table_path = ListingTableUrl::parse(format!("/{}", path))
            .map_err(|e| Status::internal(format!("invalid table url {path}: {e}")))?;

        // Configure sink
        let file_sink_config = FileSinkConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_groups: vec![],
            table_paths: vec![table_path],
            output_schema: schema.clone(),
            table_partition_cols: vec![],
            insert_op: InsertOp::Overwrite,
            keep_partition_by_columns: false,
            file_extension: String::from("parquet"),
        };
        let table_options = Default::default();
        let data_sink = ParquetSink::new(file_sink_config, table_options);

        let record_batch_stream =
            //FlightRecordBatchStream::new_from_flight_data(flight_data_stream.map_err(|e| e.into()));
            FlightRecordBatchStream::new_from_flight_data(flight_data_stream.map_err(|e| e.into()));

        // Wrap Arrow Flight stream of record batches in DataFusion adapter
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            record_batch_stream.map_err(|e| DataFusionError::External(Box::new(e))),
        ));

        // Execute write on dedicated runtime
        println!("writing data to object store");
        let rows_written = self
            .exec
            .spawn(async move { data_sink.write_all(stream, &ctx.task_ctx()).await.unwrap() })
            .await
            .unwrap();
        println!("wrote {rows_written} rows");

        //self.exec.join().await;

        Ok(rows_written as i64)
    }
}

#[tokio::main]
async fn main() {
    dotenv().unwrap();
    let exec = DedicatedExecutorBuilder::new().build();
    let store = MockStore::create().await;
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
    let flight_sql_svc = FlightServiceServer::new(FlightSql { session, exec })
        .max_decoding_message_size(64 * 1024 * 1024);

    println!("Service listening on {}", addr);

    Server::builder()
        .add_service(flight_sql_svc)
        .serve(addr)
        .await
        .unwrap();
}
