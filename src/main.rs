use std::future;
use std::sync::Arc;

use arrow::ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{CommandStatementIngest, SqlInfo};
use dedicated_executor::{DedicatedExecutor, DedicatedExecutorBuilder};
use dotenvy::dotenv;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use tonic::transport::Server;
use tonic::{Request, Status};

mod dedicated_executor;
mod localstack;

#[allow(unused)]
pub struct FlightSql {
    store: Arc<dyn ObjectStore>,
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

        // Just print which flight data packet we're processing.
        let flight_data_stream = flight_data_stream.scan(0, |n, fd| {
            println!("processing FlightData # {n}");
            *n += 1;
            future::ready(Some(fd))
        });

        let path = format!("{}.parquet", ticket.table);
        let object_store_writer = ParquetObjectWriter::new(self.store.clone(), path.into());
        let writer = AsyncArrowWriter::try_new(object_store_writer, schema, None).unwrap();
        let record_batch_stream =
            FlightRecordBatchStream::new_from_flight_data(flight_data_stream.map_err(|e| e.into()));

        #[cfg(feature = "dedicated-executor")]
        let rows_written = self
            .exec
            .spawn(async move { write_stream(writer, record_batch_stream).await })
            .await
            .unwrap();
        #[cfg(not(feature = "dedicated-executor"))]
        let rows_written = write_stream(writer, record_batch_stream).await;

        Ok(rows_written as i64)
    }
}

async fn write_stream(
    mut writer: AsyncArrowWriter<ParquetObjectWriter>,
    mut stream: FlightRecordBatchStream,
) -> usize {
    let mut rows_written = 0;
    while let Some(record_batch) = stream.next().await {
        match record_batch {
            Ok(rb) => {
                writer.write(&rb).await.unwrap();
                rows_written += rb.num_rows();
            }
            Err(e) => eprintln!("error: in record batch stream: {e}"),
        }
    }
    writer.close().await.unwrap();
    rows_written
}

#[tokio::main]
async fn main() {
    dotenv().unwrap();

    println!("Starting localstack object store");
    let localstack = localstack::localstack_container().await;
    let localstack_host = localstack.get_host().await.unwrap();
    let localstack_port = localstack.get_host_port_ipv4(4566).await.unwrap();

    #[allow(unused)]
    let exec = DedicatedExecutorBuilder::new().build();

    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://{}:{}", localstack_host, localstack_port))
            .with_allow_http(true)
            .with_bucket_name("warehouse")
            .with_access_key_id("user")
            .with_secret_access_key("password")
            .build()
            .unwrap(),
    );
    #[cfg(feature = "dedicated-executor")]
    let store = exec.wrap_object_store_for_io(store);

    let addr = "[::1]:50051".parse().unwrap();
    let flight_sql_svc = FlightServiceServer::new(FlightSql { store, exec });

    println!("Service listening on {}", addr);

    Server::builder()
        .add_service(flight_sql_svc)
        .serve(addr)
        .await
        .unwrap();
}
