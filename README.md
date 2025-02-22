# DataFusion dedicated executor investigation

This is a reproducer for https://github.com/apache/datafusion/pull/14286#issuecomment-2654795222.

## Quickstart

1. Run the server `cargo run`
2. Trigger the bug with `cargo run --bin client` in a different terminal. This default to 500 record batches which triggers the bug on my system. You can send different numbers of record batches with `cargo run client <N_BATCHES>`

- [ ] Can we trigger this without the networking object store? LocalFileSystem or even Mock? -> it doesn't seem so.

Results:
- the error is triggered in a flightsql server with a network-based object store and a sufficiently large input data set
- not triggered with MockIO object store even for large data set
- not triggered with real object store with small (single recordbatch) data set
- not triggered with real object store with multiple (~10) artifically small flight data
- the issue _is_ triggered above a certain threshold of data or record batches... may be system dependent?
  Is it crossing a threshold for datafusion to start parallelizing? Is it crossing some threshold of executor usage
  such that tokio spawns new workers or moves things between workers or...?

## Running tokio console

This application is instrumented for tokio console (`cargo install --locked tokio-console`). You need to put the following in `.cargo/config.toml`:

```toml
[build]
rustflags = ["--cfg", "tokio_unstable"]
```

and, after running the server, in another terminal, do `tokio-console`.

## Running without dedicated executor

Incidentally, by disabling the dedicated executor, this repo also demonstrates the problem we're looking to solve in the first place:

1. Disable the `dedicated-executor` feature (a default feature) on the server: `cargo run --no-default-features`
2. Run the client. On my machine, this survived much longer than the decicated executor, but consistenly displayed a timeout between client and server with
   `cargo run --bin client 5000`.
   ![image](https://github.com/user-attachments/assets/55385f67-7120-41c4-9cb7-7b3686c798a9)

3. This even shows a failure with both server and client running in release mode: `cargo run --no-default-features --release`, and for release mode I needed
   to transfer a bit more data with `cargo run --release --bin client 50000`, however if this command succeeded it would result in ~3.5GB parquet file in the
   object store, so nothing outrageous.
   ![image](https://github.com/user-attachments/assets/1ddb0cea-c5e9-4bf3-b2ef-9a2fcd306375)


The symptoms in both cases are a a client timeout while waiting for a server response, and a failed upload to minio (complete data loss).

Below is the relevant section of the backtrace I get _if I `.enable_time()` on the tokio runtime Builder!_ Otherwise I get a significantly less useful backtrace about timers.

```rust
thread 'tokio-runtime-worker' panicked at /Users/dja/.cargo/registry/src/index.crates.io-6f17d22bba15001f/tokio-1.43.0/src/net/tcp/stream.rs:160:18:
A Tokio 1.x context was found, but IO is disabled. Call `enable_io` on the runtime builder to enable IO.
stack backtrace:
...
  56: object_store::client::retry::RetryableRequest::send::{{closure}}
             at /Users/dja/.cargo/registry/src/index.crates.io-6f17d22bba15001f/object_store-0.11.2/src/client/retry.rs:277:48
  57: object_store::aws::client::Request::send::{{closure}}
             at /Users/dja/.cargo/registry/src/index.crates.io-6f17d22bba15001f/object_store-0.11.2/src/aws/client.rs:428:14
  58: object_store::aws::client::S3Client::put_part::{{closure}}
             at /Users/dja/.cargo/registry/src/index.crates.io-6f17d22bba15001f/object_store-0.11.2/src/aws/client.rs:678:39
  59: <object_store::aws::S3MultiPartUpload as object_store::upload::MultipartUpload>::put_part::{{closure}}
             at /Users/dja/.cargo/registry/src/index.crates.io-6f17d22bba15001f/object_store-0.11.2/src/aws/mod.rs:409:18
  60: <core::pin::Pin<P> as core::future::future::Future>::poll
```
