# DataFusion dedicated executor investigation

## Quickstart

1. Run the server `cargo run`
2. Trigger the bug with `cargo run client`

- [ ] Can we trigger this without the networking object store? LocalFileSystem or even Mock?
- [ ] Try triggering the issue with a client that others have access to ([flight_sql_cli?](https://github.com/apache/arrow-rs/blob/main/arrow-flight/src/bin/flight_sql_client.rs)).
- [ ] Use a non-proprietary data set. How small can it be to trigger this issue?
- [ ] Refactor the repro into an integration test case that can be triggered with `cargo test`.

Results:
- the error is triggered in a flightsql server with a network-based object store and a sufficiently large input data set
- not triggered with MockIO object store even for large data set
- not triggered with real object store with small (single recordbatch) data set
- not triggered with real object store with multiple (~10) artifically small flight data

Next questions:
- what is "sufficiently large"  to trigger the issue? Is it just "more than one flight data packet", or is it a certain threshold
  of data to trigger some kind of parallelization in datafusion? Or, is a certain throughput of data that triggers tokio to spawn
  work onto new workers?
