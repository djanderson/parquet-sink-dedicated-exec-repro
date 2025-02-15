# DataFusion dedicated executor investigation

- [ ] Can we trigger this without the networking object store? LocalFileSystem or even Mock?
- [ ] Try triggering the issue with a client that others have access to ([flight_sql_cli?](https://github.com/apache/arrow-rs/blob/main/arrow-flight/src/bin/flight_sql_client.rs)).
- [ ] Use a non-proprietary data set. How small can it be to trigger this issue?
- [ ] Refactor the repro into an integration test case that can be triggered with `cargo test`.
