# DataFusion dedicated executor investigation

This is a reproducer for https://github.com/apache/datafusion/pull/14286#issuecomment-2654795222.

## Quickstart

1. Run a minio container
```sh
docker run \
   -p 9000:9000 \
   -p 9001:9001 \
   --user $(id -u):$(id -g) \
   --name minio1 \
   -e "MINIO_ROOT_USER=admin" \
   -e "MINIO_ROOT_PASSWORD=changeme" \
   -v ${HOME}/minio/data:/data \
   quay.io/minio/minio server /data --console-address ":9001"
   ```
2. Log into http://localhost:9000 with `admin` and `changme` and create a **bucket** and **API key**

<p align="center">
<img src="https://github.com/user-attachments/assets/e71ea631-37e1-407a-b26c-5137780ff0fd" width="200">
</p>

3. Create an `.env` file with the following contents, filled in:
```sh
BUCKET_NAME=
ACCESS_KEY=
SECRET_KEY=
```
4. Run the server `cargo run`
5. Trigger the bug with `cargo run client` in a different terminal. This default to 500 record batches which triggers the bug on my system. You can send different numbers of record batches with `cargo run client <N_BATCHES>`

- [ ] Can we trigger this without the networking object store? LocalFileSystem or even Mock? -> it doesn't seem so.

Results:
- the error is triggered in a flightsql server with a network-based object store and a sufficiently large input data set
- not triggered with MockIO object store even for large data set
- not triggered with real object store with small (single recordbatch) data set
- not triggered with real object store with multiple (~10) artifically small flight data
- the issue _is_ triggered above a certain threshold of data or record batches... may be system dependent?
  Is it crossing a threshold for datafusion to start parallelizing? Is it crossing some threshold of executor usage
  such that tokio spawns new workers or moves things between workers or...?

## Running without dedicated executor

Incidentally, by disabling the dedicated executor, this repo also demonstrates the problem we're looking to solve in the first place:

1. Disable the `dedicated-executor` feature (a default feature) on the server: `cargo run --no-default-features`
2. Run the client. On my machine, this survived much longer than the decicated executor, but consistenly displayed a timeout between client and server with
   `cargo run --bin client 5000`.
   ![image](https://github.com/user-attachments/assets/55385f67-7120-41c4-9cb7-7b3686c798a9)

4. This even shows a failure with both server and client running in release mode: `cargo run --no-default-features --release`, and for release mode I needed
   to transfer a bit more data with `cargo run --release --bin client 50000`, however if this command succeeded it would result in ~3.5GB parquet file in the
   object store, so nothing outrageous.
   ![image](https://github.com/user-attachments/assets/1ddb0cea-c5e9-4bf3-b2ef-9a2fcd306375)


The symptoms in both cases are a a client timeout while waiting for a server response, and a failed upload to minio (complete data loss).
