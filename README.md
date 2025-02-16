# DataFusion dedicated executor investigation

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
