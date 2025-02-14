// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [DedicatedExecutor] for running CPU-bound tasks on a separate tokio runtime.

use std::cell::RefCell;
use std::fmt::Display;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::common::error::GenericError;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::future::{BoxFuture, Shared};
use futures::stream::BoxStream;
use futures::{Future, FutureExt, Stream, StreamExt, TryFutureExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, UploadPart,
};
use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tokio_stream::wrappers::ReceiverStream;

/// Create a [`DedicatedExecutorBuilder`] from a tokio [`Builder`]
impl From<Builder> for DedicatedExecutorBuilder {
    fn from(value: Builder) -> Self {
        Self::new_from_builder(value)
    }
}

/// Manages a separate tokio [`Runtime`] (thread pool) for executing CPU bound
/// tasks such as DataFusion `ExecutionPlans`.
///
/// See [`DedicatedExecutorBuilder`] for creating a new instance.
///
/// A `DedicatedExecutor` can helps avoid issues when runnnig IO and CPU bound tasks on the
/// same thread pool by running futures (and any `tasks` that are
/// `tokio::task::spawned` by them) on a separate tokio [`Executor`].
///
/// `DedicatedExecutor`s can be `clone`ed and all clones share the same thread pool.
///
/// Since the primary use for a `DedicatedExecutor` is offloading CPU bound
/// work, IO work can not be performed on tasks launched in the Executor.
///
/// To perform IO, see:
/// - [`Self::spawn_io`]
/// - [`Self::wrap_object_store`]
///
/// When [`DedicatedExecutorBuilder::build`] is called, a reference to the
/// "current" tokio runtime will be stored and used, via [`register_io_runtime`] by all
/// threads spawned by the executor. Any I/O done by threads in this
/// [`DedicatedExecutor`] should use [`spawn_io`], which will run them on the
/// I/O runtime.
///
/// # TODO examples
///
/// # Background
///
/// Tokio has the notion of the "current" runtime, which runs the current future
/// and any tasks spawned by it. Typically, this is the runtime created by
/// `tokio::main` and is used for the main application logic and I/O handling
///
/// For CPU bound work, such as DataFusion plan execution, it is important to
/// run on a separate thread pool to avoid blocking the I/O handling for extended
/// periods of time in order to avoid long poll latencies (which decreases the
/// throughput of small requests under concurrent load).
///
/// # IO Scheduling
///
/// I/O, such as network calls, should not be performed on the runtime managed
/// by [`DedicatedExecutor`]. As tokio is a cooperative scheduler, long-running
/// CPU tasks will not be preempted and can therefore starve servicing of other
/// tasks. This manifests in long poll-latencies, where a task is ready to run
/// but isn't being scheduled to run. For CPU-bound work this isn't a problem as
/// there is no external party waiting on a response, however, for I/O tasks,
/// long poll latencies can prevent timely servicing of IO, which can have a
/// significant detrimental effect.
///
/// # Details
///
/// The worker thread priority is set to low so that such tasks do
/// not starve other more important tasks (such as answering health checks)
///
/// Follows the example from stack overflow and spawns a new
/// thread to install a Tokio runtime "context"
/// <https://stackoverflow.com/questions/62536566>
///
/// # Trouble Shooting:
///
/// ## "No IO runtime registered. Call `register_io_runtime`/`register_current_runtime_for_io` in current thread!
///
/// This means that IO was attempted on a tokio runtime that was not registered
/// for IO. One solution is to run the task using [DedicatedExecutor::spawn].
///
/// ## "Cannot drop a runtime in a context where blocking is not allowed"`
///
/// If you try to use this structure from an async context you see something like
/// thread 'test_builder_plan' panicked at 'Cannot
/// drop a runtime in a context where blocking is not allowed it means  This
/// happens when a runtime is dropped from within an asynchronous
/// context.', .../tokio-1.4.0/src/runtime/blocking/shutdown.rs:51:21
///
/// # Notes
/// This code is derived from code originally written for [InfluxDB 3.0]
///
/// [InfluxDB 3.0]: https://github.com/influxdata/influxdb3_core/tree/6fcbb004232738d55655f32f4ad2385523d10696/executor
#[derive(Clone, Debug)]
pub struct DedicatedExecutor {
    /// State for managing Tokio Runtime Handle for CPU tasks
    state: Arc<RwLock<State>>,
}

#[allow(unused)]
impl DedicatedExecutor {
    /// Create a new builder to crate a [`DedicatedExecutor`]
    pub fn builder() -> DedicatedExecutorBuilder {
        DedicatedExecutorBuilder::new()
    }

    /// Runs the specified [`Future`] (and any tasks it spawns) on the thread
    /// pool managed by this `DedicatedExecutor`.
    ///
    /// See The struct documentation for more details
    ///
    /// The specified task is added to the tokio executor immediately and
    /// compete for the thread pool's resources.
    ///
    /// # Behavior on `Drop`
    ///
    /// UNLIKE [`tokio::task::spawn`], the returned future is **cancelled** when
    /// it is dropped. Thus, you need ensure the returned future lives until it
    /// completes (call `await`) or you wish to cancel it.
    pub fn spawn<T>(&self, task: T) -> impl Future<Output = Result<T::Output, JobError>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let handle = {
            let state = self.state.read().expect("lock not poisoned");
            state.handle.clone()
        };

        let Some(handle) = handle else {
            return futures::future::err(JobError::WorkerGone).boxed();
        };

        // use JoinSet implement "cancel on drop"
        let mut join_set = JoinSet::new();
        join_set.spawn_on(task, &handle);
        async move {
            join_set
                .join_next()
                .await
                .expect("just spawned task")
                .map_err(|e| match e.try_into_panic() {
                    Ok(e) => {
                        let s = if let Some(s) = e.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = e.downcast_ref::<&str>() {
                            s.to_string()
                        } else {
                            "unknown internal error".to_string()
                        };

                        JobError::Panic { msg: s }
                    }
                    Err(_) => JobError::WorkerGone,
                })
        }
        .boxed()
    }

    /// signals shutdown of this executor and any Clones
    pub fn shutdown(&self) {
        // hang up the channel which will cause the dedicated thread
        // to quit
        let mut state = self.state.write().expect("lock not poisoned");
        state.handle = None;
        state.start_shutdown.notify_one();
    }

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all clones of this
    /// `DedicatedExecutor` as well.
    ///
    /// Only the first all to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
    ///
    /// # Panic / Drop
    /// [`DedicatedExecutor`] implements shutdown on [`Drop`]. You should just use this behavior and NOT call
    /// [`join`](Self::join) manually during [`Drop`] or panics because this might lead to another panic, see
    /// <https://github.com/rust-lang/futures-rs/issues/2575>.
    pub async fn join(&self) {
        self.shutdown();

        // get handle mutex is held
        let handle = {
            let state = self.state.read().expect("lock not poisoned");
            state.completed_shutdown.clone()
        };

        // wait for completion while not holding the mutex to avoid
        // deadlocks
        handle.await.expect("Thread died?")
    }

    /// Returns an [`ObjectStore`] instance that will always perform I/O work on the
    /// IO_RUNTIME.
    ///
    /// Note that this object store will only work correctly if run on this
    /// dedicated executor. If you try and use it on another executor, it will
    /// panic with "no IO runtime registered" type error.
    pub fn wrap_object_store_for_io(
        &self,
        object_store: Arc<dyn ObjectStore>,
    ) -> Arc<IoObjectStore> {
        Arc::new(IoObjectStore::new(self.clone(), object_store))
    }

    /// Runs the [`SendableRecordBatchStream`] on the CPU thread pool
    ///
    /// This is a convenience method around [`Self::run_cpu_stream`]
    pub fn run_cpu_sendable_record_batch_stream(
        &self,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let schema = stream.schema();
        let stream = self.run_cpu_stream(stream, |job_error| {
            let job_error: GenericError = Box::new(job_error);
            DataFusionError::from(job_error)
                .context("Running RecordBatchStream on DedicatedExecutor")
        });

        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    /// Runs a stream on the CPU thread pool
    ///
    /// # Note
    /// Ths stream must produce Results so that any errors on the dedicated
    /// executor (like a panic or shutdown) can be communicated back.
    ///
    /// # Arguments:
    /// - stream: the stream to run on this dedicated executor
    /// - converter: a function that converts a [`JobError`] to the error type of the stream
    pub fn run_cpu_stream<X, E, S, C>(
        &self,
        stream: S,
        converter: C,
    ) -> impl Stream<Item = Result<X, E>> + Send + 'static
    where
        X: Send + 'static,
        E: Send + 'static,
        S: Stream<Item = Result<X, E>> + Send + 'static,
        C: Fn(JobError) -> E + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        // make a copy to send any job error results back
        let error_tx = tx.clone();

        // This task will run on the CPU runtime
        let task = self.spawn(async move {
            // drive the stream forward on the CPU runtime, sending results
            // back to the original (presumably IO) runtime
            let mut stream = Box::pin(stream);
            while let Some(result) = stream.next().await {
                // try to send to the sender, if error means the
                // receiver has been closed and we terminate early
                if tx.send(result).await.is_err() {
                    return;
                }
            }
        });

        // fire up a task on the current runtime which transfers results back
        // from the CPU runtime to the calling runtime
        let mut set = JoinSet::new();
        set.spawn(async move {
            if let Err(e) = task.await {
                // error running task, try and report it back. An error sending
                // means the receiver was dropped so there is nowhere to
                // report errors. Thus ignored via ok()
                error_tx.send(Err(converter(e))).await.ok();
            }
        });

        StreamAndTask {
            inner: ReceiverStream::new(rx),
            set,
        }
    }

    /// Runs a stream on the IO thread pool
    ///
    /// Ths stream must produce Results so that any errors on the dedicated
    /// executor (like a panic or shutdown) can be communicated back.
    ///
    /// Note this has a slightly different API compared to
    /// [`Self::run_cpu_stream`] because the DedicatedExecutor  doesn't monitor
    /// the CPU thread pool (that came elsewhere)
    ///
    /// # Arguments:
    /// - stream: the stream to run on this dedicated executor
    pub fn run_io_stream<X, E, S>(
        &self,
        stream: S,
    ) -> impl Stream<Item = Result<X, E>> + Send + 'static
    where
        X: Send + 'static,
        E: Send + 'static,
        S: Stream<Item = Result<X, E>> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let mut set = JoinSet::new();
        set.spawn(Self::spawn_io_static(async move {
            // drive the stream forward on the IO runtime, sending results
            // back to the original runtime
            let mut stream = Box::pin(stream);
            while let Some(result) = stream.next().await {
                // try to send to the sender, if error means the
                // receiver has been closed and we terminate early
                if tx.send(result).await.is_err() {
                    return;
                }
            }
        }));

        StreamAndTask {
            inner: ReceiverStream::new(rx),
            set,
        }
    }

    /// Registers `handle` as the IO runtime for this thread
    ///
    /// Users should not need to call this function as it is handled by
    /// [`DedicatedExecutorBuilder`]
    ///
    /// # Notes
    ///
    /// This sets a thread-local variable
    ///
    /// See [`spawn_io`](Self::spawn_io) for more details
    pub fn register_io_runtime(handle: Option<Handle>) {
        IO_RUNTIME.set(handle)
    }

    /// Runs `fut` on IO runtime of this DedicatedExecutor
    pub async fn spawn_io<Fut>(&self, fut: Fut) -> Fut::Output
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send,
    {
        Self::spawn_io_static(fut).await
    }

    /// Runs `fut` on the runtime most recently registered by *any* DedicatedExecutor.
    ///
    /// When possible, it is preferred to use [`Self::spawn_io`]
    ///
    /// This functon is provided, similarly to tokio's [`Handle::current()`] to
    /// avoid having to thread a `DedicatedExecutor` throughout your program.
    ///
    /// # Panic
    /// Needs a IO runtime [registered](register_io_runtime).
    pub async fn spawn_io_static<Fut>(fut: Fut) -> Fut::Output
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send,
    {
        let h = IO_RUNTIME.with_borrow(|h| h.clone()).expect(
            "No IO runtime registered. If you hit this panic, it likely \
            means a DataFusion plan or other CPU bound work is running on the \
            a tokio threadpool used for IO. Try spawning the work using \
            `DedicatedExecutor::spawn` or for tests `DedicatedExecutor::register_current_runtime_for_io`",
        );
        DropGuard(h.spawn(fut)).await
    }
}

/// A wrapper around a receiver stream and task that ensures the inner
/// task is cancelled on drop
struct StreamAndTask<T> {
    inner: ReceiverStream<T>,
    /// Task which produces no output. On drop the outstanding task is cancelled
    #[expect(dead_code)]
    set: JoinSet<()>,
}

impl<T> Stream for StreamAndTask<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

thread_local! {
    /// Tokio runtime `Handle` for doing network (I/O) operations, see [`spawn_io`]
    pub static IO_RUNTIME: RefCell<Option<Handle>> = const { RefCell::new(None) };
}

struct DropGuard<T>(JoinHandle<T>);
impl<T> Drop for DropGuard<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}

impl<T> Future for DropGuard<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(match std::task::ready!(self.0.poll_unpin(cx)) {
            Ok(v) => v,
            Err(e) if e.is_cancelled() => panic!("IO runtime was shut down"),
            Err(e) => std::panic::resume_unwind(e.into_panic()),
        })
    }
}

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio Executor.
///
/// The state is only used by the "outer" API, not by the newly created runtime. The new runtime waits for
/// [`start_shutdown`](Self::start_shutdown) and signals the completion via
/// [`completed_shutdown`](Self::completed_shutdown) (for which is owns the sender side).
#[derive(Debug)]
struct State {
    /// Runtime handle for CPU tasks
    ///
    /// This is `None` when the executor is shutting down.
    handle: Option<Handle>,

    /// If notified, the executor tokio runtime will begin to shutdown.
    ///
    /// We could implement this by checking `handle.is_none()` in regular intervals but requires regular wake-ups and
    /// locking of the state. Just using a proper async signal is nicer.
    start_shutdown: Arc<Notify>,

    /// Receiver side indicating that shutdown is complete.
    completed_shutdown: Shared<BoxFuture<'static, Result<(), Arc<RecvError>>>>,

    /// The inner thread that can be used to join during drop.
    thread: Option<std::thread::JoinHandle<()>>,
}

/// IMPORTANT: Implement `Drop` for [`State`], NOT for [`DedicatedExecutor`],
/// because the executor can be cloned and clones share their inner state.
impl Drop for State {
    fn drop(&mut self) {
        if self.handle.is_some() {
            eprintln!("DedicatedExecutor dropped without calling shutdown()");
            self.handle = None;
            self.start_shutdown.notify_one();
        }

        // do NOT poll the shared future if we are panicking due to https://github.com/rust-lang/futures-rs/issues/2575
        if !std::thread::panicking() && self.completed_shutdown.clone().now_or_never().is_none() {
            eprintln!("DedicatedExecutor dropped without waiting for worker termination",);
        }

        // join thread but don't care about the results
        self.thread.take().expect("not dropped yet").join().ok();
    }
}

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60 * 5);

/// Potential error returned when polling [`DedicatedExecutor::spawn`].
#[derive(Debug)]
pub enum JobError {
    WorkerGone,
    Panic { msg: String },
}

impl Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobError::WorkerGone => {
                write!(f, "Worker thread gone, executor was likely shut down")
            }
            JobError::Panic { msg } => write!(f, "Panic: {}", msg),
        }
    }
}

impl std::error::Error for JobError {}

/// Builder for [`DedicatedExecutor`]
pub struct DedicatedExecutorBuilder {
    /// Name given to all execution threads. Defaults to "DedicatedExecutor"
    name: String,
    /// Builder for tokio runtime. Defaults to multi-threaded builder
    runtime_builder: Builder,
}

impl From<JobError> for DataFusionError {
    fn from(value: JobError) -> Self {
        DataFusionError::External(Box::new(value)).context("JobError from DedicatedExecutor")
    }
}

#[allow(unused)]
impl DedicatedExecutorBuilder {
    /// Create a new `DedicatedExecutorBuilder` with default values
    ///
    /// Note that by default this `DedicatedExecutor` will not be able to
    /// perform network I/O.
    pub fn new() -> Self {
        Self {
            name: String::from("DedicatedExecutor"),
            runtime_builder: Builder::new_multi_thread(),
        }
    }

    /// Create a new `DedicatedExecutorBuilder` from a pre-existing tokio
    /// runtime [`Builder`].
    ///
    /// This method permits customizing the tokio [`Executor`] used for the
    /// [`DedicatedExecutor`]
    pub fn new_from_builder(runtime_builder: Builder) -> Self {
        Self {
            name: String::from("DedicatedExecutor"),
            runtime_builder,
        }
    }

    /// Set the name of the dedicated executor (appear in the names of each thread).
    ///
    /// Defaults to "DedicatedExecutor"
    #[cfg(test)]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the number of worker threads. Defaults to the tokio default (the
    /// number of virtual CPUs)
    #[allow(dead_code)]
    pub fn with_worker_threads(mut self, num_threads: usize) -> Self {
        self.runtime_builder.worker_threads(num_threads);
        self
    }

    /// Creates a new `DedicatedExecutor` with a dedicated tokio
    /// executor that is separate from the thread pool created via
    /// `[tokio::main]` or similar.
    ///
    /// Note: If [`DedicatedExecutorBuilder::build`] is called from an existing
    /// tokio runtime, it will assume that the existing runtime should be used
    /// for I/O.
    ///
    /// See the documentation on [`DedicatedExecutor`] for more details.
    pub fn build(self) -> DedicatedExecutor {
        let Self {
            name,
            runtime_builder,
        } = self;

        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel();
        let (tx_handle, rx_handle) = std::sync::mpsc::channel();

        let io_handle = Handle::try_current().ok();
        let thread = std::thread::Builder::new()
            .name(format!("{name} driver"))
            .spawn(move || {
                // also register the IO runtime for the current thread, since it might be used as well (esp. for the
                // current thread RT)
                DedicatedExecutor::register_io_runtime(io_handle.clone());

                println!("Creating DedicatedExecutor",);

                let mut runtime_builder = runtime_builder;
                let runtime = runtime_builder
                    .on_thread_start(move || {
                        DedicatedExecutor::register_io_runtime(io_handle.clone())
                    })
                    .build()
                    .expect("Creating tokio runtime");

                runtime.block_on(async move {
                    // Enable the "notified" receiver BEFORE sending the runtime handle back to the constructor thread
                    // (i.e .the one that runs `new`) to avoid the potential (but unlikely) race that the shutdown is
                    // started right after the constructor finishes and the new runtime calls
                    // `notify_shutdown_captured.notified().await`.
                    //
                    // Tokio provides an API for that by calling `enable` on the `notified` future (this requires
                    // pinning though).
                    let shutdown = notify_shutdown_captured.notified();
                    let mut shutdown = std::pin::pin!(shutdown);
                    shutdown.as_mut().enable();

                    if tx_handle.send(Handle::current()).is_err() {
                        return;
                    }
                    shutdown.await;
                });

                runtime.shutdown_timeout(SHUTDOWN_TIMEOUT);

                // send shutdown "done" signal
                tx_shutdown.send(()).ok();
            })
            .expect("executor setup");

        let handle = rx_handle.recv().expect("driver started");

        let state = State {
            handle: Some(handle),
            start_shutdown: notify_shutdown,
            completed_shutdown: rx_shutdown.map_err(Arc::new).boxed().shared(),
            thread: Some(thread),
        };

        DedicatedExecutor {
            state: Arc::new(RwLock::new(state)),
        }
    }
}

/// Wraps an inner [`ObjectStore`] so that all underlying methods are run on
/// the Tokio Runtime dedicated to doing IO
///
/// # See Also
///
/// [`DedicatedExecutor::spawn_io`] for more details
#[derive(Debug)]
pub struct IoObjectStore {
    dedicated_executor: DedicatedExecutor,
    inner: Arc<dyn ObjectStore>,
}

impl IoObjectStore {
    pub fn new(executor: DedicatedExecutor, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            dedicated_executor: executor,
            inner: object_store,
        }
    }

    /// Wrap the result stream if necessary
    fn wrap_if_necessary(&self, payload: GetResultPayload) -> GetResultPayload {
        match payload {
            GetResultPayload::File(_, _) => payload,
            GetResultPayload::Stream(stream) => {
                let new_stream = self.dedicated_executor.run_io_stream(stream).boxed();
                GetResultPayload::Stream(new_stream)
            }
        }
    }
}

impl Display for IoObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "IoObjectStore")
    }
}

#[async_trait]
impl ObjectStore for IoObjectStore {
    /// Return GetResult for the location and options.
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        let mut results = self
            .dedicated_executor
            .spawn_io(async move { store.get_opts(&location, options).await })
            .await?;

        // As the results can be a stream too, we must wrap that too
        results.payload = self.wrap_if_necessary(results.payload);
        Ok(results)
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from = from.clone();
        let to = to.clone();
        let store = Arc::clone(&self.inner);
        self.dedicated_executor
            .spawn_io(async move { store.copy(&from, &to).await })
            .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from = from.clone();
        let to = to.clone();
        let store = Arc::clone(&self.inner);
        self.dedicated_executor
            .spawn_io(async move { store.copy(&from, &to).await })
            .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        self.dedicated_executor
            .spawn_io(async move { store.delete(&location).await })
            .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        // run the inner list on the dedicated executor
        //
        // This requires some fiddling as we can't pass the result of list
        // across the runtime, so start another task that drives list on the IO
        // thread and sends results back to the executor thread.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let prefix_captured = prefix.cloned();
        let inner_captured = Arc::clone(&self.inner);
        let executor_captured = self.dedicated_executor.clone();
        let mut set = JoinSet::new();
        set.spawn(async move {
            executor_captured
                .spawn_io(async move {
                    // run and buffer on the IO stream
                    let mut list_stream = inner_captured.list(prefix_captured.as_ref());
                    while let Some(result) = list_stream.next().await {
                        // try to send to the sender, if error means the
                        // receiver has been closed and we terminate early
                        if tx.send(result).await.is_err() {
                            return;
                        }
                    }
                })
                .await;
        });

        StreamAndTask {
            inner: ReceiverStream::new(rx),
            set,
        }
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned();
        let store = Arc::clone(&self.inner);
        self.dedicated_executor
            .spawn_io(async move { store.list_with_delimiter(prefix.as_ref()).await })
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        let result = self
            .dedicated_executor
            .spawn_io(async move { store.put_multipart_opts(&location, opts).await })
            .await?;

        // the resulting object has async functions too which we must wrap
        Ok(Box::new(IoMultipartUpload::new(
            self.dedicated_executor.clone(),
            result,
        )))
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        self.dedicated_executor
            .spawn_io(async move { store.put_opts(&location, payload, opts).await })
            .await
    }
}

/// A wrapper around a `MultipartUpload` that runs the async functions on the
/// IO thread of the DedicatedExecutor
#[derive(Debug)]
struct IoMultipartUpload {
    dedicated_executor: DedicatedExecutor,
    /// Inner upload (needs to be an option so we can send in closure)
    inner: Option<Box<dyn MultipartUpload>>,
}
impl IoMultipartUpload {
    fn new(dedicated_executor: DedicatedExecutor, inner: Box<dyn MultipartUpload>) -> Self {
        Self {
            dedicated_executor,
            inner: Some(inner),
        }
    }
}

#[async_trait]
impl MultipartUpload for IoMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner
            .as_mut()
            .expect("paths that take put inner back")
            .put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        // because we are running the task on a different runtime, we
        // can't send references to &self. Thus take out of self.inner
        // to run on io thread
        let mut inner = self.inner.take().expect("paths that take put inner back");

        let (inner, result) = self
            .dedicated_executor
            .spawn_io(async move {
                let result = inner.as_mut().complete().await;
                (inner, result)
            })
            .await;
        self.inner = Some(inner);
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let mut inner = self.inner.take().expect("paths that take put inner back");

        let (inner, result) = self
            .dedicated_executor
            .spawn_io(async move {
                let result = inner.as_mut().abort().await;
                (inner, result)
            })
            .await;
        self.inner = Some(inner);
        result
    }
}
