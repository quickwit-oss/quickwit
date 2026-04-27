// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Instant;

use pin_project::{pin_project, pinned_drop};
use tokio::io::{AsyncBufRead, AsyncWrite};

use crate::STORAGE_METRICS;

#[derive(Clone, Copy, Debug)]
pub enum ActionLabel {
    AbortMultipartUpload,
    CompleteMultipartUpload,
    CreateMultipartUpload,
    DeleteObject,
    DeleteObjects,
    GetObject,
    HeadObject,
    ListObjects,
    PutObject,
    UploadPart,
}

impl ActionLabel {
    fn as_str(&self) -> &'static str {
        match self {
            ActionLabel::AbortMultipartUpload => "abort_multipart_upload",
            ActionLabel::CompleteMultipartUpload => "complete_multipart_upload",
            ActionLabel::CreateMultipartUpload => "create_multipart_upload",
            ActionLabel::DeleteObject => "delete_object",
            ActionLabel::DeleteObjects => "delete_objects",
            ActionLabel::GetObject => "get_object",
            ActionLabel::HeadObject => "head_object",
            ActionLabel::ListObjects => "list_objects",
            ActionLabel::PutObject => "put_object",
            ActionLabel::UploadPart => "upload_part",
        }
    }
}

pub enum RequestStatus {
    Pending,
    // only useful on feature="azure"
    #[allow(dead_code)]
    Done,
    Ready(String),
}

/// Converts an object store client SDK Result<> to the [Status] that should be
/// recorded in the metrics.
///
/// The `Marker` type is necessary to avoid conflicting implementations of the
/// trait.
pub trait AsRequestStatus<Marker> {
    fn as_status(&self) -> RequestStatus;
}

/// Wrapper around object store requests to record metrics.
///
/// For download requests with large response bodies, this wrapper tracks the
/// time to first byte (TTFB) by wrapping the initial request future, not the
/// response body stream copy operation.
///
/// This also records cancellations that occur when a timeout is applied to the
/// request future.
#[pin_project(PinnedDrop)]
pub struct RequestMetricsWrapper<F, Marker>
where
    F: Future,
    F::Output: AsRequestStatus<Marker>,
{
    #[pin]
    tracked: F,
    action: ActionLabel,
    start: Option<Instant>,
    uploaded_bytes: Option<u64>,
    status: RequestStatus,
    _marker: PhantomData<Marker>,
}

#[pinned_drop]
impl<F, Marker> PinnedDrop for RequestMetricsWrapper<F, Marker>
where
    F: Future,
    F::Output: AsRequestStatus<Marker>,
{
    fn drop(self: Pin<&mut Self>) {
        let status = match &self.status {
            RequestStatus::Pending => "cancelled",
            RequestStatus::Done => return,
            RequestStatus::Ready(s) => s.as_str(),
        };
        let label_values = [self.action.as_str(), status];
        STORAGE_METRICS
            .object_storage_requests_total
            .with_label_values(label_values)
            .inc();
        if let Some(start) = self.start {
            STORAGE_METRICS
                .object_storage_request_duration
                .with_label_values(label_values)
                .observe(start.elapsed().as_secs_f64());
        }
        if let Some(bytes) = self.uploaded_bytes {
            STORAGE_METRICS
                .object_storage_upload_num_bytes
                .with_label_values([status])
                .inc_by(bytes);
        }
    }
}

impl<F, Marker> Future for RequestMetricsWrapper<F, Marker>
where
    F: Future,
    F::Output: AsRequestStatus<Marker>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.tracked.poll(cx));
        *this.status = response.as_status();

        Poll::Ready(response)
    }
}

pub trait RequestMetricsWrapperExt<F, Marker>
where
    F: Future,
    F::Output: AsRequestStatus<Marker>,
{
    fn with_count_metric(self, action: ActionLabel) -> RequestMetricsWrapper<F, Marker>;

    /// For download requests, this should wrap the initial request future to
    /// track time to first byte (TTFB).
    fn with_count_and_duration_metrics(
        self,
        action: ActionLabel,
    ) -> RequestMetricsWrapper<F, Marker>;

    fn with_count_and_upload_metrics(
        self,
        action: ActionLabel,
        bytes: u64,
    ) -> RequestMetricsWrapper<F, Marker>;
}

impl<F, Marker> RequestMetricsWrapperExt<F, Marker> for F
where
    F: Future,
    F::Output: AsRequestStatus<Marker>,
{
    fn with_count_metric(self, action: ActionLabel) -> RequestMetricsWrapper<F, Marker> {
        RequestMetricsWrapper {
            tracked: self,
            action,
            status: RequestStatus::Pending,
            start: None,
            uploaded_bytes: None,
            _marker: PhantomData,
        }
    }

    fn with_count_and_duration_metrics(
        self,
        action: ActionLabel,
    ) -> RequestMetricsWrapper<F, Marker> {
        RequestMetricsWrapper {
            tracked: self,
            action,
            status: RequestStatus::Pending,
            start: Some(Instant::now()),
            uploaded_bytes: None,
            _marker: PhantomData,
        }
    }

    fn with_count_and_upload_metrics(
        self,
        action: ActionLabel,
        bytes: u64,
    ) -> RequestMetricsWrapper<F, Marker> {
        RequestMetricsWrapper {
            tracked: self,
            action,
            status: RequestStatus::Pending,
            start: None,
            uploaded_bytes: Some(bytes),
            _marker: PhantomData,
        }
    }
}

mod s3_impls {
    use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};

    use super::{AsRequestStatus, RequestStatus};

    pub struct S3Marker;

    impl<R, E: ProvideErrorMetadata> AsRequestStatus<S3Marker> for Result<R, SdkError<E>> {
        fn as_status(&self) -> RequestStatus {
            let status_str = match self {
                Ok(_) => "success".to_string(),
                Err(SdkError::ConstructionFailure(_)) => "construction_failure".to_string(),
                Err(SdkError::TimeoutError(_)) => "timeout_error".to_string(),
                Err(SdkError::DispatchFailure(_)) => "dispatch_failure".to_string(),
                Err(SdkError::ResponseError(_)) => "response_error".to_string(),
                Err(e @ SdkError::ServiceError(_)) => e
                    .meta()
                    .code()
                    .unwrap_or("unknown_service_error")
                    .to_string(),
                Err(_) => "unknown".to_string(),
            };
            RequestStatus::Ready(status_str)
        }
    }
}

#[cfg(feature = "azure")]
mod azure_impl {
    use super::{AsRequestStatus, RequestStatus};

    pub struct AzureMarker;

    impl<R> AsRequestStatus<AzureMarker> for Result<R, azure_storage::Error> {
        fn as_status(&self) -> RequestStatus {
            let Err(err) = self else {
                return RequestStatus::Ready("success".to_string());
            };
            let err_status_str = match err.kind() {
                azure_storage::ErrorKind::HttpResponse { status, .. } => status.to_string(),
                azure_storage::ErrorKind::Credential => "credential".to_string(),
                azure_storage::ErrorKind::Io => "io".to_string(),
                azure_storage::ErrorKind::DataConversion => "data_conversion".to_string(),
                _ => "unknown".to_string(),
            };
            RequestStatus::Ready(err_status_str)
        }
    }

    // The Azure SDK get_blob request returns Option<Result> because it chunks
    // the download into a stream of get requests.
    impl<R> AsRequestStatus<AzureMarker> for Option<Result<R, azure_storage::Error>> {
        fn as_status(&self) -> RequestStatus {
            match self {
                None => RequestStatus::Done,
                Some(res) => res.as_status(),
            }
        }
    }
}

pub enum DownloadStatus {
    InProgress,
    Done,
    Failed(&'static str),
}

/// Track io errors during downloads.
///
/// Downloads are a bit different from other requests because the request might
/// fail while getting the bytes from the response body, long after getting a
/// successful response header.
#[pin_project(PinnedDrop)]
struct DownloadMetricsWrapper<'a, R, W>
where
    R: AsyncBufRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    #[pin]
    tracked: copy_buf::CopyBuf<'a, R, W>,
    status: DownloadStatus,
}

#[pinned_drop]
impl<'a, R, W> PinnedDrop for DownloadMetricsWrapper<'a, R, W>
where
    R: AsyncBufRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    fn drop(self: Pin<&mut Self>) {
        let error_opt = match &self.status {
            DownloadStatus::InProgress => Some("cancelled"),
            DownloadStatus::Failed(e) => Some(*e),
            DownloadStatus::Done => None,
        };

        STORAGE_METRICS
            .object_storage_download_num_bytes
            .with_label_values([error_opt.unwrap_or("success")])
            .inc_by(self.tracked.amt);

        if let Some(error) = error_opt {
            STORAGE_METRICS
                .object_storage_download_errors
                .with_label_values([error])
                .inc();
        }
    }
}

impl<'a, R, W> Future for DownloadMetricsWrapper<'a, R, W>
where
    R: AsyncBufRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    type Output = io::Result<u64>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.tracked.poll(cx));
        *this.status = match &response {
            Ok(_) => DownloadStatus::Done,
            Err(e) => DownloadStatus::Failed(io_error_as_label(e.kind())),
        };
        Poll::Ready(response)
    }
}

pub async fn copy_with_download_metrics<'a, R, W>(
    reader: &'a mut R,
    writer: &'a mut W,
) -> io::Result<u64>
where
    R: AsyncBufRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    DownloadMetricsWrapper {
        tracked: copy_buf::CopyBuf {
            reader,
            writer,
            amt: 0,
        },
        status: DownloadStatus::InProgress,
    }
    .await
}

/// This is a fork of `tokio::io::copy_buf` that enables tracking the number of
/// bytes transferred. This estimate should be accurate as long as the network
/// is the bottleneck.
mod copy_buf {

    use std::future::Future;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll, ready};

    use tokio::io::{AsyncBufRead, AsyncWrite};

    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct CopyBuf<'a, R: ?Sized, W: ?Sized> {
        pub reader: &'a mut R,
        pub writer: &'a mut W,
        pub amt: u64,
    }

    impl<R, W> Future for CopyBuf<'_, R, W>
    where
        R: AsyncBufRead + Unpin + ?Sized,
        W: AsyncWrite + Unpin + ?Sized,
    {
        type Output = io::Result<u64>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            loop {
                let me = &mut *self;
                let buffer = ready!(Pin::new(&mut *me.reader).poll_fill_buf(cx))?;
                if buffer.is_empty() {
                    ready!(Pin::new(&mut self.writer).poll_flush(cx))?;
                    return Poll::Ready(Ok(self.amt));
                }

                let i = ready!(Pin::new(&mut *me.writer).poll_write(cx, buffer))?;
                if i == 0 {
                    return Poll::Ready(Err(std::io::ErrorKind::WriteZero.into()));
                }
                self.amt += i as u64;
                Pin::new(&mut *self.reader).consume(i);
            }
        }
    }
}

fn io_error_as_label(error: io::ErrorKind) -> &'static str {
    use io::ErrorKind::*;
    // most of these variants are not expected to happen
    match error {
        AddrInUse => "addr_in_use",
        AddrNotAvailable => "addr_not_available",
        AlreadyExists => "already_exists",
        ArgumentListTooLong => "argument_list_too_long",
        BrokenPipe => "broken_pipe",
        ConnectionAborted => "connection_aborted",
        ConnectionRefused => "connection_refused",
        ConnectionReset => "connection_reset",
        CrossesDevices => "crosses_devices",
        Deadlock => "deadlock",
        DirectoryNotEmpty => "directory_not_empty",
        ExecutableFileBusy => "executable_file_busy",
        FileTooLarge => "file_too_large",
        HostUnreachable => "host_unreachable",
        Interrupted => "interrupted",
        InvalidData => "invalid_data",
        InvalidFilename => "invalid_filename",
        InvalidInput => "invalid_input",
        IsADirectory => "is_a_directory",
        NetworkDown => "network_down",
        NetworkUnreachable => "network_unreachable",
        NotADirectory => "not_a_directory",
        NotConnected => "not_connected",
        NotFound => "not_found",
        NotSeekable => "not_seekable",
        Other => "other",
        OutOfMemory => "out_of_memory",
        PermissionDenied => "permission_denied",
        QuotaExceeded => "quota_exceeded",
        ReadOnlyFilesystem => "read_only_filesystem",
        ResourceBusy => "resource_busy",
        StaleNetworkFileHandle => "stale_network_file_handle",
        StorageFull => "storage_full",
        TimedOut => "timed_out",
        TooManyLinks => "too_many_links",
        UnexpectedEof => "unexpected_eof",
        Unsupported => "unsupported",
        WouldBlock => "would_block",
        WriteZero => "write_zero",
        _ => "uncategorized",
    }
}

#[cfg(feature = "gcs")]
pub mod opendal_helpers {
    use quickwit_common::metrics::HistogramTimer;

    use super::*;

    /// Records a request occurrence for this action with unknown status.
    pub fn record_request(action: ActionLabel) {
        STORAGE_METRICS
            .object_storage_requests_total
            .with_label_values([action.as_str(), "unknown"])
            .inc();
    }

    /// Records an upload volume for this action with unknown status.
    pub fn record_upload(bytes: u64) {
        STORAGE_METRICS
            .object_storage_upload_num_bytes
            .with_label_values(["unknown"])
            .inc_by(bytes);
    }

    /// Records an download volume for this action with unknown status.
    pub fn record_download(bytes: u64) {
        STORAGE_METRICS
            .object_storage_download_num_bytes
            .with_label_values(["unknown"])
            .inc_by(bytes);
    }

    /// Records a request occurrence for this action with unknown status.
    pub fn record_request_with_timer(action: ActionLabel) -> HistogramTimer {
        record_request(action);
        STORAGE_METRICS
            .object_storage_request_duration
            .with_label_values([action.as_str(), "unknown"])
            .start_timer()
    }
}
