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

use std::borrow::Cow;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Instant;

use pin_project::{pin_project, pinned_drop};
use tokio::io::{AsyncBufRead, AsyncWrite};

use crate::STORAGE_METRICS;

pub enum Status {
    Pending,
    #[allow(dead_code)]
    Done,
    Ready(String),
}

/// Converts an object store client SDK Result<> to the [Status] that should be
/// recorded in the metrics.
///
/// The `Marker` type is necessary to avoid conflicting implementations of the
/// trait.
pub trait AsStatus<Marker> {
    fn as_status(&self) -> Status;
}

/// Wrapper around object store requests to record metrics, including cancellation.
#[pin_project(PinnedDrop)]
pub struct RequestMetricsWrapper<F, Marker>
where
    F: Future,
    F::Output: AsStatus<Marker>,
{
    #[pin]
    tracked: F,
    action: &'static str,
    start: Option<Instant>,
    uploaded_bytes: Option<u64>,
    status: Status,
    _marker: PhantomData<Marker>,
}

#[pinned_drop]
impl<F, Marker> PinnedDrop for RequestMetricsWrapper<F, Marker>
where
    F: Future,
    F::Output: AsStatus<Marker>,
{
    fn drop(self: Pin<&mut Self>) {
        let status = match &self.status {
            Status::Pending => "cancelled",
            Status::Done => return,
            Status::Ready(s) => s.as_str(),
        };
        let label_values = [self.action, status];
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
                .inc_by(bytes);
        }
    }
}

impl<F, Marker> Future for RequestMetricsWrapper<F, Marker>
where
    F: Future,
    F::Output: AsStatus<Marker>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.tracked.poll(cx));
        *this.status = response.as_status();

        Poll::Ready(response)
    }
}

pub trait S3MetricsWrapperExt<F, Marker>
where
    F: Future,
    F::Output: AsStatus<Marker>,
{
    fn with_count_metric(self, action: &'static str) -> RequestMetricsWrapper<F, Marker>;

    fn with_count_and_duration_metrics(
        self,
        action: &'static str,
    ) -> RequestMetricsWrapper<F, Marker>;

    fn with_count_and_upload_metrics(
        self,
        action: &'static str,
        bytes: u64,
    ) -> RequestMetricsWrapper<F, Marker>;
}

impl<F, Marker> S3MetricsWrapperExt<F, Marker> for F
where
    F: Future,
    F::Output: AsStatus<Marker>,
{
    fn with_count_metric(self, action: &'static str) -> RequestMetricsWrapper<F, Marker> {
        RequestMetricsWrapper {
            tracked: self,
            action,
            status: Status::Pending,
            start: None,
            uploaded_bytes: None,
            _marker: PhantomData,
        }
    }

    fn with_count_and_duration_metrics(
        self,
        action: &'static str,
    ) -> RequestMetricsWrapper<F, Marker> {
        RequestMetricsWrapper {
            tracked: self,
            action,
            status: Status::Pending,
            start: Some(Instant::now()),
            uploaded_bytes: None,
            _marker: PhantomData,
        }
    }

    fn with_count_and_upload_metrics(
        self,
        action: &'static str,
        bytes: u64,
    ) -> RequestMetricsWrapper<F, Marker> {
        RequestMetricsWrapper {
            tracked: self,
            action,
            status: Status::Pending,
            start: None,
            uploaded_bytes: Some(bytes),
            _marker: PhantomData,
        }
    }
}

pub struct S3Marker;

impl<R, E> AsStatus<S3Marker> for Result<R, E>
where E: aws_sdk_s3::error::ProvideErrorMetadata
{
    fn as_status(&self) -> Status {
        let status_str = match self {
            Ok(_) => "success".to_string(),
            Err(e) => e.meta().code().unwrap_or("unknown").to_string(),
        };
        Status::Ready(status_str)
    }
}

#[cfg(feature = "azure")]
pub struct AzureMarker;

#[cfg(feature = "azure")]
impl<R> AsStatus<AzureMarker> for Result<R, azure_storage::Error> {
    fn as_status(&self) -> Status {
        let Err(err) = self else {
            return Status::Ready("success".to_string());
        };
        let err_status_str = match err.kind() {
            azure_storage::ErrorKind::HttpResponse { status, .. } => status.to_string(),
            azure_storage::ErrorKind::Credential => "credential".to_string(),
            azure_storage::ErrorKind::Io => "io".to_string(),
            azure_storage::ErrorKind::DataConversion => "data_conversion".to_string(),
            _ => "unknown".to_string(),
        };
        Status::Ready(err_status_str)
    }
}

// The Azure SDK get_blob request returns Option<Result> because it chunks
// the download into a stream of get requests.
#[cfg(feature = "azure")]
impl<R> AsStatus<AzureMarker> for Option<Result<R, azure_storage::Error>> {
    fn as_status(&self) -> Status {
        match self {
            None => Status::Done,
            Some(res) => res.as_status(),
        }
    }
}

/// Track io errors during downloads.
///
/// Downloads are a bit different from other requests because the request might
/// fail while getting the bytes from the response body, long after getting a
/// successful response header.
#[pin_project(PinnedDrop)]
struct DownloadMetricsWrapper<F>
where F: Future<Output = std::io::Result<u64>>
{
    #[pin]
    tracked: F,
    result: Option<Result<u64, io::ErrorKind>>,
}

#[pinned_drop]
impl<F> PinnedDrop for DownloadMetricsWrapper<F>
where F: Future<Output = io::Result<u64>>
{
    fn drop(self: Pin<&mut Self>) {
        let status = match &self.result {
            None => Cow::Borrowed("cancelled"),
            Some(Err(e)) => Cow::Owned(format!("{e:?}")),
            Some(Ok(downloaded_bytes)) => {
                STORAGE_METRICS
                    .object_storage_download_num_bytes
                    .inc_by(*downloaded_bytes);
                return;
            }
        };
        STORAGE_METRICS
            .object_storage_download_errors
            .with_label_values([status.as_ref()])
            .inc();
    }
}

impl<F> Future for DownloadMetricsWrapper<F>
where F: Future<Output = io::Result<u64>>
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = ready!(this.tracked.poll(cx));
        *this.result = match &response {
            Ok(s) => Some(Ok(*s)),
            Err(e) => Some(Err(e.kind())),
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
        tracked: tokio::io::copy_buf(reader, writer),
        result: None,
    }
    .await
}
