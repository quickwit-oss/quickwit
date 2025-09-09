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

use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;

use crate::{StorageError, StorageErrorKind};

impl<E> From<SdkError<E>> for StorageError
where E: std::error::Error + ToStorageErrorKind + Send + Sync + 'static
{
    fn from(error: SdkError<E>) -> StorageError {
        let error_kind = match &error {
            SdkError::ConstructionFailure(_) => StorageErrorKind::Internal,
            SdkError::DispatchFailure(failure) => {
                if failure.is_io() {
                    StorageErrorKind::Io
                } else if failure.is_timeout() {
                    StorageErrorKind::Timeout
                } else {
                    StorageErrorKind::Internal
                }
            }
            SdkError::ResponseError(response_error) => {
                match response_error.raw().status().as_u16() {
                    404 /* NOT_FOUND */ => StorageErrorKind::NotFound,
                    403 /* UNAUTHORIZED */ => StorageErrorKind::Unauthorized,
                    _ => StorageErrorKind::Internal,
                }
            }
            SdkError::ServiceError(service_error) => service_error.err().to_storage_error_kind(),
            SdkError::TimeoutError(_) => StorageErrorKind::Timeout,
            _ => StorageErrorKind::Internal,
        };
        let source = if let SdkError::DispatchFailure(failure) = &error {
            // For DNS lookup failures, try to extract the hostname from the error
            let error_str = format!("{}", DisplayErrorContext(&error));
            if error_str.contains("dns error") && error_str.contains("failed to lookup address information") {
                // Try to extract hostname from the error context
                if let Some(hostname) = extract_hostname_from_dispatch_failure(&failure) {
                    anyhow::anyhow!("dns error: failed to lookup address information for hostname '{}': {}", hostname, error_str)
                } else {
                    anyhow::anyhow!("dns error (hostname not extractable): {}", error_str)
                }
            } else {
                anyhow::anyhow!("{}", DisplayErrorContext(error))
            }
        } else {
            anyhow::anyhow!("{}", DisplayErrorContext(error))
        };
        error_kind.with_error(source)
    }
}

pub trait ToStorageErrorKind {
    fn to_storage_error_kind(&self) -> StorageErrorKind;
}

impl ToStorageErrorKind for GetObjectError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        let error_code = self.code().unwrap_or("unknown");
        crate::STORAGE_METRICS
            .object_storage_get_errors_total
            .with_label_values([error_code])
            .inc();
        match self {
            GetObjectError::InvalidObjectState(_) => StorageErrorKind::Service,
            GetObjectError::NoSuchKey(_) => StorageErrorKind::NotFound,
            _ => StorageErrorKind::Service,
        }
    }
}

impl ToStorageErrorKind for DeleteObjectError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for DeleteObjectsError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for UploadPartError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for CompleteMultipartUploadError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for AbortMultipartUploadError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        match self {
            AbortMultipartUploadError::NoSuchUpload(_) => StorageErrorKind::Internal,
            _ => StorageErrorKind::Service,
        }
    }
}

impl ToStorageErrorKind for CreateMultipartUploadError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for PutObjectError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        StorageErrorKind::Service
    }
}

impl ToStorageErrorKind for HeadObjectError {
    fn to_storage_error_kind(&self) -> StorageErrorKind {
        match self {
            HeadObjectError::NotFound(_) => StorageErrorKind::NotFound,
            _ => StorageErrorKind::Service,
        }
    }
}

/// Attempts to extract the hostname that failed DNS resolution from a DispatchFailure.
/// This is a best-effort approach using pattern matching on the error string.
fn extract_hostname_from_dispatch_failure(failure: &impl std::fmt::Debug) -> Option<String> {
    // Convert the failure to string to extract hostname
    let error_string = format!("{:?}", failure);
    
    // Look for patterns that might contain the hostname
    // Common patterns in hyper/AWS SDK errors for DNS failures:
    // - "tcp connect error: dns error: failed to lookup address information"  
    // - Connection attempts often have the hostname embedded
    
    // Try to find hostname in various patterns
    if let Some(hostname) = extract_hostname_from_error_string(&error_string) {
        return Some(hostname);
    }
    
    // If we can't extract it from the error string, return None
    None
}

/// Extract hostname from error string patterns
fn extract_hostname_from_error_string(error_str: &str) -> Option<String> {
    // Pattern 1: Look for "Connect" followed by hostname
    if let Some(start) = error_str.find("Connect(") {
        let substr = &error_str[start + 8..];
        if let Some(end) = substr.find(')') {
            let hostname = substr[..end].trim_matches('"');
            if !hostname.is_empty() && hostname != "Unknown" {
                return Some(hostname.to_string());
            }
        }
    }
    
    // Pattern 2: Look for hostname in connection error messages
    if let Some(start) = error_str.find("connection: ") {
        let substr = &error_str[start + 12..];
        if let Some(end) = substr.find(' ') {
            let hostname = substr[..end].trim_matches('"');
            if !hostname.is_empty() && hostname != "Unknown" {
                return Some(hostname.to_string());
            }
        }
    }
    
    // Pattern 3: Look for "Host: " pattern
    if let Some(start) = error_str.find("Host: ") {
        let substr = &error_str[start + 6..];
        if let Some(end) = substr.find(&[' ', '\n', '\r', ',', ')'][..]) {
            let hostname = substr[..end].trim_matches('"');
            if !hostname.is_empty() {
                return Some(hostname.to_string());
            }
        }
    }
    
    // Pattern 4: Extract from URL-like patterns s3://bucket or https://endpoint
    if let Some(hostname) = extract_hostname_from_url_pattern(error_str) {
        return Some(hostname);
    }
    
    None
}

/// Extract hostname from URL patterns in error strings
fn extract_hostname_from_url_pattern(error_str: &str) -> Option<String> {
    // Look for https:// or http:// patterns
    for prefix in &["https://", "http://"] {
        if let Some(start) = error_str.find(prefix) {
            let url_start = start + prefix.len();
            let substr = &error_str[url_start..];
            
            // Find the end of hostname (before path or query)
            let hostname_end = substr.find(&['/', '?', ' ', '\n', '\r', ')', '"'][..])
                .unwrap_or(substr.len());
            
            let hostname = &substr[..hostname_end];
            if !hostname.is_empty() && !hostname.contains("Unknown") {
                return Some(hostname.to_string());
            }
        }
    }
    
    // Look for s3:// patterns
    if let Some(start) = error_str.find("s3://") {
        let url_start = start + 5;
        let substr = &error_str[url_start..];
        
        // Find the end of bucket name
        let bucket_end = substr.find(&['/', ' ', '\n', '\r', ')', '"'][..])
            .unwrap_or(substr.len());
        
        let bucket = &substr[..bucket_end];
        if !bucket.is_empty() {
            // For s3://, we might want to show the bucket, but the actual hostname 
            // would be the S3 endpoint. Let's return the s3:// URL for context.
            return Some(format!("s3://{}", bucket));
        }
    }
    
    None
}
