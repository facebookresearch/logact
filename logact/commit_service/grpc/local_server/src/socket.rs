/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs;
use std::os::unix::fs::FileTypeExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use anyhow::Context as _;
use anyhow::Result;
use tokio::net::UnixListener;
use tokio::net::UnixStream;

use crate::private_fs::ensure_private_parent_directory;

/// Bind a private Unix-domain socket, rejecting live or unexpected paths.
pub(crate) async fn bind_socket(path: impl AsRef<Path>) -> Result<UnixListener> {
    let path = path.as_ref();
    ensure_private_parent_directory(path).await?;

    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if !metadata.file_type().is_socket() => {
            anyhow::bail!("refusing to replace non-socket path {}", path.display());
        }
        Ok(_) => match UnixStream::connect(path).await {
            Ok(_) => anyhow::bail!("LogAct daemon is already serving at {}", path.display()),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                tokio::fs::remove_file(path)
                    .await
                    .with_context(|| format!("failed to remove stale socket {}", path.display()))?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "could not verify whether socket {} is stale",
                        path.display()
                    )
                });
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect socket {}", path.display()));
        }
    }

    // The verified 0700 parent prevents access during the bind/chmod sequence.
    let listener = UnixListener::bind(path)
        .with_context(|| format!("failed to bind socket {}", path.display()))?;
    if let Err(error) = tokio::fs::set_permissions(path, fs::Permissions::from_mode(0o600)).await {
        drop(listener);
        let cleanup_error = tokio::fs::remove_file(path).await.err();
        let mut error = anyhow::Error::new(error)
            .context(format!("failed to restrict socket {}", path.display()));
        if let Some(cleanup_error) = cleanup_error {
            error = error.context(format!(
                "also failed to remove socket {}: {cleanup_error}",
                path.display()
            ));
        }
        return Err(error);
    }
    Ok(listener)
}
