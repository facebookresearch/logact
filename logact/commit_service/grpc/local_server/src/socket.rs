/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use anyhow::Context as _;
use anyhow::Result;
use logact_private_fs::PrivateUnixSocketLock;
use logact_private_fs::prepare_private_unix_socket;
use tokio::net::UnixListener;

/// Bind a private Unix-domain socket, rejecting live or unexpected paths.
pub(crate) async fn bind_socket(
    path: impl AsRef<Path>,
) -> Result<(UnixListener, PrivateUnixSocketLock)> {
    let path = path.as_ref();
    let socket_lock = prepare_private_unix_socket(path).await?;

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
    Ok((listener, socket_lock))
}
