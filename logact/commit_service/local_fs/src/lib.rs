/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ffi::OsString;
use std::fs::File;
use std::fs::TryLockError;
use std::os::unix::fs::FileTypeExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context as _;
use anyhow::Result;
use anyhow::bail;
use anyhow::ensure;
use tokio::net::UnixStream;

/// Exclusive ownership of a local daemon's Unix-domain socket.
///
/// The operating system releases the lock when this value is dropped or its
/// process exits. The lock file itself deliberately remains on disk.
#[derive(Debug)]
#[must_use = "dropping this value releases the daemon singleton lock"]
pub struct PrivateUnixSocketLock {
    _file: File,
}

/// Create or validate the private parent directory for a local state file.
pub async fn ensure_private_parent_directory(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .context("path must include a parent directory")?;
    match tokio::fs::symlink_metadata(parent).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = tokio::fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(parent).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to create private directory {}; its parent must already exist",
                            parent.display()
                        )
                    });
                }
            }
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect directory {}", parent.display()));
        }
    }

    let metadata = tokio::fs::symlink_metadata(parent)
        .await
        .with_context(|| format!("failed to inspect directory {}", parent.display()))?;
    ensure!(
        !metadata.file_type().is_symlink(),
        "private directory {} must not be a symlink",
        parent.display()
    );
    ensure!(
        metadata.is_dir(),
        "private path {} is not a directory",
        parent.display()
    );
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "private directory {} must not be accessible by group or other users",
        parent.display()
    );
    Ok(())
}

/// Lock and prepare a private Unix-domain socket path for binding.
///
/// A live listener and unexpected filesystem entries are preserved. A stale
/// socket left by an unclean shutdown is removed so a service manager can
/// restart the daemon without manual cleanup. The returned lock must remain
/// alive for as long as the server owns the socket.
pub async fn prepare_private_unix_socket(path: &Path) -> Result<PrivateUnixSocketLock> {
    ensure_private_parent_directory(path).await?;

    let lock_path = socket_lock_path(path);
    let lock_file = tokio::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .await
        .with_context(|| format!("failed to open daemon lock {}", lock_path.display()))?
        .into_std()
        .await;
    match lock_file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            bail!("another service owns daemon lock {}", lock_path.display());
        }
        Err(TryLockError::Error(error)) => {
            return Err(error)
                .with_context(|| format!("failed to acquire daemon lock {}", lock_path.display()));
        }
    }
    let socket_lock = PrivateUnixSocketLock { _file: lock_file };

    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if !metadata.file_type().is_socket() => {
            bail!("refusing to replace non-socket path {}", path.display());
        }
        Ok(_) => match UnixStream::connect(path).await {
            Ok(_) => bail!("a service is already listening at {}", path.display()),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                tokio::fs::remove_file(path)
                    .await
                    .with_context(|| format!("failed to remove stale socket {}", path.display()))?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "could not determine whether socket {} is stale",
                        path.display()
                    )
                });
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect socket path {}", path.display()));
        }
    }

    Ok(socket_lock)
}

fn socket_lock_path(socket: &Path) -> PathBuf {
    let mut path = OsString::from(socket.as_os_str());
    path.push(".lock");
    path.into()
}
