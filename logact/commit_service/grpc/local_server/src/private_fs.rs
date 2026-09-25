/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use anyhow::Context as _;
use anyhow::Result;
use anyhow::ensure;

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
