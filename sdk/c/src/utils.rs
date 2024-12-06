use std::{collections::VecDeque, path::Path, sync::Arc, time::Duration};

use datenlord::fs::fs_util::INum;
use datenlord::{
    common::error::{DatenLordError, DatenLordResult},
    fs::{
        datenlordfs::{direntry::FileType, DatenLordFs, S3MetaData},
        fs_util::{FileAttr, ROOT_ID},
        virtualfs::VirtualFs,
    },
};
use nix::fcntl::OFlag;

/// A directory entry type.
pub struct Entry {
    pub name: String,
    pub ino: INum,
    pub file_type: FileType,
}

impl Entry {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn ino(&self) -> INum {
        self.ino
    }

    pub fn file_type(&self) -> String {
        match self.file_type {
            FileType::File => "file".to_string(),
            FileType::Dir => "dir".to_string(),
            FileType::Symlink => "symlink".to_string(),
        }
    }
}

/// Find the parent inode and attribute of the given path.
pub async fn find_parent_attr(
    path: &str,
    fs: Arc<DatenLordFs<S3MetaData>>,
) -> DatenLordResult<(Duration, FileAttr)> {
    let path = Path::new(path);

    // If the path is root, return root inode
    if path.parent().is_none() {
        return fs.getattr(ROOT_ID).await;
    }

    // Delete the last component to find the parent inode
    let parent_path = path.parent().ok_or(DatenLordError::ArgumentInvalid {
        context: vec!["Cannot find parent path".to_string()],
    })?;
    let parent_path_components = parent_path.components();

    // Find the file from parent inode
    let mut current_inode = ROOT_ID;
    for component in parent_path_components {
        if let Some(name) = component.as_os_str().to_str() {
            match fs.lookup(0, 0, current_inode, name).await {
                Ok((_duration, attr, _generation)) => {
                    current_inode = attr.ino;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        } else {
            return Err(DatenLordError::ArgumentInvalid {
                context: vec!["Invalid path component".to_string()],
            });
        }
    }
    fs.getattr(current_inode).await
}

// The current implementation searches for items and places them into a queue.
// It continues doing so until the subdirectory is found to be empty, at which point it deletes it.
// This method introduces some overhead due to repeated searches.
// An optimization could be applied to reduce the query overhead.
pub async fn recursive_delete_dir(
    fs: Arc<DatenLordFs<S3MetaData>>,
    dir_path: &str,
    recursive: bool,
) -> DatenLordResult<()> {
    let mut dir_stack = VecDeque::new();
    dir_stack.push_back(dir_path.to_string());

    while let Some(current_dir_path) = dir_stack.pop_front() {
        let (_, parent_attr) = find_parent_attr(&current_dir_path, fs.clone()).await?;
        let path = Path::new(&current_dir_path);

        let current_name = path
            .file_name()
            .ok_or(DatenLordError::ArgumentInvalid {
                context: vec!["Invalid file path".to_string()],
            })?
            .to_str()
            .ok_or(DatenLordError::ArgumentInvalid {
                context: vec!["Invalid file path".to_string()],
            })?;

        let (_, dir_attr, _) = fs.lookup(0, 0, parent_attr.ino, current_name).await?;
        let current_dir_ino = dir_attr.ino;

        // Open directory
        let dir_handle = fs
            .opendir(0, 0, current_dir_ino, OFlag::O_RDONLY.bits() as u32)
            .await?;

        // Read directory entries
        let entries = match fs.readdir(0, 0, current_dir_ino, dir_handle, 0).await {
            Ok(e) => e,
            Err(e) => {
                // Release the directory handle before returning the error
                let _ = fs.releasedir(current_dir_ino, dir_handle, 0).await;
                return Err(e);
            }
        };

        for entry in entries.iter() {
            let entry_path = Path::new(&current_dir_path).join(entry.name());

            if entry.file_type() == FileType::Dir {
                if recursive {
                    dir_stack.push_front(entry_path.to_string_lossy().to_string());
                }
            } else {
                fs.unlink(0, 0, current_dir_ino, &entry.name()).await?;
            }
        }

        // Always release the directory handle
        fs.releasedir(current_dir_ino, dir_handle, 0).await?;

        if recursive || entries.is_empty() {
            fs.rmdir(0, 0, parent_attr.ino, current_name).await?;
        }

        if !recursive {
            return Ok(());
        }
    }

    Ok(())
}
