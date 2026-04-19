use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use crate::model::{Entry, SortMode};

pub fn is_hidden(name: &str) -> bool {
    name.starts_with('.')
}

pub fn read_entries(dir: &Path, show_hidden: bool) -> Result<Vec<Entry>> {
    let mut items = Vec::new();
    for item in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let item = item?;
        let path = item.path();
        let name = item.file_name().to_string_lossy().to_string();
        if !show_hidden && is_hidden(&name) {
            continue;
        }
        let meta = item.metadata().ok();
        let is_dir = meta.as_ref().is_some_and(|m| m.is_dir());
        let size = meta.as_ref().map(|m| m.len()).unwrap_or(0);
        items.push(Entry {
            name,
            path,
            is_dir,
            size,
        });
    }
    Ok(items)
}

pub fn sort_entries(entries: &mut [Entry], mode: SortMode) {
    entries.sort_by(|a, b| {
        let dirs_first = match (a.is_dir, b.is_dir) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        };
        if dirs_first != std::cmp::Ordering::Equal {
            return dirs_first;
        }
        match mode {
            SortMode::NameAsc => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
            SortMode::NameDesc => b.name.to_lowercase().cmp(&a.name.to_lowercase()),
            SortMode::SizeAsc => a
                .size
                .cmp(&b.size)
                .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase())),
            SortMode::SizeDesc => b
                .size
                .cmp(&a.size)
                .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase())),
        }
    });
}

pub fn format_size(size: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut sizef = size as f64;
    let mut unit = 0;
    while sizef >= 1024.0 && unit < UNITS.len() - 1 {
        sizef /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", size, UNITS[unit])
    } else {
        format!("{sizef:.1} {}", UNITS[unit])
    }
}

pub fn resolve_destination(cwd: &Path, raw: &str, src: &Path) -> PathBuf {
    let mut dst = PathBuf::from(raw);
    if dst.is_relative() {
        dst = cwd.join(dst);
    }
    if dst.is_dir() {
        dst.join(src.file_name().unwrap_or_default())
    } else {
        dst
    }
}

pub fn copy_path(src: &Path, dst: &Path) -> Result<()> {
    if src.is_dir() {
        copy_dir_all(src, dst)
    } else {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(src, dst)?;
        Ok(())
    }
}

pub fn copy_dir_all(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_all(&from, &to)?;
        } else {
            if let Some(parent) = to.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&from, &to)?;
        }
    }
    Ok(())
}
