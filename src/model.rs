use std::{path::PathBuf, time::Instant};

use ratatui::{layout::Rect, text::Line};
use ratatui_image::thread::ThreadProtocol;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortMode {
    NameAsc,
    NameDesc,
    SizeAsc,
    SizeDesc,
}

impl SortMode {
    pub fn next(self) -> Self {
        match self {
            Self::NameAsc => Self::NameDesc,
            Self::NameDesc => Self::SizeAsc,
            Self::SizeAsc => Self::SizeDesc,
            Self::SizeDesc => Self::NameAsc,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::NameAsc => "name ↑",
            Self::NameDesc => "name ↓",
            Self::SizeAsc => "size ↑",
            Self::SizeDesc => "size ↓",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImagePreviewMode {
    Image,
    Info,
}

impl ImagePreviewMode {
    pub fn toggle(self) -> Self {
        match self {
            Self::Image => Self::Info,
            Self::Info => Self::Image,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Info => "info",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandMode {
    Normal,
    Search,
    Rename,
    NewFile,
    NewDir,
    Copy,
    Move,
    DeleteConfirm,
    GoTo,
    Grep,
}

impl CommandMode {
    pub fn prompt(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Search => "SEARCH /",
            Self::Rename => "RENAME",
            Self::NewFile => "NEW FILE",
            Self::NewDir => "NEW DIR",
            Self::Copy => "COPY TO",
            Self::Move => "MOVE TO",
            Self::DeleteConfirm => "DELETE",
            Self::GoTo => "GOTO",
            Self::Grep => "GREP",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub name: String,
    pub path: PathBuf,
    pub is_dir: bool,
    pub size: u64,
}

#[derive(Clone, Debug)]
pub struct HitBox {
    pub rect: Rect,
    pub target: PathBuf,
}

#[derive(Clone, Debug)]
pub struct PreviewData {
    pub lines: Vec<Line<'static>>,
    pub scroll_y: u16,
    pub scroll_x: u16,
    pub max_scroll_y: u16,
}

impl PreviewData {
    pub fn new(lines: Vec<Line<'static>>) -> Self {
        Self {
            lines,
            scroll_y: 0,
            scroll_x: 0,
            max_scroll_y: 0,
        }
    }
}

pub struct ImageRenderState {
    pub protocol: ThreadProtocol,
}

#[derive(Clone, Debug)]
pub struct LastClick(pub u16, pub u16, pub Instant);

#[derive(Clone, Debug)]
pub struct GrepResult {
    pub path: PathBuf,
    pub line_number: u64,
    pub line_content: String,
}
