use std::{fs, path::Path};

use image::{DynamicImage, ImageBuffer, RgbaImage, imageops::FilterType};
use ratatui::{
    style::{Color, Modifier, Style},
    text::{Line, Span},
};

use syntect::{
    easy::HighlightLines,
    highlighting::{Color as SynColor, Theme, ThemeSet},
    parsing::SyntaxSet,
    util::LinesWithEndings,
};

use crate::{
    fs_ops::format_size,
    model::{Entry, ImagePreviewMode, PreviewData},
};

const MAX_PREVIEW_BYTES: u64 = 512 * 1024;
const MAX_PREVIEW_LINES: usize = 400;
const MAX_PREVIEW_LINE_WIDTH: usize = 400;
pub const DEFAULT_PREVIEW_IMAGE_DIMENSION: u32 = 1280;

#[derive(Clone)]
pub struct PreparedImage {
    pub rgba: Vec<u8>,
    pub original_dimensions: (u32, u32),
    pub preview_dimensions: (u32, u32),
}

impl PreparedImage {
    pub fn to_dynamic_image(&self) -> anyhow::Result<DynamicImage> {
        let Some(buffer): Option<RgbaImage> = ImageBuffer::from_raw(
            self.preview_dimensions.0,
            self.preview_dimensions.1,
            self.rgba.clone(),
        ) else {
            anyhow::bail!("invalid preview buffer dimensions");
        };

        Ok(DynamicImage::ImageRgba8(buffer))
    }
}

pub struct Highlighter {
    ps: SyntaxSet,
    theme: Theme,
}

impl Highlighter {
    pub fn new() -> Self {
        let ps = SyntaxSet::load_defaults_newlines();
        let ts = ThemeSet::load_defaults();
        let theme = ts
            .themes
            .get("base16-ocean.dark")
            .cloned()
            .or_else(|| ts.themes.values().next().cloned())
            .unwrap_or_default();
        Self { ps, theme }
    }

    pub fn highlight_file(&self, path: &Path, text: &str) -> Vec<Line<'static>> {
        let syntax = self
            .ps
            .find_syntax_for_file(path)
            .ok()
            .flatten()
            .unwrap_or_else(|| self.ps.find_syntax_plain_text());

        let mut highlighter = HighlightLines::new(syntax, &self.theme);
        let mut out = Vec::new();

        for line in LinesWithEndings::from(text).take(MAX_PREVIEW_LINES) {
            let truncated = truncate_for_preview(line, MAX_PREVIEW_LINE_WIDTH);
            let ranges = highlighter
                .highlight_line(&truncated, &self.ps)
                .unwrap_or_default();

            let spans = ranges
                .into_iter()
                .map(|(style, part)| {
                    Span::styled(part.to_string(), syntect_to_style(style.foreground))
                })
                .collect::<Vec<_>>();

            out.push(Line::from(spans));
        }

        if out.is_empty() {
            out.push(Line::from(""));
        }

        out
    }
}

pub fn syntect_to_style(c: SynColor) -> Style {
    Style::default().fg(Color::Rgb(c.r, c.g, c.b))
}

pub fn truncate_for_preview(s: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if i >= max_chars {
            out.push('…');
            break;
        }
        out.push(ch);
    }
    out
}

pub fn is_image_path(path: &Path) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .map(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "png" | "jpg" | "jpeg" | "gif" | "bmp" | "webp"
            )
        })
        .unwrap_or(false)
}

pub fn decode_image(path: &Path) -> anyhow::Result<DynamicImage> {
    Ok(image::open(path)?)
}

pub fn read_image_dimensions(path: &Path) -> anyhow::Result<(u32, u32)> {
    Ok(image::image_dimensions(path)?)
}

pub fn clamp_image_dimensions(
    width: u32,
    height: u32,
    max_width: u32,
    max_height: u32,
) -> (u32, u32) {
    if width <= max_width && height <= max_height {
        return (width, height);
    }

    let scale = (max_width as f64 / width as f64).min(max_height as f64 / height as f64);
    let scaled_width = ((width as f64) * scale).round().max(1.0) as u32;
    let scaled_height = ((height as f64) * scale).round().max(1.0) as u32;

    (scaled_width, scaled_height)
}

pub fn prepare_image_for_preview(
    path: &Path,
    max_width: u32,
    max_height: u32,
) -> anyhow::Result<PreparedImage> {
    let original_dimensions = read_image_dimensions(path)?;
    let capped_width = max_width.max(1);
    let capped_height = max_height.max(1);
    let preview_dimensions = clamp_image_dimensions(
        original_dimensions.0,
        original_dimensions.1,
        capped_width,
        capped_height,
    );

    let mut image = decode_image(path)?;
    if preview_dimensions != original_dimensions {
        image = image.resize(
            preview_dimensions.0,
            preview_dimensions.1,
            FilterType::Triangle,
        );
    }

    let rgba = image.to_rgba8().into_raw();

    Ok(PreparedImage {
        rgba,
        original_dimensions,
        preview_dimensions,
    })
}

pub fn build_preview(
    entry: &Entry,
    highlighter: &Highlighter,
    image_mode: ImagePreviewMode,
) -> PreviewData {
    if entry.is_dir {
        let count = fs::read_dir(&entry.path)
            .ok()
            .map(|rd| rd.count())
            .unwrap_or(0);
        return PreviewData::new(vec![
            Line::from(Span::styled(
                "Directory",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(format!("name: {}", entry.name)),
            Line::from(format!("path: {}", entry.path.display())),
            Line::from(format!("children: {}", count)),
        ]);
    }

    let meta = match fs::metadata(&entry.path) {
        Ok(m) => m,
        Err(_) => {
            return PreviewData::new(vec![
                Line::from(Span::styled(
                    "File",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(""),
                Line::from(format!("name: {}", entry.name)),
                Line::from(format!("path: {}", entry.path.display())),
                Line::from("Unable to read metadata."),
            ]);
        }
    };

    if is_image_path(&entry.path) {
        let dimensions = read_image_dimensions(&entry.path).ok();

        return match image_mode {
            ImagePreviewMode::Image => {
                let mut lines = vec![
                    Line::from(Span::styled(
                        "Image Widget Mode",
                        Style::default()
                            .fg(Color::Magenta)
                            .add_modifier(Modifier::BOLD),
                    )),
                    Line::from(""),
                    Line::from(format!("name: {}", entry.name)),
                    Line::from(format!("path: {}", entry.path.display())),
                    Line::from(format!("size: {}", format_size(meta.len()))),
                ];

                if let Some((w, h)) = dimensions {
                    lines.push(Line::from(format!("original: {}x{}", w, h)));
                }

                lines.push(Line::from(format!(
                    "preview source adapts to pane size (fallback {} px)",
                    DEFAULT_PREVIEW_IMAGE_DIMENSION
                )));
                lines.push(Line::from("Rendering through ratatui-image thread mode."));
                lines.push(Line::from(
                    "Press [i] for file info mode, [o] to open/edit, [p] for protocol.",
                ));
                PreviewData::new(lines)
            }
            ImagePreviewMode::Info => {
                let mut lines = vec![
                    Line::from(Span::styled(
                        "Image Info",
                        Style::default()
                            .fg(Color::Magenta)
                            .add_modifier(Modifier::BOLD),
                    )),
                    Line::from(""),
                    Line::from(format!("name: {}", entry.name)),
                    Line::from(format!("path: {}", entry.path.display())),
                    Line::from(format!("size: {}", format_size(meta.len()))),
                ];

                match dimensions {
                    Some((w, h)) => {
                        lines.push(Line::from(format!("original: {}x{}", w, h)));
                        let (pw, ph) = clamp_image_dimensions(
                            w,
                            h,
                            DEFAULT_PREVIEW_IMAGE_DIMENSION,
                            DEFAULT_PREVIEW_IMAGE_DIMENSION,
                        );
                        if (pw, ph) != (w, h) {
                            lines.push(Line::from(format!(
                                "fallback preview source: {}x{}",
                                pw, ph
                            )));
                        }
                    }
                    None => lines.push(Line::from("dimensions: unavailable")),
                }

                lines.push(Line::from(""));
                lines.push(Line::from(
                    "Press [i] to go back to image mode, [o] for status, [p] for protocol.",
                ));
                PreviewData::new(lines)
            }
        };
    }

    if meta.len() > MAX_PREVIEW_BYTES {
        return PreviewData::new(vec![
            Line::from(Span::styled(
                "Large File",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(format!("name: {}", entry.name)),
            Line::from(format!("path: {}", entry.path.display())),
            Line::from(format!("size: {}", format_size(meta.len()))),
            Line::from(format!(
                "Preview skipped: over {}",
                format_size(MAX_PREVIEW_BYTES)
            )),
        ]);
    }

    let bytes = match fs::read(&entry.path) {
        Ok(b) => b,
        Err(_) => {
            return PreviewData::new(vec![
                Line::from(Span::styled(
                    "File",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(""),
                Line::from(format!("name: {}", entry.name)),
                Line::from(format!("path: {}", entry.path.display())),
                Line::from("Unable to read content."),
            ]);
        }
    };

    if bytes.contains(&0) {
        return PreviewData::new(vec![
            Line::from(Span::styled(
                "Binary File",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(format!("name: {}", entry.name)),
            Line::from(format!("path: {}", entry.path.display())),
            Line::from(format!("size: {}", format_size(meta.len()))),
        ]);
    }

    let text = match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => {
            return PreviewData::new(vec![
                Line::from(Span::styled(
                    "Non UTF-8 File",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                )),
                Line::from(""),
                Line::from(format!("name: {}", entry.name)),
                Line::from(format!("path: {}", entry.path.display())),
                Line::from(format!("size: {}", format_size(meta.len()))),
            ]);
        }
    };

    let mut lines = vec![
        Line::from(Span::styled(
            "Text Preview",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(format!("name: {}", entry.name)),
        Line::from(format!("path: {}", entry.path.display())),
        Line::from(format!("size: {}", format_size(meta.len()))),
        Line::from(""),
    ];

    lines.extend(highlighter.highlight_file(&entry.path, &text));
    PreviewData::new(lines)
}
