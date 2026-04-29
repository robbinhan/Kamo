use std::cmp::min;

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Flex, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Clear, HighlightSpacing, List, ListItem, ListState, Paragraph, Wrap,
    },
};
use ratatui_image::StatefulImage;
use unicode_width::UnicodeWidthStr;

use crate::{
    app::{App, breadcrumb_segments, context_menu_rect},
    model::ContextAction,
    preview::{is_image_path, truncate_for_preview},
};

// ── Color palette ───────────────────────────────────────────────
const CLR_BG: Color = Color::Rgb(30, 30, 46);
const CLR_SURFACE: Color = Color::Rgb(49, 50, 68);
const CLR_TEXT: Color = Color::Rgb(205, 214, 244);
const CLR_SUBTEXT: Color = Color::Rgb(166, 173, 200);
const CLR_DIR: Color = Color::Rgb(137, 180, 250);
const CLR_FILE: Color = Color::Rgb(205, 214, 244);
const CLR_IMAGE: Color = Color::Rgb(203, 166, 247);
const CLR_ACCENT: Color = Color::Rgb(166, 227, 161);
const CLR_WARN: Color = Color::Rgb(249, 226, 175);
const CLR_ERR: Color = Color::Rgb(243, 139, 168);
const CLR_HIGHLIGHT_BG: Color = Color::Rgb(88, 91, 112);
const CLR_BREADCRUMB: Color = Color::Rgb(147, 153, 178);
const CLR_LABEL: Color = Color::Rgb(108, 112, 134);
const CLR_DIM: Color = Color::Rgb(88, 91, 112);

fn file_icon(name: &str, is_dir: bool) -> &'static str {
    if is_dir {
        // Nerd Font folder icon
        return "\u{e5ff}";
    }
    // Match the full filename first (dotfiles, special names)
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        ".gitignore" | ".gitmodules" => return "\u{e702}",
        ".dockerignore" | "dockerfile" | "docker-compose.yml" | "docker-compose.yaml" => {
            return "\u{e7b0}";
        }
        "makefile" | "cmakelists.txt" => return "\u{e673}",
        "license" | "licence" => return "\u{e60a}",
        "readme" | "readme.md" => return "\u{e7a2}",
        _ => {}
    }
    let ext = name.rsplit('.').next().unwrap_or("");
    match ext {
        // Languages
        "rs" => "\u{e7a8}",
        "py" => "\u{e73c}",
        "js" | "jsx" | "mjs" => "\u{e781}",
        "ts" | "tsx" | "mts" => "\u{e628}",
        "go" => "\u{e627}",
        "c" => "\u{e61e}",
        "cpp" | "cc" | "cxx" => "\u{e61d}",
        "h" | "hpp" => "\u{e61f}",
        "java" => "\u{e738}",
        "kt" | "kts" => "\u{e634}",
        "rb" => "\u{e739}",
        "swift" => "\u{e755}",
        "zig" => "\u{e6a9}",
        "lua" => "\u{e620}",
        "php" => "\u{e73d}",
        "cs" => "\u{e648}",
        // Shell
        "sh" | "bash" | "zsh" | "fish" => "\u{e795}",
        // Config / data
        "toml" => "\u{e6b2}",
        "yaml" | "yml" => "\u{e6a8}",
        "json" => "\u{e60b}",
        "xml" => "\u{e619}",
        "ini" | "cfg" | "conf" => "\u{e615}",
        // Docs
        "md" | "mdx" => "\u{e73e}",
        "txt" | "rst" | "log" => "\u{e7a2}",
        "pdf" => "\u{e68d}",
        // Web
        "html" | "htm" => "\u{e736}",
        "css" | "scss" | "sass" | "less" => "\u{e749}",
        "sql" => "\u{e706}",
        // Images
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp" => "\u{e60d}",
        "svg" => "\u{e68e}",
        "ico" => "\u{e60d}",
        // Media
        "mp4" | "mov" | "avi" | "mkv" | "webm" => "\u{e684}",
        "mp3" | "wav" | "flac" | "aac" | "m4a" => "\u{e683}",
        // Archives
        "zip" | "tar" | "gz" | "7z" | "rar" | "xz" | "bz2" => "\u{e700}",
        // Lock / misc
        "lock" => "\u{e60a}",
        _ => "\u{e5ff}",
    }
}

pub fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Percentage((100 - percent_y) / 2),
        Constraint::Percentage(percent_y),
        Constraint::Percentage((100 - percent_y) / 2),
    ])
    .flex(Flex::Legacy)
    .split(area);

    Layout::horizontal([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ])
    .flex(Flex::Legacy)
    .split(vertical[1])[1]
}

// ── Breadcrumb ──────────────────────────────────────────────────

pub fn draw_breadcrumb(app: &mut App, f: &mut Frame, area: Rect) {
    app.breadcrumb_hits.clear();

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CLR_SURFACE))
        .style(Style::default().fg(CLR_TEXT));
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let segments = breadcrumb_segments(&app.cwd);
    let mut spans = Vec::new();
    let mut x = inner.x;
    let y = inner.y;

    // Home icon prefix
    let home_icon = " ";
    spans.push(Span::styled(
        home_icon,
        Style::default().fg(CLR_DIR).add_modifier(Modifier::BOLD),
    ));
    x = x.saturating_add(UnicodeWidthStr::width(home_icon) as u16);

    for (i, (label, target)) in segments.iter().enumerate() {
        if i > 0 {
            let sep = " › ";
            spans.push(Span::styled(sep, Style::default().fg(CLR_DIM)));
            x = x.saturating_add(UnicodeWidthStr::width(sep) as u16);
        }

        let width = UnicodeWidthStr::width(label.as_str()) as u16;
        app.breadcrumb_hits.push(crate::model::HitBox {
            rect: Rect::new(x, y, width.max(1), 1),
            target: target.clone(),
        });

        let is_last = i == segments.len() - 1;
        let style = if is_last {
            Style::default()
                .fg(CLR_TEXT)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(CLR_BREADCRUMB)
        };
        spans.push(Span::styled(label.clone(), style));

        x = x.saturating_add(width);
    }

    f.render_widget(Paragraph::new(Line::from(spans)), inner);
}

// ── File list ───────────────────────────────────────────────────

pub fn draw_list(app: &mut App, f: &mut Frame, area: Rect) {
    app.list_area = area;
    let rows = area.height.saturating_sub(2) as usize;

    if app.grep_viewing {
        draw_grep_list(app, f, area, rows);
        return;
    }

    let items = app
        .filtered_indices
        .iter()
        .skip(app.list_offset)
        .take(rows)
        .map(|&idx| {
            let entry = &app.entries[idx];
            let icon = file_icon(&entry.name, entry.is_dir);
            let name = truncate_for_preview(&entry.name, 28);
            let size = if entry.is_dir {
                String::from("\u{2014}") // em dash
            } else {
                crate::fs_ops::format_size(entry.size)
            };

            let (name_style, icon_color) = if entry.is_dir {
                (
                    Style::default().fg(CLR_DIR).add_modifier(Modifier::BOLD),
                    CLR_DIR,
                )
            } else if is_image_path(&entry.path) {
                (Style::default().fg(CLR_IMAGE), CLR_IMAGE)
            } else {
                (Style::default().fg(CLR_FILE), CLR_SUBTEXT)
            };

            let spans = vec![
                Span::raw(" "),
                Span::styled(icon, Style::default().fg(icon_color)),
                Span::raw(" "),
                Span::styled(name, name_style),
                Span::raw(" "),
                Span::styled(size, Style::default().fg(CLR_DIM)),
            ];

            ListItem::new(Line::from(spans))
        })
        .collect::<Vec<_>>();

    let title = format!(
        " Files  {} / {} ",
        app.filtered_indices.len(),
        app.entries.len()
    );

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(CLR_SURFACE))
                .title(title)
                .title_style(
                    Style::default()
                        .fg(CLR_SUBTEXT)
                        .add_modifier(Modifier::BOLD),
                ),
        )
        .highlight_symbol(" ▶")
        .highlight_spacing(HighlightSpacing::Always)
        .highlight_style(
            Style::default()
                .bg(CLR_HIGHLIGHT_BG)
                .fg(CLR_TEXT)
                .add_modifier(Modifier::BOLD),
        );

    let mut state = ListState::default();

    if !app.filtered_indices.is_empty() {
        let relative_selected = app.selected.saturating_sub(app.list_offset);
        if relative_selected < rows {
            state.select(Some(relative_selected));
        }
    }

    f.render_stateful_widget(list, area, &mut state);
}

fn draw_grep_list(app: &mut App, f: &mut Frame, area: Rect, rows: usize) {
    let items = app
        .grep_results
        .iter()
        .skip(app.list_offset)
        .take(rows)
        .map(|result| {
            let file_name = result
                .path
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_default();
            let line_preview: String = result
                .line_content
                .chars()
                .take(36)
                .collect::<String>()
                .replace('\t', " ")
                .replace('\n', "");

            let icon = file_icon(&file_name, false);
            let spans = vec![
                Span::raw(" "),
                Span::styled(icon, Style::default().fg(CLR_DIR)),
                Span::raw(" "),
                Span::styled(
                    truncate_for_preview(&file_name, 18),
                    Style::default().fg(CLR_DIR),
                ),
                Span::styled(
                    format!(":{}", result.line_number),
                    Style::default().fg(CLR_WARN),
                ),
                Span::raw("  "),
                Span::styled(line_preview, Style::default().fg(CLR_SUBTEXT)),
            ];

            ListItem::new(Line::from(spans))
        })
        .collect::<Vec<_>>();

    let title = format!(" Grep  {} matches ", app.grep_results.len());

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(CLR_ERR))
                .title(title)
                .title_style(Style::default().fg(CLR_ERR).add_modifier(Modifier::BOLD)),
        )
        .highlight_symbol(" ▶")
        .highlight_spacing(HighlightSpacing::Always)
        .highlight_style(
            Style::default()
                .bg(Color::Rgb(60, 40, 50))
                .fg(CLR_TEXT)
                .add_modifier(Modifier::BOLD),
        );

    let mut state = ListState::default();
    if !app.grep_results.is_empty() {
        let relative_selected = app.selected.saturating_sub(app.list_offset);
        if relative_selected < rows {
            state.select(Some(relative_selected));
        }
    }
    f.render_stateful_widget(list, area, &mut state);
}

// ── Preview ─────────────────────────────────────────────────────

pub fn draw_preview(app: &mut App, f: &mut Frame, area: Rect) {
    app.preview_area = area;

    // Grep result context view
    if app.grep_viewing {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(CLR_ERR))
            .title(" Match Context ")
            .title_style(Style::default().fg(CLR_ERR).add_modifier(Modifier::BOLD));
        let inner = block.inner(area);
        f.render_widget(block, area);

        if let Some(result) = app.grep_results.get(app.selected) {
            let file_name = result
                .path
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_default();
            let mut lines = vec![
                Line::from(vec![
                    Span::styled("  file  ", Style::default().fg(CLR_LABEL)),
                    Span::styled(
                        file_name,
                        Style::default()
                            .fg(CLR_DIR)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("  path  ", Style::default().fg(CLR_LABEL)),
                    Span::styled(
                        result.path.display().to_string(),
                        Style::default().fg(CLR_SUBTEXT),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("  line  ", Style::default().fg(CLR_LABEL)),
                    Span::styled(
                        result.line_number.to_string(),
                        Style::default()
                            .fg(CLR_WARN)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "  ── match ──",
                    Style::default().fg(CLR_DIM),
                )),
                Line::from(""),
            ];

            for line in result.line_content.lines() {
                lines.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled(line, Style::default().fg(CLR_TEXT)),
                ]));
            }

            let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
            f.render_widget(Clear, inner);
            f.render_widget(paragraph, inner);
        } else {
            let msg = Paragraph::new("  Select a result to view context")
                .style(Style::default().fg(CLR_DIM));
            f.render_widget(msg, inner);
        }
        return;
    }

    let is_image_selected = app
        .selected_entry()
        .map(|e| is_image_path(&e.path))
        .unwrap_or(false);

    if is_image_selected
        && app.image_mode == crate::model::ImagePreviewMode::Image
        && app.use_native_preview()
    {
        let title = format!("  Preview  ·  native ({}) ", app.preview_backend_label());
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(CLR_SURFACE))
            .title(title)
            .title_style(Style::default().fg(CLR_SUBTEXT));
        let inner = block.inner(area);
        f.render_widget(block, area);
        f.render_widget(Clear, inner);
        f.render_widget(
            Block::default().style(Style::default().bg(Color::Reset)),
            inner,
        );

        if app.image_loading {
            let loading = Paragraph::new("  Loading preview...")
                .style(Style::default().fg(CLR_DIM));
            f.render_widget(loading, inner);
        }
        return;
    }

    if is_image_selected && app.image_mode == crate::model::ImagePreviewMode::Image {
        if let Some(image_state) = app.image_state.as_mut() {
            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(CLR_SURFACE))
                .title("  Preview  ·  image ")
                .title_style(Style::default().fg(CLR_SUBTEXT));
            let inner = block.inner(area);
            f.render_widget(block, area);

            let image = StatefulImage::default();
            f.render_stateful_widget(image, inner, &mut image_state.protocol);
            return;
        }
    }

    let content_height = app.preview.lines.len() as u16;
    let visible_height = area.height.saturating_sub(2);
    app.preview.max_scroll_y = content_height.saturating_sub(visible_height);
    app.preview.scroll_y = min(app.preview.scroll_y, app.preview.max_scroll_y);

    let title = app
        .selected_entry()
        .map(|e| format!("  Preview  ·  {} ", e.name))
        .unwrap_or_else(|| String::from("  Preview "));

    let paragraph = Paragraph::new(app.preview.lines.clone())
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(CLR_SURFACE))
                .title(title)
                .title_style(Style::default().fg(CLR_SUBTEXT)),
        )
        .scroll((app.preview.scroll_y, app.preview.scroll_x))
        .wrap(Wrap { trim: false });

    f.render_widget(Clear, area);
    f.render_widget(paragraph, area);
}

// ── Help bar ────────────────────────────────────────────────────

fn help_item(key: &str, label: &str) -> Vec<Span<'static>> {
    vec![
        Span::styled(
            format!(" {key} "),
            Style::default()
                .fg(CLR_TEXT)
                .bg(CLR_SURFACE)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(format!(" {label} "), Style::default().fg(CLR_SUBTEXT)),
    ]
}

pub fn draw_help(app: &App, f: &mut Frame, area: Rect) {
    let items = if app.grep_viewing {
        vec![
            Span::styled(
                " GREP ",
                Style::default()
                    .fg(CLR_ERR)
                    .bg(Color::Rgb(60, 40, 50))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  j/k navigate", Style::default().fg(CLR_SUBTEXT)),
            Span::styled("  ", Style::default()),
            Span::styled(
                " Enter ",
                Style::default()
                    .fg(CLR_TEXT)
                    .bg(CLR_SURFACE)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" open at line", Style::default().fg(CLR_SUBTEXT)),
            Span::styled("  ", Style::default()),
            Span::styled(
                " Esc ",
                Style::default()
                    .fg(CLR_TEXT)
                    .bg(CLR_SURFACE)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" exit", Style::default().fg(CLR_SUBTEXT)),
            Span::styled("  ", Style::default()),
            Span::styled(
                " ^D/^U ",
                Style::default()
                    .fg(CLR_TEXT)
                    .bg(CLR_SURFACE)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" half page", Style::default().fg(CLR_SUBTEXT)),
        ]
    } else {
        let mut spans = Vec::new();
        let items = [
            ("q", "quit"),
            ("/", "search"),
            (".", "hidden"),
            ("g", "goto"),
            ("G", "grep"),
            ("-", "back"),
            ("s", "sort"),
            ("o", "open"),
            ("d", "delete"),
            ("R", "rename"),
            ("n", "file"),
            ("N", "dir"),
            ("c", "copy"),
            ("m", "move"),
        ];

        for (i, (key, label)) in items.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw(" "));
            }
            spans.extend(help_item(key, label));
        }
        spans
    };

    let p = Paragraph::new(Line::from(items))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(CLR_SURFACE))
                .title(" Keys ")
                .title_style(Style::default().fg(CLR_DIM)),
        )
        .wrap(Wrap { trim: true });
    f.render_widget(p, area);
}

// ── Command bar ─────────────────────────────────────────────────

pub fn draw_command_bar(app: &App, f: &mut Frame, area: Rect) {
    let content = match app.command_mode {
        crate::model::CommandMode::Normal => {
            let mode = Span::styled(
                " NORMAL ",
                Style::default()
                    .fg(CLR_BG)
                    .bg(CLR_ACCENT)
                    .add_modifier(Modifier::BOLD),
            );
            Line::from(vec![mode, Span::raw("  "), Span::styled(
                "Ready",
                Style::default().fg(CLR_DIM),
            )])
        }
        crate::model::CommandMode::DeleteConfirm => {
            let mode = Span::styled(
                " DELETE ",
                Style::default()
                    .fg(CLR_BG)
                    .bg(CLR_ERR)
                    .add_modifier(Modifier::BOLD),
            );
            Line::from(vec![
                mode,
                Span::raw("  "),
                Span::styled(
                    "y confirm  ·  n/Esc cancel",
                    Style::default().fg(CLR_WARN),
                ),
            ])
        }
        crate::model::CommandMode::GoTo => {
            let mode = Span::styled(
                " GOTO ",
                Style::default()
                    .fg(CLR_BG)
                    .bg(CLR_DIR)
                    .add_modifier(Modifier::BOLD),
            );
            let mut spans = vec![mode, Span::raw("  "), Span::styled(
                &app.input_buffer,
                Style::default().fg(CLR_TEXT),
            )];
            if let Some(ghost) = app.goto_completions.first() {
                let input = app.input_buffer.trim();
                if !input.is_empty() && ghost.len() > input.len() {
                    let suffix = &ghost[input.len()..];
                    spans.push(Span::styled(suffix, Style::default().fg(CLR_DIM)));
                }
            }
            spans.push(Span::styled(
                "   Tab complete",
                Style::default().fg(CLR_DIM),
            ));
            Line::from(spans)
        }
        crate::model::CommandMode::Grep => {
            let mode = Span::styled(
                " GREP ",
                Style::default()
                    .fg(CLR_BG)
                    .bg(CLR_ERR)
                    .add_modifier(Modifier::BOLD),
            );
            Line::from(vec![
                mode,
                Span::raw("  "),
                Span::styled(&app.input_buffer, Style::default().fg(CLR_TEXT)),
                Span::styled("   Enter search", Style::default().fg(CLR_DIM)),
            ])
        }
        _ => {
            let color = match app.command_mode {
                crate::model::CommandMode::Search => CLR_ACCENT,
                crate::model::CommandMode::Rename => CLR_WARN,
                crate::model::CommandMode::NewFile | crate::model::CommandMode::NewDir => CLR_DIR,
                crate::model::CommandMode::Copy | crate::model::CommandMode::Move => CLR_IMAGE,
                _ => CLR_SUBTEXT,
            };
            let mode = Span::styled(
                format!(" {} ", app.command_mode.prompt()),
                Style::default()
                    .fg(CLR_BG)
                    .bg(color)
                    .add_modifier(Modifier::BOLD),
            );
            Line::from(vec![
                mode,
                Span::raw("  "),
                Span::styled(&app.input_buffer, Style::default().fg(CLR_TEXT)),
            ])
        }
    };

    let p = Paragraph::new(content).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(CLR_SURFACE))
            .title(" Command ")
            .title_style(Style::default().fg(CLR_DIM)),
    );
    f.render_widget(p, area);
}

// ── Status bar ──────────────────────────────────────────────────

pub fn draw_status(app: &App, f: &mut Frame, area: Rect) {
    let status_text = if app.command_mode == crate::model::CommandMode::GoTo
        && !app.goto_completions.is_empty()
    {
        let max_show = 6;
        let shown: Vec<&str> = app
            .goto_completions
            .iter()
            .take(max_show)
            .map(|s| s.as_str())
            .collect();
        let mut text = shown.join("  ");
        if app.goto_completions.len() > max_show {
            text.push_str(&format!("  +{} more", app.goto_completions.len() - max_show));
        }
        text
    } else {
        app.status.clone()
    };
    let p = Paragraph::new(status_text).style(Style::default().fg(CLR_DIM));
    f.render_widget(p, area);
}

// ── Delete popup ────────────────────────────────────────────────

pub fn draw_delete_popup(app: &App, f: &mut Frame) {
    if app.command_mode != crate::model::CommandMode::DeleteConfirm {
        return;
    }

    let area = centered_rect(50, 25, f.area());
    f.render_widget(Clear, area);

    let target = app
        .selected_entry()
        .map(|e| e.name.clone())
        .unwrap_or_else(|| String::from("<none>"));

    let lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::raw("  "),
            Span::styled(
                file_icon(&target, false),
                Style::default().fg(CLR_ERR),
            ),
            Span::raw("  "),
            Span::styled(&target, Style::default().fg(CLR_TEXT).add_modifier(Modifier::BOLD)),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "  This cannot be undone.",
            Style::default().fg(CLR_WARN),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled(
                " y ",
                Style::default()
                    .fg(CLR_BG)
                    .bg(CLR_ERR)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" delete   ", Style::default().fg(CLR_SUBTEXT)),
            Span::styled(
                " n ",
                Style::default()
                    .fg(CLR_TEXT)
                    .bg(CLR_SURFACE)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" cancel", Style::default().fg(CLR_SUBTEXT)),
        ]),
    ];

    let popup = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(CLR_ERR))
                .title(" Delete ")
                .title_style(Style::default().fg(CLR_ERR).add_modifier(Modifier::BOLD)),
        )
        .wrap(Wrap { trim: true });

    f.render_widget(popup, area);
}

// ── Context menu ────────────────────────────────────────────────

fn action_icon(action: ContextAction) -> &'static str {
    match action {
        ContextAction::Open => "\u{e5ff}",
        ContextAction::OpenEditor => "\u{e70c}",
        ContextAction::Rename => "\u{e60e}",
        ContextAction::Copy => "\u{e612}",
        ContextAction::Move => "\u{e613}",
        ContextAction::Delete => "\u{e624}",
        ContextAction::NewFile => "\u{e60f}",
        ContextAction::NewDir => "\u{e5ff}",
        ContextAction::ToggleHidden => "\u{e5f9}",
        ContextAction::SortMode => "\u{e611}",
        ContextAction::CopyPath => "\u{e60c}",
    }
}

fn action_shortcut(action: ContextAction) -> &'static str {
    match action {
        ContextAction::Open => "Enter",
        ContextAction::OpenEditor => "o",
        ContextAction::Rename => "R",
        ContextAction::Copy => "c",
        ContextAction::Move => "m",
        ContextAction::Delete => "d",
        ContextAction::NewFile => "n",
        ContextAction::NewDir => "N",
        ContextAction::ToggleHidden => ".",
        ContextAction::SortMode => "s",
        ContextAction::CopyPath => "",
    }
}

pub fn draw_context_menu(app: &App, f: &mut Frame) {
    let Some(ref menu) = app.context_menu else {
        return;
    };

    let rect = context_menu_rect(menu);
    // Clamp to screen bounds
    let area = f.area();
    let x = rect.x.min(area.width.saturating_sub(rect.width));
    let y = rect.y.min(area.height.saturating_sub(rect.height));
    let clamped = Rect::new(x, y, rect.width, rect.height);

    f.render_widget(Clear, clamped);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CLR_ACCENT))
        .style(Style::default().bg(CLR_BG));
    let inner = block.inner(clamped);
    f.render_widget(block, clamped);

    for (i, action) in menu.actions.iter().enumerate() {
        if i >= inner.height as usize {
            break;
        }
        let row_y = inner.y + i as u16;
        let row = Rect::new(inner.x, row_y, inner.width, 1);

        let is_selected = i == menu.selected;
        let icon = action_icon(*action);
        let label = action.label();
        let shortcut = action_shortcut(*action);

        let bg = if is_selected {
            CLR_HIGHLIGHT_BG
        } else {
            CLR_BG
        };

        let icon_color = match action {
            ContextAction::Delete => CLR_ERR,
            ContextAction::Open | ContextAction::OpenEditor => CLR_ACCENT,
            ContextAction::Rename | ContextAction::Move => CLR_WARN,
            ContextAction::Copy | ContextAction::CopyPath => CLR_IMAGE,
            ContextAction::NewFile | ContextAction::NewDir => CLR_DIR,
            _ => CLR_SUBTEXT,
        };

        let mut spans = vec![
            Span::styled(format!(" {icon} "), Style::default().fg(icon_color).bg(bg)),
            Span::styled(
                format!("{label:<12}"),
                Style::default()
                    .fg(if is_selected { CLR_TEXT } else { CLR_SUBTEXT })
                    .bg(bg)
                    .add_modifier(if is_selected { Modifier::BOLD } else { Modifier::empty() }),
            ),
        ];

        if !shortcut.is_empty() {
            spans.push(Span::styled(
                format!("{shortcut:>4}"),
                Style::default().fg(CLR_DIM).bg(bg),
            ));
        } else {
            spans.push(Span::raw("    "));
        }

        spans.push(Span::raw(" "));

        let line = Line::from(spans);
        f.render_widget(Paragraph::new(line), row);
    }
}

// ── Main layout ─────────────────────────────────────────────────

pub fn ui(f: &mut Frame, app: &mut App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // breadcrumb
            Constraint::Min(10),   // body
            Constraint::Length(3), // help
            Constraint::Length(3), // command
            Constraint::Length(1), // status
        ])
        .split(f.area());

    draw_breadcrumb(app, f, root[0]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(root[1]);

    draw_list(app, f, body[0]);
    draw_preview(app, f, body[1]);
    draw_help(app, f, root[2]);
    draw_command_bar(app, f, root[3]);
    draw_status(app, f, root[4]);
    draw_delete_popup(app, f);
    draw_context_menu(app, f);
}
