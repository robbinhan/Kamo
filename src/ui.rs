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
    app::{App, breadcrumb_segments},
    preview::{is_image_path, truncate_for_preview},
};

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

pub fn draw_breadcrumb(app: &mut App, f: &mut Frame, area: Rect) {
    app.breadcrumb_hits.clear();

    let block = Block::default().borders(Borders::ALL).title("Path");
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let segments = breadcrumb_segments(&app.cwd);
    let mut spans = Vec::new();
    let mut x = inner.x;
    let y = inner.y;

    for (i, (label, target)) in segments.iter().enumerate() {
        if i > 0 {
            let sep = " / ";
            spans.push(Span::styled(sep, Style::default().fg(Color::DarkGray)));
            x = x.saturating_add(UnicodeWidthStr::width(sep) as u16);
        }

        let width = UnicodeWidthStr::width(label.as_str()) as u16;
        app.breadcrumb_hits.push(crate::model::HitBox {
            rect: Rect::new(x, y, width.max(1), 1),
            target: target.clone(),
        });

        spans.push(Span::styled(
            label.clone(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        ));

        x = x.saturating_add(width);
    }

    f.render_widget(Paragraph::new(Line::from(spans)), inner);
}

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
            let marker = if entry.is_dir { "[D]" } else { "[F]" };
            let name = truncate_for_preview(&entry.name, 34);
            let size = if entry.is_dir {
                "dir".to_string()
            } else {
                crate::fs_ops::format_size(entry.size)
            };

            let text = format!("{marker} {:<34} {:>10}", name, size);

            let style = if entry.is_dir {
                Style::default().fg(Color::Yellow)
            } else if is_image_path(&entry.path) {
                Style::default().fg(Color::Magenta)
            } else {
                Style::default().fg(Color::White)
            };

            ListItem::new(Line::from(text)).style(style)
        })
        .collect::<Vec<_>>();

    let title = format!(
        "Files ({}/{})",
        app.filtered_indices.len(),
        app.entries.len()
    );

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_symbol("▶ ")
        .highlight_spacing(HighlightSpacing::Always)
        .highlight_style(
            Style::default()
                .bg(Color::Cyan)
                .fg(Color::Black)
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
                .take(40)
                .collect::<String>()
                .replace('\t', " ")
                .replace('\n', "");
            let text = format!(
                "{:<20} L{:<6} {}",
                truncate_for_preview(&file_name, 20),
                result.line_number,
                line_preview
            );
            ListItem::new(Line::from(text)).style(Style::default().fg(Color::White))
        })
        .collect::<Vec<_>>();

    let title = format!("Grep Results ({})", app.grep_results.len());

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .title_style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
        )
        .highlight_symbol("▶ ")
        .highlight_spacing(HighlightSpacing::Always)
        .highlight_style(
            Style::default()
                .bg(Color::Red)
                .fg(Color::White)
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

pub fn draw_preview(app: &mut App, f: &mut Frame, area: Rect) {
    app.preview_area = area;

    // Show grep result context when in grep view
    if app.grep_viewing {
        let block = Block::default().borders(Borders::ALL).title("Match Context");
        let inner = block.inner(area);
        f.render_widget(block, area);

        if let Some(result) = app.grep_results.get(app.selected) {
            let mut lines = vec![
                Line::from(vec![
                    Span::styled(
                        "File: ",
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(result.path.display().to_string()),
                ]),
                Line::from(vec![
                    Span::styled(
                        "Line: ",
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(result.line_number.to_string()),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "Content:",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
            ];

            // Show surrounding context lines
            for line in result.line_content.lines() {
                lines.push(Line::from(Span::styled(
                    format!("  {line}"),
                    Style::default().fg(Color::White),
                )));
            }

            let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
            f.render_widget(Clear, inner);
            f.render_widget(paragraph, inner);
        } else {
            let msg = Paragraph::new("Select a result to view context")
                .style(Style::default().fg(Color::DarkGray));
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
        let title = format!("Preview · native ({})", app.preview_backend_label());
        let block = Block::default().borders(Borders::ALL).title(title);
        let inner = block.inner(area);
        f.render_widget(block, area);
        f.render_widget(Clear, inner);
        f.render_widget(
            Block::default().style(Style::default().bg(Color::Reset)),
            inner,
        );

        if app.image_loading {
            let loading = Paragraph::new("Loading preview...")
                .style(Style::default().fg(Color::DarkGray))
                .wrap(Wrap { trim: true });
            f.render_widget(loading, inner);
        }
        return;
    }

    if is_image_selected && app.image_mode == crate::model::ImagePreviewMode::Image {
        if let Some(image_state) = app.image_state.as_mut() {
            let block = Block::default()
                .borders(Borders::ALL)
                .title("Preview · image");
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
        .map(|e| format!("Preview · {}", e.name))
        .unwrap_or_else(|| String::from("Preview"));

    let paragraph = Paragraph::new(app.preview.lines.clone())
        .block(Block::default().borders(Borders::ALL).title(title))
        .scroll((app.preview.scroll_y, app.preview.scroll_x))
        .wrap(Wrap { trim: false });

    f.render_widget(Clear, area);
    f.render_widget(paragraph, area);
}

pub fn draw_help(app: &App, f: &mut Frame, area: Rect) {
    let help = if app.grep_viewing {
        Line::from(vec![
            Span::styled(
                "GREP VIEW",
                Style::default()
                    .fg(Color::Red)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  j/k nav  Enter jump  Esc exit"),
        ])
    } else {
        Line::from(vec![
            Span::raw("q quit  "),
            Span::raw("/ search  "),
            Span::raw("g goto  "),
            Span::raw("G grep  "),
            Span::raw("- back  "),
            Span::raw("_ fwd  "),
            Span::raw("s sort  "),
            Span::raw("R rename  "),
            Span::raw("d delete  "),
            Span::raw("n/N file/dir  "),
            Span::raw("c/m copy/move"),
        ])
    };

    let p = Paragraph::new(help).block(Block::default().borders(Borders::ALL).title("Help"));
    f.render_widget(p, area);
}

pub fn draw_command_bar(app: &App, f: &mut Frame, area: Rect) {
    let content = match app.command_mode {
        crate::model::CommandMode::Normal => Line::from(vec![
            Span::styled(
                "NORMAL",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  Ready. Use shortcuts from Help or click files."),
        ]),
        crate::model::CommandMode::DeleteConfirm => Line::from(vec![
            Span::styled(
                "DELETE",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
            Span::raw("  Confirm in popup: y = yes, n/Esc = cancel"),
        ]),
        crate::model::CommandMode::GoTo => {
            let mut spans = vec![
                Span::styled(
                    "GOTO",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(" > "),
                Span::raw(app.input_buffer.clone()),
            ];
            // Show inline ghost completion (like fish shell autosuggestion)
            if let Some(ghost) = app.goto_completions.first() {
                let input = app.input_buffer.trim();
                if !input.is_empty() && ghost.len() > input.len() {
                    let suffix = &ghost[input.len()..];
                    spans.push(Span::styled(
                        suffix,
                        Style::default().fg(Color::DarkGray),
                    ));
                }
            }
            spans.push(Span::styled(
                "  [Tab] complete",
                Style::default().fg(Color::DarkGray),
            ));
            Line::from(spans)
        }
        _ => Line::from(vec![
            Span::styled(
                app.command_mode.prompt(),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" > "),
            Span::raw(app.input_buffer.clone()),
        ]),
    };

    let p = Paragraph::new(content).block(Block::default().borders(Borders::ALL).title("Command"));
    f.render_widget(p, area);
}

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
    let p = Paragraph::new(status_text).style(Style::default().fg(Color::DarkGray));
    f.render_widget(p, area);
}

pub fn draw_delete_popup(app: &App, f: &mut Frame) {
    if app.command_mode != crate::model::CommandMode::DeleteConfirm {
        return;
    }

    let area = centered_rect(60, 30, f.area());
    f.render_widget(Clear, area);

    let target = app
        .selected_entry()
        .map(|e| e.name.clone())
        .unwrap_or_else(|| String::from("<none>"));

    let lines = vec![
        Line::from(Span::styled(
            "Delete confirmation",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(format!("Delete: {}", target)),
        Line::from("This action cannot be undone."),
        Line::from(""),
        Line::from("Press y to confirm, n or Esc to cancel."),
    ];

    let popup = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Confirm Delete"),
        )
        .wrap(Wrap { trim: true });

    f.render_widget(popup, area);
}

pub fn ui(f: &mut Frame, app: &mut App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(f.area());

    draw_breadcrumb(app, f, root[0]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(42), Constraint::Percentage(58)])
        .split(root[1]);

    draw_list(app, f, body[0]);
    draw_preview(app, f, body[1]);
    draw_help(app, f, root[2]);
    draw_command_bar(app, f, root[3]);
    draw_status(app, f, root[4]);
    draw_delete_popup(app, f);
}
