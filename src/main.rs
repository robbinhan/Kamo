mod app;
mod fs_ops;
mod model;
mod preview;
mod preview_backend;
mod ui;

use std::{
    io::{self, Stdout},
    process::Command,
    sync::mpsc,
    thread,
    time::Duration,
};

use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use ratatui_image::{
    errors::Errors,
    thread::{ResizeRequest, ResizeResponse},
};

use crate::{
    app::{App, ImageLoadRequest, ImageLoadResponse, OpenTarget, PendingOpen},
    preview::prepare_image_for_preview,
};

fn spawn_resize_worker(
    resize_req_rx: mpsc::Receiver<ResizeRequest>,
    resize_resp_tx: mpsc::Sender<Result<ResizeResponse, Errors>>,
) {
    thread::spawn(move || {
        while let Ok(req) = resize_req_rx.recv() {
            let resp = req.resize_encode();
            let _ = resize_resp_tx.send(resp);
        }
    });
}

fn spawn_image_load_worker(
    image_req_rx: mpsc::Receiver<ImageLoadRequest>,
    image_resp_tx: mpsc::Sender<ImageLoadResponse>,
) {
    thread::spawn(move || {
        while let Ok(mut req) = image_req_rx.recv() {
            while let Ok(next) = image_req_rx.try_recv() {
                req = next;
            }

            let resp = match prepare_image_for_preview(
                &req.cache_key.path,
                req.cache_key.max_width,
                req.cache_key.max_height,
            ) {
                Ok(prepared) => ImageLoadResponse::Loaded {
                    cache_key: req.cache_key,
                    id: req.id,
                    prepared,
                },
                Err(err) => ImageLoadResponse::Failed {
                    cache_key: req.cache_key,
                    id: req.id,
                    error: err.to_string(),
                },
            };

            let _ = image_resp_tx.send(resp);
        }
    });
}

fn shell_quote(path: &std::path::Path) -> String {
    let raw = path.to_string_lossy();
    format!("'{}'", raw.replace('\'', "'\\''"))
}

fn run_pending_open(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &mut App,
    pending: PendingOpen,
) -> Result<bool> {
    match &pending.target {
        OpenTarget::SystemDefault => {
            #[cfg(target_os = "macos")]
            let status = Command::new("open").arg(&pending.path).status()?;

            #[cfg(target_os = "linux")]
            let status = Command::new("xdg-open").arg(&pending.path).status()?;

            #[cfg(target_os = "windows")]
            let status = Command::new("cmd")
                .args(["/C", "start", ""])
                .arg(&pending.path)
                .status()?;

            let success = status.success();
            app.set_open_result(&pending, success);
            Ok(success)
        }
        OpenTarget::TerminalEditor { editor, detached } => {
            if *detached {
                let cwd = pending
                    .path
                    .parent()
                    .unwrap_or_else(|| std::path::Path::new("."));
                let status = Command::new("wezterm")
                    .args(["cli", "spawn", "--cwd"])
                    .arg(cwd)
                    .args([
                        "sh",
                        "-lc",
                        &format!("{} {}", editor, shell_quote(&pending.path)),
                    ])
                    .status();
                let success = status.is_ok_and(|status| status.success());
                app.native_needs_full_clear = true;
                app.set_open_result(&pending, success);
                return Ok(success);
            }

            let _ = app.hide_native_preview();
            disable_raw_mode()?;
            execute!(
                terminal.backend_mut(),
                DisableMouseCapture,
                LeaveAlternateScreen
            )?;
            terminal.show_cursor()?;

            let launch_result = Command::new("sh")
                .args(["-lc", &format!("{} {}", editor, shell_quote(&pending.path))])
                .status();

            enable_raw_mode()?;
            execute!(
                terminal.backend_mut(),
                EnterAlternateScreen,
                EnableMouseCapture
            )?;
            terminal.clear()?;

            let success = launch_result.is_ok_and(|status| status.success());
            app.native_needs_full_clear = true;
            app.set_open_result(&pending, success);
            Ok(success)
        }
    }
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<Stdout>>, app: &mut App) -> Result<()> {
    let mut needs_draw = true;

    loop {
        needs_draw |= app.pump_image_load_responses();
        needs_draw |= app.pump_resize_responses();
        needs_draw |= app.ensure_image_ready()?;

        if needs_draw {
            let _ = app.prepare_native_preview();
            if app.native_needs_full_clear {
                terminal.clear()?;
                app.native_needs_full_clear = false;
            }
            terminal.draw(|f| ui::ui(f, app))?;
            let _ = app.render_native_preview();
            needs_draw = false;
        }

        if let Some(pending) = app.take_pending_open() {
            let _ = run_pending_open(terminal, app, pending);
            needs_draw = true;
            continue;
        }

        if app.should_quit {
            break;
        }

        if event::poll(Duration::from_millis(50))? {
            let event = event::read()?;
            if let Err(err) = app.handle_event(event) {
                app.status = format!("Error: {err}");
                app.command_mode = model::CommandMode::Normal;
                app.input_buffer.clear();
            }
            needs_draw = true;
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let (image_req_tx, image_req_rx) = mpsc::channel::<ImageLoadRequest>();
    let (image_resp_tx, image_resp_rx) = mpsc::channel::<ImageLoadResponse>();
    let (resize_req_tx, resize_req_rx) = mpsc::channel::<ResizeRequest>();
    let (resize_resp_tx, resize_resp_rx) = mpsc::channel::<Result<ResizeResponse, Errors>>();

    spawn_image_load_worker(image_req_rx, image_resp_tx);
    spawn_resize_worker(resize_req_rx, resize_resp_tx);

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut app = App::new(image_req_tx, image_resp_rx, resize_req_tx, resize_resp_rx)?;

    let result = run_app(&mut terminal, &mut app);
    let _ = app.hide_native_preview();

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableMouseCapture,
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    result
}
