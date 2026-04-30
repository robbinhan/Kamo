use std::{
    cmp::min,
    collections::{HashMap, VecDeque},
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        mpsc::{Receiver, Sender},
    },
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow, bail};
use crossterm::event::{
    Event, KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::layout::Rect;
use ratatui_image::{
    errors::Errors,
    picker::{Picker, ProtocolType},
    thread::{ResizeRequest, ResizeResponse, ThreadProtocol},
};

use crate::{
    fs_ops::{copy_path, read_entries, resolve_destination, sort_entries},
    model::{
        CommandMode, ContextAction, ContextMenu, Entry, GrepResult, HitBox, ImagePreviewMode,
        ImageRenderState, PreviewData, SortMode,
    },
    preview::{
        DEFAULT_PREVIEW_IMAGE_DIMENSION, Highlighter, PreparedImage, build_preview, is_html_path,
        is_image_path, is_visual_preview,
    },
    preview_backend::{
        NativePreviewBackend, NativePreviewController, NativeRenderFit, NativeRenderSpec,
        encode_image_payload, fit_wezterm_render_spec, probe_wezterm_cell_size,
    },
};

const DOUBLE_CLICK_MS: u64 = 450;
const IMAGE_PREVIEW_DEBOUNCE_MS: u64 = 40;
const IMAGE_CACHE_CAPACITY: usize = 48;
const IMAGE_CACHE_BUCKET_PX: u32 = 128;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ImageCacheKey {
    pub path: PathBuf,
    pub file_size: u64,
    pub modified_ms: u128,
    pub max_width: u32,
    pub max_height: u32,
}

pub struct ImageLoadRequest {
    pub cache_key: ImageCacheKey,
    pub id: u64,
}

pub enum ImageLoadResponse {
    Loaded {
        cache_key: ImageCacheKey,
        id: u64,
        prepared: PreparedImage,
    },
    Failed {
        cache_key: ImageCacheKey,
        id: u64,
        error: String,
    },
}

#[derive(Clone, Debug)]
pub enum OpenTarget {
    TerminalEditor { editor: String, detached: bool },
    SystemDefault,
}

#[derive(Clone, Debug)]
pub struct PendingOpen {
    pub path: PathBuf,
    pub target: OpenTarget,
    pub line_number: Option<u64>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NativePayloadKey {
    pub cache_key: ImageCacheKey,
    pub render_width: u16,
    pub render_height: u16,
    pub render_fit: NativeRenderFit,
}

pub struct App {
    pub cwd: PathBuf,
    pub entries: Vec<Entry>,
    pub filtered_indices: Vec<usize>,
    pub selected: usize,
    pub list_offset: usize,
    pub list_area: Rect,
    pub preview_area: Rect,
    pub breadcrumb_hits: Vec<HitBox>,
    pub last_click: Option<(u16, u16, Instant)>,

    pub preview: PreviewData,
    pub image_mode: ImagePreviewMode,
    pub image_state: Option<ImageRenderState>,
    pub image_path: Option<PathBuf>,
    pub image_loading: bool,
    pub image_dirty: bool,
    pub image_request_id: u64,
    pub image_original_dimensions: Option<(u32, u32)>,
    pub image_preview_dimensions: Option<(u32, u32)>,
    pub image_debounce_deadline: Option<Instant>,
    pub image_pending_cache_key: Option<ImageCacheKey>,
    pub image_cache: HashMap<ImageCacheKey, PreparedImage>,
    pub image_cache_order: VecDeque<ImageCacheKey>,
    pub native_payload_cache: HashMap<NativePayloadKey, Arc<[u8]>>,
    pub native_preview: Option<NativePreviewController>,
    pub native_needs_full_clear: bool,
    pub wezterm_cell_size: Option<(u32, u32)>,

    pub picker: Picker,
    pub image_load_req_tx: Sender<ImageLoadRequest>,
    pub image_load_resp_rx: Receiver<ImageLoadResponse>,
    pub resize_req_tx: Sender<ResizeRequest>,
    pub resize_resp_rx: Receiver<Result<ResizeResponse, Errors>>,

    pub show_hidden: bool,
    pub sort_mode: SortMode,
    pub status: String,
    pub should_quit: bool,
    pub command_mode: CommandMode,
    pub input_buffer: String,
    pub search_query: String,
    pub dir_history: Vec<PathBuf>,
    pub dir_history_idx: usize,
    pub goto_completions: Vec<String>,
    pub goto_completion_idx: usize,
    pub grep_results: Vec<GrepResult>,
    pub grep_result_rx: Receiver<GrepResult>,
    pub grep_result_tx: Sender<GrepResult>,
    pub grep_done_rx: Receiver<bool>,
    pub grep_done_tx: Sender<bool>,
    pub grep_active: bool,
    pub grep_viewing: bool,
    pub highlighter: Highlighter,
    pub pending_open: Option<PendingOpen>,
    pub context_menu: Option<ContextMenu>,
}

impl App {
    pub fn new(
        image_load_req_tx: Sender<ImageLoadRequest>,
        image_load_resp_rx: Receiver<ImageLoadResponse>,
        resize_req_tx: Sender<ResizeRequest>,
        resize_resp_rx: Receiver<Result<ResizeResponse, Errors>>,
    ) -> Result<Self> {
        let cwd = std::env::current_dir()?;
        let picker = Picker::from_query_stdio()?;
        let native_preview = NativePreviewController::detect();
        let wezterm_cell_size = matches!(
            native_preview
                .as_ref()
                .map(NativePreviewController::backend),
            Some(NativePreviewBackend::WezTerm)
        )
        .then(|| probe_wezterm_cell_size(&cwd))
        .flatten();
        let (grep_result_tx, grep_result_rx) = std::sync::mpsc::channel();
        let (grep_done_tx, grep_done_rx) = std::sync::mpsc::channel();

        let mut app = Self {
            cwd,
            entries: Vec::new(),
            filtered_indices: Vec::new(),
            selected: 0,
            list_offset: 0,
            list_area: Rect::default(),
            preview_area: Rect::default(),
            breadcrumb_hits: Vec::new(),
            last_click: None,

            preview: PreviewData::new(vec![]),
            image_mode: ImagePreviewMode::Image,
            image_state: None,
            image_path: None,
            image_loading: false,
            image_dirty: true,
            image_request_id: 0,
            image_original_dimensions: None,
            image_preview_dimensions: None,
            image_debounce_deadline: None,
            image_pending_cache_key: None,
            image_cache: HashMap::new(),
            image_cache_order: VecDeque::new(),
            native_payload_cache: HashMap::new(),
            native_preview,
            native_needs_full_clear: false,
            wezterm_cell_size,

            picker,
            image_load_req_tx,
            image_load_resp_rx,
            resize_req_tx,
            resize_resp_rx,

            show_hidden: false,
            sort_mode: SortMode::NameAsc,
            status: String::from("Ready"),
            should_quit: false,
            command_mode: CommandMode::Normal,
            input_buffer: String::new(),
            search_query: String::new(),
            dir_history: Vec::new(),
            dir_history_idx: 0,
            goto_completions: Vec::new(),
            goto_completion_idx: 0,
            grep_results: Vec::new(),
            grep_result_rx,
            grep_result_tx,
            grep_done_rx,
            grep_done_tx,
            grep_active: false,
            grep_viewing: false,
            highlighter: Highlighter::new(),
            pending_open: None,
            context_menu: None,
        };

        app.reload_entries()?;
        Ok(app)
    }

    pub fn selected_entry(&self) -> Option<&Entry> {
        let idx = *self.filtered_indices.get(self.selected)?;
        self.entries.get(idx)
    }

    pub fn visible_rows(&self) -> usize {
        self.list_area.height.saturating_sub(2) as usize
    }

    pub fn ensure_visible(&mut self) {
        let rows = self.visible_rows().max(1);
        if self.selected < self.list_offset {
            self.list_offset = self.selected;
        } else if self.selected >= self.list_offset + rows {
            self.list_offset = self.selected + 1 - rows;
        }
    }

    pub fn reload_entries(&mut self) -> Result<()> {
        self.entries = read_entries(&self.cwd, self.show_hidden)?;
        sort_entries(&mut self.entries, self.sort_mode);
        self.apply_filter();
        self.mark_image_dirty(false);
        self.refresh_preview();
        Ok(())
    }

    pub fn apply_filter(&mut self) {
        let q = self.search_query.to_lowercase();
        self.filtered_indices = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, e)| q.is_empty() || e.name.to_lowercase().contains(&q))
            .map(|(i, _)| i)
            .collect();

        if self.filtered_indices.is_empty() {
            self.selected = 0;
            self.list_offset = 0;
        } else {
            self.selected = min(self.selected, self.filtered_indices.len() - 1);
            self.ensure_visible();
        }
    }

    pub fn refresh_preview(&mut self) {
        self.preview = if let Some(entry) = self.selected_entry() {
            build_preview(entry, &self.highlighter, self.image_mode)
        } else {
            PreviewData::new(vec![
                ratatui::text::Line::from("No match"),
                ratatui::text::Line::from(format!("path: {}", self.cwd.display())),
            ])
        };

        self.status = if self.use_native_preview() {
            format!(
                "cwd: {} | total: {} | shown: {} | hidden: {} | sort: {} | image: {} | backend: {}",
                self.cwd.display(),
                self.entries.len(),
                self.filtered_indices.len(),
                if self.show_hidden { "on" } else { "off" },
                self.sort_mode.label(),
                self.image_mode.label(),
                self.preview_backend_label(),
            )
        } else {
            format!(
                "cwd: {} | total: {} | shown: {} | hidden: {} | sort: {} | image: {} | proto: {}",
                self.cwd.display(),
                self.entries.len(),
                self.filtered_indices.len(),
                if self.show_hidden { "on" } else { "off" },
                self.sort_mode.label(),
                self.image_mode.label(),
                self.protocol_label(),
            )
        };
    }

    pub fn mark_image_dirty(&mut self, debounce: bool) {
        self.image_dirty = true;
        self.image_loading = false;
        self.image_state = None;
        self.image_original_dimensions = None;
        self.image_preview_dimensions = None;
        self.image_pending_cache_key = None;
        self.image_debounce_deadline =
            debounce.then(|| Instant::now() + Duration::from_millis(IMAGE_PREVIEW_DEBOUNCE_MS));
    }

    pub fn touch_cache_key(&mut self, key: &ImageCacheKey) {
        if let Some(pos) = self
            .image_cache_order
            .iter()
            .position(|existing| existing == key)
        {
            self.image_cache_order.remove(pos);
        }
        self.image_cache_order.push_back(key.clone());
    }

    pub fn insert_cached_image(&mut self, key: ImageCacheKey, prepared: PreparedImage) {
        self.image_cache.insert(key.clone(), prepared);
        self.touch_cache_key(&key);

        while self.image_cache.len() > IMAGE_CACHE_CAPACITY {
            let Some(oldest) = self.image_cache_order.pop_front() else {
                break;
            };

            if self.image_cache.remove(&oldest).is_some() {
                self.native_payload_cache
                    .retain(|key, _| key.cache_key != oldest);
                break;
            }
        }
    }

    pub fn preview_cache_key(
        &self,
        entry: &Entry,
        max_width: u32,
        max_height: u32,
    ) -> Result<ImageCacheKey> {
        let metadata = fs::metadata(&entry.path)?;
        let modified = metadata.modified().ok();
        let modified_ms = modified
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis())
            .unwrap_or_default();

        let bucket_width = bucket_dimension(max_width);
        let bucket_height = bucket_dimension(max_height);

        Ok(ImageCacheKey {
            path: entry.path.clone(),
            file_size: metadata.len(),
            modified_ms,
            max_width: bucket_width,
            max_height: bucket_height,
        })
    }

    pub fn ensure_image_ready(&mut self) -> Result<bool> {
        let Some(entry) = self.selected_entry().cloned() else {
            let changed =
                self.image_state.is_some() || self.image_path.is_some() || self.image_loading;
            self.image_state = None;
            self.image_path = None;
            self.image_loading = false;
            self.image_dirty = false;
            self.image_original_dimensions = None;
            self.image_preview_dimensions = None;
            self.image_debounce_deadline = None;
            self.image_pending_cache_key = None;
            let _ = self.hide_native_preview();
            return Ok(changed);
        };

        let entry_path = entry.path.clone();

        if !is_visual_preview(&entry_path) || self.image_mode != ImagePreviewMode::Image {
            let changed =
                self.image_state.is_some() || self.image_path.is_some() || self.image_loading;
            self.image_state = None;
            self.image_path = None;
            self.image_loading = false;
            self.image_dirty = false;
            self.image_original_dimensions = None;
            self.image_preview_dimensions = None;
            self.image_debounce_deadline = None;
            self.image_pending_cache_key = None;
            let _ = self.hide_native_preview();
            return Ok(changed);
        }

        if !self.image_dirty && self.image_path.as_ref() == Some(&entry_path) {
            return Ok(false);
        }

        if let Some(deadline) = self.image_debounce_deadline {
            if Instant::now() < deadline {
                if self.image_path.as_ref() != Some(&entry_path) || self.image_loading {
                    self.image_path = Some(entry_path.clone());
                    self.image_loading = false;
                    self.image_state = None;
                    self.status = format!(
                        "Waiting to preview: {} | debounce: {}ms",
                        entry_path.display(),
                        IMAGE_PREVIEW_DEBOUNCE_MS
                    );
                    return Ok(true);
                }
                return Ok(false);
            }
        }

        self.image_debounce_deadline = None;
        self.image_request_id = self.image_request_id.wrapping_add(1);
        let (max_width, max_height) = self.preview_decode_bounds();
        let cache_key = self.preview_cache_key(&entry, max_width, max_height)?;

        self.image_path = Some(entry_path.clone());
        self.image_pending_cache_key = Some(cache_key.clone());

        if let Some(prepared) = self.image_cache.get(&cache_key).cloned() {
            self.touch_cache_key(&cache_key);
            self.image_loading = false;
            self.image_dirty = false;
            self.image_original_dimensions = Some(prepared.original_dimensions);
            self.image_preview_dimensions = Some(prepared.preview_dimensions);
            if self.use_native_preview() {
                self.image_state = None;
                self.status = format!(
                    "Image ready (cache): {} | original: {}x{} | preview source: {}x{} | backend: {}",
                    entry_path.display(),
                    prepared.original_dimensions.0,
                    prepared.original_dimensions.1,
                    prepared.preview_dimensions.0,
                    prepared.preview_dimensions.1,
                    self.preview_backend_label()
                );
            } else {
                let inner = self
                    .picker
                    .new_resize_protocol(prepared.to_dynamic_image()?);
                let protocol = ThreadProtocol::new(self.resize_req_tx.clone(), Some(inner));
                self.image_state = Some(ImageRenderState { protocol });
                self.status = format!(
                    "Image ready (cache): {} | original: {}x{} | preview source: {}x{} | proto: {}",
                    entry_path.display(),
                    prepared.original_dimensions.0,
                    prepared.original_dimensions.1,
                    prepared.preview_dimensions.0,
                    prepared.preview_dimensions.1,
                    self.protocol_label()
                );
            }
            return Ok(true);
        }

        self.image_state = None;
        self.image_loading = true;
        self.image_dirty = false;
        self.image_original_dimensions = None;
        self.image_preview_dimensions = None;
        self.status = if self.use_native_preview() {
            format!(
                "Loading image: {} | target: {}x{} | backend: {}",
                entry_path.display(),
                cache_key.max_width,
                cache_key.max_height,
                self.preview_backend_label()
            )
        } else {
            format!(
                "Loading image: {} | target: {}x{} | proto: {}",
                entry_path.display(),
                cache_key.max_width,
                cache_key.max_height,
                self.protocol_label()
            )
        };

        self.image_load_req_tx.send(ImageLoadRequest {
            cache_key,
            id: self.image_request_id,
        })?;

        Ok(true)
    }

    pub fn pump_image_load_responses(&mut self) -> bool {
        let mut changed = false;
        while let Ok(msg) = self.image_load_resp_rx.try_recv() {
            match msg {
                ImageLoadResponse::Loaded {
                    cache_key,
                    id,
                    prepared,
                } => {
                    if id != self.image_request_id
                        || self.image_pending_cache_key.as_ref() != Some(&cache_key)
                    {
                        continue;
                    }

                    let path = cache_key.path.clone();
                    self.image_loading = false;
                    self.image_pending_cache_key = Some(cache_key.clone());
                    self.image_original_dimensions = Some(prepared.original_dimensions);
                    self.image_preview_dimensions = Some(prepared.preview_dimensions);
                    self.insert_cached_image(cache_key, prepared.clone());
                    let optimized = if prepared.original_dimensions != prepared.preview_dimensions {
                        format!(
                            " | preview source: {}x{}",
                            prepared.preview_dimensions.0, prepared.preview_dimensions.1
                        )
                    } else {
                        String::new()
                    };
                    if self.use_native_preview() {
                        self.image_state = None;
                        self.status = format!(
                            "Image ready: {} | original: {}x{}{} | backend: {}",
                            path.display(),
                            prepared.original_dimensions.0,
                            prepared.original_dimensions.1,
                            optimized,
                            self.preview_backend_label()
                        );
                    } else {
                        let inner = match prepared.to_dynamic_image() {
                            Ok(image) => self.picker.new_resize_protocol(image),
                            Err(err) => {
                                self.image_loading = false;
                                self.image_state = None;
                                self.status = format!("Cached preview decode failed: {err}");
                                changed = true;
                                continue;
                            }
                        };
                        let protocol = ThreadProtocol::new(self.resize_req_tx.clone(), Some(inner));
                        self.image_state = Some(ImageRenderState { protocol });
                        self.status = format!(
                            "Image ready: {} | original: {}x{}{} | proto: {}",
                            path.display(),
                            prepared.original_dimensions.0,
                            prepared.original_dimensions.1,
                            optimized,
                            self.protocol_label()
                        );
                    }
                    changed = true;
                }
                ImageLoadResponse::Failed {
                    cache_key,
                    id,
                    error,
                } => {
                    if id != self.image_request_id
                        || self.image_pending_cache_key.as_ref() != Some(&cache_key)
                    {
                        continue;
                    }

                    self.image_state = None;
                    self.image_loading = false;
                    self.image_pending_cache_key = None;
                    self.image_original_dimensions = None;
                    self.image_preview_dimensions = None;
                    self.status = if self.use_native_preview() {
                        format!(
                            "Image decode failed: {error} | backend: {}",
                            self.preview_backend_label()
                        )
                    } else {
                        format!(
                            "Image decode failed: {error} | proto: {}",
                            self.protocol_label()
                        )
                    };
                    changed = true;
                }
            }
        }
        changed
    }

    pub fn pump_resize_responses(&mut self) -> bool {
        let mut changed = false;
        while let Ok(msg) = self.resize_resp_rx.try_recv() {
            match msg {
                Ok(resp) => {
                    if let Some(state) = self.image_state.as_mut() {
                        changed |= state.protocol.update_resized_protocol(resp);
                    }
                }
                Err(err) => {
                    self.status = format!(
                        "Image resize worker error: {err} | proto: {}",
                        self.protocol_label()
                    );
                    changed = true;
                }
            }
        }
        changed
    }

    pub fn set_selected(&mut self, idx: usize) {
        if self.filtered_indices.is_empty() {
            self.selected = 0;
            self.list_offset = 0;
            self.refresh_preview();
            return;
        }

        let next = min(idx, self.filtered_indices.len() - 1);
        if self.selected == next {
            return;
        }

        self.selected = next;
        self.ensure_visible();
        self.mark_image_dirty(true);
        self.refresh_preview();
    }

    pub fn select_next(&mut self) {
        if !self.filtered_indices.is_empty() {
            self.set_selected(min(self.selected + 1, self.filtered_indices.len() - 1));
        }
    }

    pub fn select_prev(&mut self) {
        if !self.filtered_indices.is_empty() {
            self.set_selected(self.selected.saturating_sub(1));
        }
    }

    pub fn page_down(&mut self) {
        let step = self.visible_rows().max(1);
        if !self.filtered_indices.is_empty() {
            self.set_selected(min(self.selected + step, self.filtered_indices.len() - 1));
        }
    }

    pub fn page_up(&mut self) {
        let step = self.visible_rows().max(1);
        if !self.filtered_indices.is_empty() {
            self.set_selected(self.selected.saturating_sub(step));
        }
    }

    pub fn half_page_down(&mut self) {
        let step = self.visible_rows().max(1) / 2;
        if !self.filtered_indices.is_empty() {
            self.set_selected(min(self.selected + step, self.filtered_indices.len() - 1));
        }
    }

    pub fn half_page_up(&mut self) {
        let step = self.visible_rows().max(1) / 2;
        if !self.filtered_indices.is_empty() {
            self.set_selected(self.selected.saturating_sub(step));
        }
    }

    pub fn open_selected(&mut self) -> Result<()> {
        let Some(entry) = self.selected_entry() else {
            return Ok(());
        };

        let is_dir = entry.is_dir;
        let path = entry.path.clone();
        let name = entry.name.clone();

        if is_dir {
            self.push_history();
            self.cwd = path;
            self.selected = 0;
            self.list_offset = 0;
            self.search_query.clear();
            self.grep_viewing = false;
            self.reload_entries()?;
        } else {
            self.status = format!("Opened preview for {name}");
        }

        Ok(())
    }

    pub fn protocol_label(&self) -> String {
        format!("{:?}", self.picker.protocol_type())
    }

    pub fn use_native_preview(&self) -> bool {
        self.native_preview.is_some()
    }

    pub fn preview_backend_label(&self) -> String {
        match self
            .native_preview
            .as_ref()
            .map(NativePreviewController::backend)
        {
            Some(NativePreviewBackend::WezTerm) => String::from("wezterm-osc1337"),
            Some(NativePreviewBackend::Kitty) => String::from("kitty-graphics"),
            None => self.protocol_label(),
        }
    }

    pub fn current_native_preview_signature(&self) -> Option<(u64, Rect)> {
        let spec = self.current_native_render_spec()?;
        if spec.area.width == 0 || spec.area.height == 0 {
            return None;
        }

        let backend = self.native_preview.as_ref()?.backend();
        let cache_key = self.image_pending_cache_key.as_ref()?;
        let mut hasher = DefaultHasher::new();
        cache_key.hash(&mut hasher);
        spec.hash(&mut hasher);
        backend.hash(&mut hasher);
        Some((hasher.finish(), spec.area))
    }

    pub fn current_native_render_spec(&self) -> Option<NativeRenderSpec> {
        let backend = self.native_preview.as_ref()?.backend();
        if self.image_mode != ImagePreviewMode::Image {
            return None;
        }

        let cache_key = self.image_pending_cache_key.as_ref()?;
        let prepared = self.image_cache.get(cache_key)?;
        match backend {
            NativePreviewBackend::WezTerm => self.wezterm_render_spec(prepared.preview_dimensions),
            NativePreviewBackend::Kitty => Some(NativeRenderSpec {
                area: self.native_render_area(prepared.preview_dimensions),
                fit: NativeRenderFit::CellRect,
            }),
        }
    }

    pub fn native_render_area(&self, preview_dimensions: (u32, u32)) -> Rect {
        let area = inner_preview_rect(self.preview_area);
        if area.width == 0 || area.height == 0 {
            return area;
        }

        let max_width_cells = area.width.saturating_sub(1).max(1);
        let max_height_cells = area.height.saturating_sub(1).max(1);
        let (font_width, font_height) = self.picker.font_size();
        let cell_width = u32::from(font_width.max(1));
        let cell_height = u32::from(font_height.max(1));
        let render_width =
            (preview_dimensions.0 / cell_width).clamp(1, u32::from(max_width_cells)) as u16;
        let render_height =
            (preview_dimensions.1 / cell_height).clamp(1, u32::from(max_height_cells)) as u16;

        Rect::new(
            area.x + area.width.saturating_sub(render_width) / 2,
            area.y + area.height.saturating_sub(render_height) / 2,
            render_width,
            render_height,
        )
    }

    pub fn wezterm_render_spec(&self, preview_dimensions: (u32, u32)) -> Option<NativeRenderSpec> {
        fit_wezterm_render_spec(
            inner_preview_rect(self.preview_area),
            preview_dimensions,
            self.cell_size_hint(),
        )
    }

    pub fn native_image_payload(
        &mut self,
        cache_key: &ImageCacheKey,
        spec: NativeRenderSpec,
    ) -> Result<Arc<[u8]>> {
        let payload_key = NativePayloadKey {
            cache_key: cache_key.clone(),
            render_width: spec.area.width,
            render_height: spec.area.height,
            render_fit: spec.fit,
        };
        if let Some(payload) = self.native_payload_cache.get(&payload_key) {
            return Ok(payload.clone());
        }

        let backend = self
            .native_preview
            .as_ref()
            .map(NativePreviewController::backend)
            .ok_or_else(|| anyhow!("native preview backend unavailable"))?;
        let prepared = self
            .image_cache
            .get(cache_key)
            .ok_or_else(|| anyhow!("missing cached preview for native render"))?;
        let payload: Arc<[u8]> = Arc::from(encode_image_payload(backend, prepared, spec)?);
        self.native_payload_cache
            .insert(payload_key, payload.clone());
        Ok(payload)
    }

    pub fn native_clear_rect(&self) -> Rect {
        inner_preview_rect(self.preview_area)
    }

    pub fn prepare_native_preview(&mut self) -> Result<bool> {
        let desired = self.current_native_preview_signature();
        let clear_rect = self.native_clear_rect();
        let Some(controller) = self.native_preview.as_mut() else {
            return Ok(false);
        };

        let had_image = controller.shown().is_some();
        let Some((desired_key, desired_area)) = desired else {
            return if had_image {
                controller.clear(clear_rect)
            } else {
                Ok(false)
            };
        };

        let should_hide = controller
            .shown()
            .is_some_and(|shown| shown.key != desired_key || shown.area != desired_area);
        if should_hide {
            return controller.clear(clear_rect);
        }

        Ok(false)
    }

    pub fn hide_native_preview(&mut self) -> Result<bool> {
        let clear_rect = self.native_clear_rect();
        let Some(controller) = self.native_preview.as_mut() else {
            return Ok(false);
        };
        if controller.shown().is_some() {
            controller.clear(clear_rect)
        } else {
            Ok(false)
        }
    }

    pub fn render_native_preview(&mut self) -> Result<bool> {
        let Some(spec) = self.current_native_render_spec() else {
            return Ok(false);
        };
        let Some((desired_key, _)) = self.current_native_preview_signature() else {
            return Ok(false);
        };

        let Some(cache_key) = self.image_pending_cache_key.clone() else {
            return Ok(false);
        };
        let payload = self.native_image_payload(&cache_key, spec)?;
        let Some(controller) = self.native_preview.as_mut() else {
            return Ok(false);
        };

        if controller.show(desired_key, spec.area, payload.as_ref())? {
            self.status = format!(
                "Image ready: native backend {}",
                self.preview_backend_label()
            );
            return Ok(true);
        }

        Ok(false)
    }

    pub fn preview_decode_bounds(&self) -> (u32, u32) {
        let inner_width = self.preview_area.width.saturating_sub(3).max(1);
        let inner_height = self.preview_area.height.saturating_sub(3).max(1);
        let (cell_width, cell_height) = self.cell_size_hint();
        let max_width = u32::from(inner_width.max(1)) * cell_width;
        let max_height = u32::from(inner_height.max(1)) * cell_height;

        if max_width == 0 || max_height == 0 {
            return (
                DEFAULT_PREVIEW_IMAGE_DIMENSION,
                DEFAULT_PREVIEW_IMAGE_DIMENSION,
            );
        }

        (
            max_width.max(DEFAULT_PREVIEW_IMAGE_DIMENSION / 2),
            max_height.max(DEFAULT_PREVIEW_IMAGE_DIMENSION / 2),
        )
    }

    pub fn cell_size_hint(&self) -> (u32, u32) {
        if matches!(
            self.native_preview
                .as_ref()
                .map(NativePreviewController::backend),
            Some(NativePreviewBackend::WezTerm)
        ) {
            if let Some(cell_size) = self.wezterm_cell_size {
                return cell_size;
            }
        }

        let (font_width, font_height) = self.picker.font_size();
        (u32::from(font_width.max(1)), u32::from(font_height.max(1)))
    }

    pub fn refresh_native_metrics(&mut self) {
        self.wezterm_cell_size = if matches!(
            self.native_preview
                .as_ref()
                .map(NativePreviewController::backend),
            Some(NativePreviewBackend::WezTerm)
        ) {
            probe_wezterm_cell_size(&self.cwd)
        } else {
            None
        };
    }

    pub fn queue_open_selected(&mut self) -> Result<()> {
        let Some(path) = self.selected_path() else {
            self.status = String::from("No file selected");
            return Ok(());
        };

        let target = if is_html_path(&path) && awrit_available() {
            // HTML files: prefer awrit for interactive terminal browsing
            // Serve via temporary HTTP server since Electron custom sessions block file://
            let url = serve_html_via_http(&path);
            let quoted = shell_quote_str(&url);
            OpenTarget::TerminalEditor {
                editor: format!("awrit {quoted}"),
                detached: can_spawn_editor_tab(),
            }
        } else if prefers_system_open(&path) {
            OpenTarget::SystemDefault
        } else if let Some(editor) = preferred_terminal_editor() {
            OpenTarget::TerminalEditor {
                editor: editor.to_string(),
                detached: can_spawn_editor_tab(),
            }
        } else {
            OpenTarget::SystemDefault
        };

        self.status = match &target {
            OpenTarget::TerminalEditor { editor, detached } => {
                if *detached {
                    format!("Opening in new editor tab: {editor} | {}", path.display())
                } else {
                    format!("Opening in terminal editor: {editor} | {}", path.display())
                }
            }
            OpenTarget::SystemDefault => {
                format!("Opening in default app: {}", path.display())
            }
        };
        self.pending_open = Some(PendingOpen { path, target, line_number: None });
        Ok(())
    }

    pub fn take_pending_open(&mut self) -> Option<PendingOpen> {
        self.pending_open.take()
    }

    pub fn set_open_result(&mut self, pending: &PendingOpen, success: bool) {
        let path_display = match pending.line_number {
            Some(line) => format!("{}:{}", pending.path.display(), line),
            None => pending.path.display().to_string(),
        };
        self.status = match (&pending.target, success) {
            (OpenTarget::TerminalEditor { editor, detached }, true) => {
                if *detached {
                    format!("Opened in new editor tab: {editor} | {path_display}")
                } else {
                    format!("Returned from {editor}: {path_display}")
                }
            }
            (OpenTarget::TerminalEditor { editor, .. }, false) => {
                format!("Failed to launch {editor}: {path_display}")
            }
            (OpenTarget::SystemDefault, true) => {
                format!("Opened in default app: {path_display}")
            }
            (OpenTarget::SystemDefault, false) => {
                format!("Failed to open in default app: {path_display}")
            }
        };
    }

    pub fn cycle_image_protocol(&mut self) {
        if self.use_native_preview() {
            self.status = format!(
                "Native preview backend active: {}",
                self.preview_backend_label()
            );
            return;
        }

        let next = next_protocol(self.picker.protocol_type());
        self.picker.set_protocol_type(next);
        self.mark_image_dirty(false);
        self.refresh_preview();
        self.status = if protocol_known_broken(next) {
            format!(
                "Image protocol switched to {} (known broken here; placeholders may appear)",
                self.protocol_label()
            )
        } else {
            format!("Image protocol switched to {}", self.protocol_label())
        };
    }

    pub fn push_history(&mut self) {
        // Truncate forward history when navigating to a new place
        self.dir_history.truncate(self.dir_history_idx);
        // Avoid duplicate consecutive entries
        if self.dir_history.last() != Some(&self.cwd) {
            self.dir_history.push(self.cwd.clone());
        }
        self.dir_history_idx = self.dir_history.len();
    }

    pub fn go_parent(&mut self) -> Result<()> {
        if let Some(parent) = self.cwd.parent().map(|p| p.to_path_buf()) {
            self.push_history();
            self.cwd = parent;
            self.selected = 0;
            self.list_offset = 0;
            self.search_query.clear();
            self.grep_viewing = false;
            self.reload_entries()?;
        }
        Ok(())
    }

    pub fn go_to(&mut self, path: PathBuf) -> Result<()> {
        self.push_history();
        self.cwd = path;
        self.selected = 0;
        self.list_offset = 0;
        self.search_query.clear();
        self.grep_viewing = false;
        self.reload_entries()?;
        Ok(())
    }

    pub fn go_back(&mut self) -> Result<()> {
        if self.dir_history_idx == 0 {
            self.status = String::from("No earlier directory");
            return Ok(());
        }
        self.dir_history_idx -= 1;
        let path = self.dir_history[self.dir_history_idx].clone();
        self.cwd = path;
        self.selected = 0;
        self.list_offset = 0;
        self.search_query.clear();
        self.reload_entries()?;
        self.status = format!("Back: {}", self.cwd.display());
        Ok(())
    }

    pub fn go_forward(&mut self) -> Result<()> {
        if self.dir_history_idx + 1 >= self.dir_history.len() {
            self.status = String::from("No later directory");
            return Ok(());
        }
        self.dir_history_idx += 1;
        let path = self.dir_history[self.dir_history_idx].clone();
        self.cwd = path;
        self.selected = 0;
        self.list_offset = 0;
        self.search_query.clear();
        self.reload_entries()?;
        self.status = format!("Forward: {}", self.cwd.display());
        Ok(())
    }

    pub fn go_to_path_input(&mut self, input: &str) -> Result<()> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            bail!("path is empty");
        }

        let path = if trimmed.starts_with('~') {
            if let Some(home) = dirs_home() {
                home.join(trimmed.strip_prefix("~/").unwrap_or(trimmed.strip_prefix('~').unwrap_or(trimmed)))
            } else {
                PathBuf::from(trimmed)
            }
        } else if trimmed.starts_with('/') || (trimmed.len() >= 2 && trimmed.as_bytes()[1] == b':') {
            PathBuf::from(trimmed)
        } else {
            self.cwd.join(trimmed)
        };

        let canonical = fs::canonicalize(&path).or_else(|_| {
            if trimmed.starts_with('~') {
                Ok(path.clone())
            } else {
                Err(anyhow::anyhow!("path not found: {}", path.display()))
            }
        })?;

        if !canonical.is_dir() {
            bail!("not a directory: {}", canonical.display());
        }

        self.go_to(canonical)
    }

    pub fn resolve_goto_base(&self, input: &str) -> (PathBuf, String) {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return (self.cwd.clone(), String::new());
        }

        // Expand ~
        let expanded = if trimmed.starts_with('~') {
            if let Some(home) = dirs_home() {
                home.join(trimmed.strip_prefix("~/").unwrap_or(trimmed.strip_prefix('~').unwrap_or(trimmed)))
            } else {
                PathBuf::from(trimmed)
            }
        } else {
            PathBuf::from(trimmed)
        };

        if trimmed.ends_with('/') || trimmed.ends_with('~') {
            // User typed a trailing slash — complete from that directory
            let dir = if trimmed.starts_with('/') || (trimmed.len() >= 2 && trimmed.as_bytes()[1] == b':') {
                expanded.clone()
            } else if trimmed.starts_with('~') {
                expanded.clone()
            } else {
                self.cwd.join(&expanded)
            };
            return (dir, String::new());
        }

        // Split into parent directory + partial name
        match expanded.parent() {
            Some(parent) if parent != expanded => {
                let prefix = expanded
                    .file_name()
                    .map(|f| f.to_string_lossy().to_string())
                    .unwrap_or_default();
                let dir = if expanded.is_absolute() {
                    parent.to_path_buf()
                } else {
                    self.cwd.join(parent)
                };
                (dir, prefix)
            }
            _ => (self.cwd.clone(), trimmed.to_string()),
        }
    }

    pub fn compute_goto_completions(&mut self) {
        let input = self.input_buffer.clone();
        let (dir, prefix) = self.resolve_goto_base(&input);
        let prefix_lower = prefix.to_lowercase();

        self.goto_completions.clear();
        self.goto_completion_idx = 0;

        let entries = match fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        let use_tilde = input.starts_with('~');
        let home = dirs_home();

        let mut dirs: Vec<String> = Vec::new();
        for entry in entries.flatten() {
            let file_type = entry.file_type().ok();
            if !file_type.map_or(false, |ft| ft.is_dir()) {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') {
                continue; // skip hidden dirs in completion
            }
            if !prefix.is_empty() && !name.to_lowercase().starts_with(&prefix_lower) {
                continue;
            }
            // Build the full path string for the completion
            let full_path = dir.join(&name);
            let display = if use_tilde {
                if let Some(ref home) = home {
                    if let Ok(rel) = full_path.strip_prefix(home) {
                        format!("~/{}", rel.display())
                    } else {
                        full_path.to_string_lossy().to_string()
                    }
                } else {
                    full_path.to_string_lossy().to_string()
                }
            } else if full_path.is_absolute() {
                full_path.to_string_lossy().to_string()
            } else {
                // Relative path: reconstruct from input prefix
                let parent_part = input.trim_end_matches(|c: char| c != '/');
                format!("{parent_part}{name}")
            };
            dirs.push(display);
        }

        dirs.sort();
        self.goto_completions = dirs;
    }

    pub fn goto_tab_complete(&mut self) {
        if self.goto_completions.is_empty() {
            self.compute_goto_completions();
            if self.goto_completions.is_empty() {
                return;
            }
        }

        let completion = self.goto_completions[self.goto_completion_idx].clone();
        self.input_buffer = format!("{completion}/");
        self.goto_completion_idx = (self.goto_completion_idx + 1) % self.goto_completions.len();
    }

    pub fn start_grep(&mut self, query: String) {
        self.grep_results.clear();
        self.grep_active = true;
        self.status = format!("Grepping: {query}");

        let dir = self.cwd.clone();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();

        self.grep_result_tx = result_tx;
        self.grep_result_rx = result_rx;
        self.grep_done_tx = done_tx;
        self.grep_done_rx = done_rx;

        let tx = self.grep_result_tx.clone();
        let done_tx = self.grep_done_tx.clone();

        std::thread::spawn(move || {
            run_grep_search(&dir, &query, tx);
            let _ = done_tx.send(true);
        });
    }

    pub fn pump_grep_results(&mut self) -> bool {
        if !self.grep_active {
            return false;
        }

        let mut changed = false;
        while let Ok(result) = self.grep_result_rx.try_recv() {
            self.grep_results.push(result);
            changed = true;
        }

        if changed && self.command_mode == CommandMode::Grep {
            let count = self.grep_results.len();
            self.status = format!("Grepping... {} matches found", count);
        }

        // Check if search is done
        if self.grep_done_rx.try_recv().is_ok() {
            self.grep_active = false;
            let count = self.grep_results.len();
            self.status = format!("Grep complete: {} matches", count);
            changed = true;
        }

        changed
    }

    pub fn goto_grep_result(&mut self) -> Result<()> {
        let Some(result) = self.grep_results.get(self.selected).cloned() else {
            return Ok(());
        };

        // Open the file in editor at the matching line
        let path = result.path.clone();
        let line = result.line_number;

        let target = if prefers_system_open(&path) {
            OpenTarget::SystemDefault
        } else if let Some(editor) = preferred_terminal_editor() {
            OpenTarget::TerminalEditor {
                editor: editor.to_string(),
                detached: can_spawn_editor_tab(),
            }
        } else {
            OpenTarget::SystemDefault
        };

        self.status = format!(
            "Opening {}:{} in editor | {}",
            path.display(),
            line,
            result.line_content.trim()
        );

        self.grep_viewing = false;
        self.command_mode = CommandMode::Normal;
        self.input_buffer.clear();

        self.pending_open = Some(PendingOpen {
            path,
            target,
            line_number: Some(line),
        });
        Ok(())
    }




    pub fn toggle_hidden(&mut self) -> Result<()> {
        self.show_hidden = !self.show_hidden;
        self.reload_entries()
    }

    pub fn toggle_sort(&mut self) -> Result<()> {
        self.sort_mode = self.sort_mode.next();
        self.reload_entries()
    }

    pub fn toggle_image_mode(&mut self) {
        if let Some(entry) = self.selected_entry() {
            if is_visual_preview(&entry.path) {
                self.image_mode = self.image_mode.toggle();
                self.mark_image_dirty(false);
                self.refresh_preview();
                return;
            }
        }
        self.status = String::from("Selected file has no visual preview");
    }

    pub fn preview_scroll_down(&mut self) {
        self.preview.scroll_y = min(
            self.preview.scroll_y.saturating_add(1),
            self.preview.max_scroll_y,
        );
    }

    pub fn preview_scroll_up(&mut self) {
        self.preview.scroll_y = self.preview.scroll_y.saturating_sub(1);
    }

    pub fn preview_page_down(&mut self) {
        let step = self.preview_area.height.saturating_sub(2).max(1);
        self.preview.scroll_y = min(
            self.preview.scroll_y.saturating_add(step),
            self.preview.max_scroll_y,
        );
    }

    pub fn preview_page_up(&mut self) {
        let step = self.preview_area.height.saturating_sub(2).max(1);
        self.preview.scroll_y = self.preview.scroll_y.saturating_sub(step);
    }

    pub fn begin_mode(&mut self, mode: CommandMode, initial: String) {
        self.command_mode = mode;
        self.input_buffer = initial;
    }

    pub fn selected_name(&self) -> String {
        self.selected_entry()
            .map(|e| e.name.clone())
            .unwrap_or_default()
    }

    pub fn selected_path(&self) -> Option<PathBuf> {
        self.selected_entry().map(|e| e.path.clone())
    }

    pub fn commit_command(&mut self) -> Result<()> {
        match self.command_mode {
            CommandMode::Normal | CommandMode::DeleteConfirm => {}
            CommandMode::Search => {
                self.search_query = self.input_buffer.clone();
                self.command_mode = CommandMode::Normal;
                self.apply_filter();
                self.mark_image_dirty(true);
                self.refresh_preview();
                self.status = format!("Search: {}", self.search_query);
            }
            CommandMode::Rename => {
                let src = self
                    .selected_path()
                    .ok_or_else(|| anyhow!("nothing selected"))?;
                let new_name = self.input_buffer.trim().to_string();
                if new_name.is_empty() {
                    bail!("new name is empty");
                }
                let dst = src.parent().unwrap_or(&self.cwd).join(&new_name);
                fs::rename(&src, &dst)?;
                self.command_mode = CommandMode::Normal;
                self.input_buffer.clear();
                self.reload_entries()?;
                self.status = format!("Renamed to {}", new_name);
            }
            CommandMode::NewFile => {
                let target = self.cwd.join(self.input_buffer.trim());
                if self.input_buffer.trim().is_empty() {
                    bail!("file name is empty");
                }
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::File::create(&target)?;
                self.command_mode = CommandMode::Normal;
                self.input_buffer.clear();
                self.reload_entries()?;
                self.status = format!("Created file {}", target.display());
            }
            CommandMode::NewDir => {
                let target = self.cwd.join(self.input_buffer.trim());
                if self.input_buffer.trim().is_empty() {
                    bail!("directory name is empty");
                }
                fs::create_dir_all(&target)?;
                self.command_mode = CommandMode::Normal;
                self.input_buffer.clear();
                self.reload_entries()?;
                self.status = format!("Created directory {}", target.display());
            }
            CommandMode::Copy => {
                let src = self
                    .selected_path()
                    .ok_or_else(|| anyhow!("nothing selected"))?;
                let raw = self.input_buffer.trim();
                if raw.is_empty() {
                    bail!("destination is empty");
                }
                let dst = resolve_destination(&self.cwd, raw, &src);
                copy_path(&src, &dst)?;
                self.command_mode = CommandMode::Normal;
                self.input_buffer.clear();
                self.reload_entries()?;
                self.status = format!("Copied to {}", dst.display());
            }
            CommandMode::Move => {
                let src = self
                    .selected_path()
                    .ok_or_else(|| anyhow!("nothing selected"))?;
                let raw = self.input_buffer.trim();
                if raw.is_empty() {
                    bail!("destination is empty");
                }
                let dst = resolve_destination(&self.cwd, raw, &src);
                fs::rename(&src, &dst)?;
                self.command_mode = CommandMode::Normal;
                self.input_buffer.clear();
                self.reload_entries()?;
                self.status = format!("Moved to {}", dst.display());
            }
            CommandMode::GoTo => {
                let input = self.input_buffer.clone();
                self.command_mode = CommandMode::Normal;
                self.input_buffer.clear();
                self.go_to_path_input(&input)?;
            }
            CommandMode::Grep => {
                let query = self.input_buffer.trim().to_string();
                if query.is_empty() {
                    bail!("search query is empty");
                }
                self.command_mode = CommandMode::Normal;
                self.input_buffer.clear();
                self.grep_viewing = true;
                self.selected = 0;
                self.list_offset = 0;
                self.start_grep(query);
            }
        }
        Ok(())
    }

    pub fn delete_selected(&mut self) -> Result<()> {
        let path = self
            .selected_path()
            .ok_or_else(|| anyhow!("nothing selected"))?;

        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }

        self.command_mode = CommandMode::Normal;
        self.input_buffer.clear();

        if self.selected > 0 {
            self.selected -= 1;
        }

        self.reload_entries()?;
        self.status = format!("Deleted {}", path.display());
        Ok(())
    }

    pub fn cancel_command(&mut self) {
        self.command_mode = CommandMode::Normal;
        self.input_buffer.clear();
        self.status = String::from("Canceled");
    }

    pub fn open_context_menu(&mut self, x: u16, y: u16) {
        // Don't open context menu during input modes
        if self.command_mode != CommandMode::Normal {
            return;
        }
        let clicked_idx = self.click_index(x, y);

        let (target_path, actions) = if let Some(idx) = clicked_idx {
            // Right-clicked on an entry
            self.set_selected(idx);
            let entry = &self.entries[self.filtered_indices[idx]];
            let actions = vec![
                ContextAction::Open,
                ContextAction::OpenEditor,
                ContextAction::Rename,
                ContextAction::Copy,
                ContextAction::Move,
                ContextAction::Delete,
                ContextAction::CopyPath,
                ContextAction::NewFile,
                ContextAction::NewDir,
                ContextAction::ToggleHidden,
                ContextAction::SortMode,
            ];
            (Some(entry.path.clone()), actions)
        } else {
            // Right-clicked on empty area
            let actions = vec![
                ContextAction::NewFile,
                ContextAction::NewDir,
                ContextAction::ToggleHidden,
                ContextAction::SortMode,
                ContextAction::CopyPath,
            ];
            (None, actions)
        };

        self.context_menu = Some(ContextMenu {
            actions,
            selected: 0,
            x,
            y,
            target_path,
        });
    }

    pub fn close_context_menu(&mut self) {
        self.context_menu = None;
    }

    pub fn context_menu_select_next(&mut self) {
        if let Some(menu) = self.context_menu.as_mut() {
            if !menu.actions.is_empty() {
                menu.selected = (menu.selected + 1) % menu.actions.len();
            }
        }
    }

    pub fn context_menu_select_prev(&mut self) {
        if let Some(menu) = self.context_menu.as_mut() {
            if !menu.actions.is_empty() {
                menu.selected = menu.selected.checked_sub(1).unwrap_or(menu.actions.len() - 1);
            }
        }
    }

    pub fn execute_context_action(&mut self) -> Result<()> {
        let Some(menu) = self.context_menu.take() else {
            return Ok(());
        };
        let Some(action) = menu.actions.get(menu.selected).copied() else {
            return Ok(());
        };

        match action {
            ContextAction::Open => {
                if menu.target_path.is_some() {
                    self.open_selected()?;
                }
            }
            ContextAction::OpenEditor => {
                self.queue_open_selected()?;
            }
            ContextAction::Rename => {
                self.begin_mode(CommandMode::Rename, self.selected_name());
            }
            ContextAction::Copy => {
                self.begin_mode(CommandMode::Copy, self.selected_name());
            }
            ContextAction::Move => {
                self.begin_mode(CommandMode::Move, self.selected_name());
            }
            ContextAction::Delete => {
                self.command_mode = CommandMode::DeleteConfirm;
            }
            ContextAction::NewFile => {
                self.begin_mode(CommandMode::NewFile, String::new());
            }
            ContextAction::NewDir => {
                self.begin_mode(CommandMode::NewDir, String::new());
            }
            ContextAction::ToggleHidden => {
                self.toggle_hidden()?;
            }
            ContextAction::SortMode => {
                self.toggle_sort()?;
            }
            ContextAction::CopyPath => {
                if let Some(path) = self.selected_path() {
                    let path_str = path.to_string_lossy().to_string();
                    // Copy to clipboard via pbcopy (macOS) or xclip (Linux)
                    #[cfg(target_os = "macos")]
                    {
                        use std::io::Write;
                        use std::process::Stdio;
                        if let Ok(mut child) = Command::new("pbcopy")
                            .stdin(Stdio::piped())
                            .spawn()
                        {
                            if let Some(ref mut stdin) = child.stdin {
                                let _ = stdin.write_all(path_str.as_bytes());
                            }
                            let _ = child.wait();
                        }
                    }
                    #[cfg(target_os = "linux")]
                    {
                        use std::io::Write;
                        use std::process::Stdio;
                        if let Ok(mut child) = Command::new("xclip")
                            .args(["-selection", "clipboard"])
                            .stdin(Stdio::piped())
                            .spawn()
                        {
                            if let Some(ref mut stdin) = child.stdin {
                                let _ = stdin.write_all(path_str.as_bytes());
                            }
                            let _ = child.wait();
                        }
                    }
                    self.status = format!("Copied path: {}", path.display());
                }
            }
        }
        Ok(())
    }

    pub fn handle_normal_key(&mut self, key: KeyEvent) -> Result<()> {
        // Context menu takes priority when open
        if self.context_menu.is_some() {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => self.close_context_menu(),
                KeyCode::Down | KeyCode::Char('j') => self.context_menu_select_next(),
                KeyCode::Up | KeyCode::Char('k') => self.context_menu_select_prev(),
                KeyCode::Enter => self.execute_context_action()?,
                _ => {}
            }
            return Ok(());
        }

        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Down | KeyCode::Char('j') => {
                if self.grep_viewing {
                    if self.selected + 1 < self.grep_results.len() {
                        self.selected += 1;
                        self.ensure_visible();
                    }
                } else {
                    self.select_next();
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.grep_viewing {
                    self.selected = self.selected.saturating_sub(1);
                    self.ensure_visible();
                } else {
                    self.select_prev();
                }
            }
            KeyCode::PageDown => {
                if self.grep_viewing {
                    let step = self.visible_rows().max(1);
                    self.selected = (self.selected + step).min(self.grep_results.len().saturating_sub(1));
                    self.ensure_visible();
                } else {
                    self.page_down();
                }
            }
            KeyCode::PageUp => {
                if self.grep_viewing {
                    let step = self.visible_rows().max(1);
                    self.selected = self.selected.saturating_sub(step);
                    self.ensure_visible();
                } else {
                    self.page_up();
                }
            }
            KeyCode::Char('d') if key.modifiers == KeyModifiers::CONTROL => {
                if self.grep_viewing {
                    let step = self.visible_rows().max(1) / 2;
                    self.selected = (self.selected + step).min(self.grep_results.len().saturating_sub(1));
                    self.ensure_visible();
                } else {
                    self.half_page_down();
                }
            }
            KeyCode::Char('u') if key.modifiers == KeyModifiers::CONTROL => {
                if self.grep_viewing {
                    let step = self.visible_rows().max(1) / 2;
                    self.selected = self.selected.saturating_sub(step);
                    self.ensure_visible();
                } else {
                    self.half_page_up();
                }
            }
            KeyCode::Home => {
                if self.grep_viewing {
                    self.selected = 0;
                    self.list_offset = 0;
                } else {
                    self.set_selected(0);
                }
            }
            KeyCode::End => {
                if self.grep_viewing {
                    if !self.grep_results.is_empty() {
                        self.selected = self.grep_results.len() - 1;
                        self.ensure_visible();
                    }
                } else if !self.filtered_indices.is_empty() {
                    self.set_selected(self.filtered_indices.len() - 1)
                }
            }
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                if self.grep_viewing {
                    self.goto_grep_result()?;
                } else {
                    self.open_selected()?;
                }
            }
            KeyCode::Backspace | KeyCode::Left | KeyCode::Char('h') => self.go_parent()?,
            KeyCode::Char('.') => self.toggle_hidden()?,
            KeyCode::Char('r') => self.reload_entries()?,
            KeyCode::Char('s') => self.toggle_sort()?,
            KeyCode::Char('/') => self.begin_mode(CommandMode::Search, self.search_query.clone()),
            KeyCode::Char('R') => self.begin_mode(CommandMode::Rename, self.selected_name()),
            KeyCode::Char('n') => self.begin_mode(CommandMode::NewFile, String::new()),
            KeyCode::Char('N') => self.begin_mode(CommandMode::NewDir, String::new()),
            KeyCode::Char('c') => self.begin_mode(CommandMode::Copy, self.selected_name()),
            KeyCode::Char('m') => self.begin_mode(CommandMode::Move, self.selected_name()),
            KeyCode::Char('d') => self.command_mode = CommandMode::DeleteConfirm,
            KeyCode::Char('i') => self.toggle_image_mode(),
            KeyCode::Char('o') => self.queue_open_selected()?,
            KeyCode::Char('p') => self.cycle_image_protocol(),
            KeyCode::Char('J') => self.preview_scroll_down(),
            KeyCode::Char('K') => self.preview_scroll_up(),
            KeyCode::Char('F') => self.preview_page_down(),
            KeyCode::Char('B') => self.preview_page_up(),
            KeyCode::Char('-') => self.go_back()?,
            KeyCode::Char('_') => self.go_forward()?,
            KeyCode::Char('g') => self.begin_mode(CommandMode::GoTo, String::new()),
            KeyCode::Char('G') => self.begin_mode(CommandMode::Grep, String::new()),
            KeyCode::Char('0') => self.go_to(self.cwd.clone())?, // noop refresh
            KeyCode::Esc => {
                if self.grep_viewing {
                    self.grep_viewing = false;
                    self.grep_results.clear();
                    self.selected = 0;
                    self.list_offset = 0;
                    self.status = String::from("Exited grep view");
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub fn handle_input_key(&mut self, key: KeyEvent) -> Result<()> {
        match self.command_mode {
            CommandMode::DeleteConfirm => match key.code {
                KeyCode::Esc | KeyCode::Char('n') => self.cancel_command(),
                KeyCode::Char('y') => self.delete_selected()?,
                _ => {}
            },
            CommandMode::GoTo => match key.code {
                KeyCode::Esc => self.cancel_command(),
                KeyCode::Enter => self.commit_command()?,
                KeyCode::Backspace => {
                    self.input_buffer.pop();
                    self.goto_completions.clear();
                    self.goto_completion_idx = 0;
                }
                KeyCode::Tab => {
                    self.goto_tab_complete();
                }
                KeyCode::Char(c)
                    if key.modifiers.is_empty()
                        || key.modifiers == KeyModifiers::SHIFT
                        || key.modifiers == KeyModifiers::ALT =>
                {
                    self.input_buffer.push(c);
                    self.goto_completions.clear();
                    self.goto_completion_idx = 0;
                }
                _ => {}
            },
            CommandMode::Grep => match key.code {
                KeyCode::Esc => {
                    self.command_mode = CommandMode::Normal;
                    self.input_buffer.clear();
                    self.status = String::from("Grep canceled");
                }
                KeyCode::Enter => self.commit_command()?,
                KeyCode::Backspace => {
                    self.input_buffer.pop();
                }
                KeyCode::Char(c)
                    if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
                {
                    self.input_buffer.push(c);
                }
                _ => {}
            },
            CommandMode::Search
            | CommandMode::Rename
            | CommandMode::NewFile
            | CommandMode::NewDir
            | CommandMode::Copy
            | CommandMode::Move => match key.code {
                KeyCode::Esc => self.cancel_command(),
                KeyCode::Enter => self.commit_command()?,
                KeyCode::Backspace => {
                    self.input_buffer.pop();
                    if self.command_mode == CommandMode::Search {
                        self.search_query = self.input_buffer.clone();
                        self.apply_filter();
                        self.mark_image_dirty(true);
                        self.refresh_preview();
                    }
                }
                KeyCode::Char(c)
                    if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
                {
                    self.input_buffer.push(c);
                    if self.command_mode == CommandMode::Search {
                        self.search_query = self.input_buffer.clone();
                        self.apply_filter();
                        self.mark_image_dirty(true);
                        self.refresh_preview();
                    }
                }
                _ => {}
            },
            CommandMode::Normal => {}
        }
        Ok(())
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Result<()> {
        match self.command_mode {
            CommandMode::Normal => self.handle_normal_key(key),
            _ => self.handle_input_key(key),
        }
    }

    pub fn click_index(&self, x: u16, y: u16) -> Option<usize> {
        if !contains(self.list_area, x, y) {
            return None;
        }

        let top = self.list_area.y.saturating_add(1);
        let bottom = self
            .list_area
            .y
            .saturating_add(self.list_area.height.saturating_sub(2));

        if y < top || y > bottom {
            return None;
        }

        let row = y.saturating_sub(top) as usize;
        let idx = self.list_offset + row;
        (idx < self.filtered_indices.len()).then_some(idx)
    }

    pub fn clicked_breadcrumb(&self, x: u16, y: u16) -> Option<PathBuf> {
        self.breadcrumb_hits
            .iter()
            .find(|hit| contains(hit.rect, x, y))
            .map(|hit| hit.target.clone())
    }

    pub fn is_double_click(&self, x: u16, y: u16, now: Instant) -> bool {
        self.last_click.as_ref().is_some_and(|(lx, ly, t)| {
            *lx == x && *ly == y && now.duration_since(*t) <= Duration::from_millis(DOUBLE_CLICK_MS)
        })
    }

    pub fn handle_mouse(&mut self, mouse: MouseEvent) -> Result<()> {
        if self.command_mode == CommandMode::DeleteConfirm {
            return Ok(());
        }

        let x = mouse.column;
        let y = mouse.row;

        match mouse.kind {
            MouseEventKind::ScrollDown => {
                if self.context_menu.is_some() {
                    self.context_menu_select_next();
                    return Ok(());
                }
                if contains(self.preview_area, x, y) {
                    self.preview_scroll_down();
                } else {
                    self.select_next();
                }
            }
            MouseEventKind::ScrollUp => {
                if self.context_menu.is_some() {
                    self.context_menu_select_prev();
                    return Ok(());
                }
                if contains(self.preview_area, x, y) {
                    self.preview_scroll_up();
                } else {
                    self.select_prev();
                }
            }
            MouseEventKind::Down(MouseButton::Right) => {
                self.open_context_menu(x, y);
            }
            MouseEventKind::Down(MouseButton::Left) => {
                // If context menu is open, click outside closes it
                if let Some(ref menu) = self.context_menu {
                    let raw = context_menu_rect(menu);
                    // Clamp to a reasonable screen area for hit testing
                    let menu_h = menu.actions.len() as u16 + 2;
                    let clamped = Rect::new(
                        raw.x.min(200),
                        raw.y.min(60),
                        raw.width,
                        menu_h.min(60),
                    );
                    if !contains(clamped, x, y) {
                        self.close_context_menu();
                        return Ok(());
                    }
                    // Click inside menu: select the item
                    let menu_inner_y = clamped.y + 1; // skip border
                    if y >= menu_inner_y {
                        let idx = (y - menu_inner_y) as usize;
                        if idx < menu.actions.len() {
                            if let Some(m) = self.context_menu.as_mut() {
                                m.selected = idx;
                            }
                            self.execute_context_action()?;
                        }
                    }
                    return Ok(());
                }
                if let Some(target) = self.clicked_breadcrumb(x, y) {
                    self.go_to(target)?;
                    self.last_click = Some((x, y, Instant::now()));
                    return Ok(());
                }

                if let Some(idx) = self.click_index(x, y) {
                    let was_selected = self.selected == idx;
                    self.set_selected(idx);

                    let now = Instant::now();
                    let is_double = self.is_double_click(x, y, now);
                    self.last_click = Some((x, y, now));

                    if is_double || was_selected {
                        self.open_selected()?;
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    pub fn handle_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::Key(key) => self.handle_key(key)?,
            Event::Mouse(mouse) => self.handle_mouse(mouse)?,
            Event::Resize(_, _) => {
                self.refresh_native_metrics();
                self.native_needs_full_clear = true;
                self.refresh_preview();
            }
            Event::FocusGained => {
                // The terminal contents may have changed while another tab/editor was active,
                // so invalidate ratatui's previous frame assumptions before the next draw.
                self.refresh_native_metrics();
                self.native_needs_full_clear = true;
            }
            Event::FocusLost | Event::Paste(_) => {}
        }
        Ok(())
    }
}

fn shell_quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Start a temporary HTTP server to serve a local HTML file.
/// Returns the URL to access it. The server auto-terminates after ~300s.
fn url_encode_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{byte:02X}"));
            }
        }
    }
    out
}

/// Start a temporary HTTP server to serve a local HTML file.
/// Returns the URL to access it. The server auto-terminates after ~300s.
fn serve_html_via_http(path: &Path) -> String {
    use std::net::TcpListener;

    let port = TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|l| l.local_addr().ok())
        .map(|addr| addr.port())
        .unwrap_or(8765);

    let dir = path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let file_name = path
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_default();

    let _ = Command::new("sh")
        .args([
            "-lc",
            &format!(
                "exec python3 -m http.server {} --directory '{}' --bind 127.0.0.1 2>/dev/null",
                port,
                dir.display(),
            ),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();

    let encoded = url_encode_path(&file_name);
    format!("http://127.0.0.1:{port}/{encoded}")
}

fn awrit_available() -> bool {
    Command::new("awrit")
        .arg("--help")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

fn prefers_system_open(path: &Path) -> bool {
    if is_image_path(path) {
        return true;
    }

    matches!(
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase()),
        Some(ext) if matches!(
            ext.as_str(),
            "pdf"
                | "doc"
                | "docx"
                | "ppt"
                | "pptx"
                | "xls"
                | "xlsx"
                | "numbers"
                | "pages"
                | "key"
                | "odt"
                | "ods"
                | "odp"
                | "rtf"
                | "mp4"
                | "mov"
                | "avi"
                | "mkv"
                | "webm"
                | "mp3"
                | "wav"
                | "flac"
                | "aac"
                | "m4a"
        )
    )
}

fn can_spawn_editor_tab() -> bool {
    std::env::var("WEZTERM_PANE").is_ok_and(|value| !value.is_empty())
        && std::env::var("WEZTERM_EXECUTABLE").is_ok_and(|value| !value.is_empty())
}

fn preferred_terminal_editor() -> Option<&'static str> {
    if std::env::var("VISUAL").is_ok_and(|value| !value.trim().is_empty()) {
        return Some("$VISUAL");
    }
    if std::env::var("EDITOR").is_ok_and(|value| !value.trim().is_empty()) {
        return Some("$EDITOR");
    }

    ["nvim", "vim", "hx", "nano", "vi"]
        .into_iter()
        .find(|candidate| editor_command_available(candidate))
}

fn editor_command_available(command: &str) -> bool {
    Command::new(command).arg("--version").output().is_ok()
}

fn is_wezterm_or_konsole() -> bool {
    std::env::var("WEZTERM_EXECUTABLE").is_ok_and(|s| !s.is_empty())
        || std::env::var("KONSOLE_VERSION").is_ok_and(|s| !s.is_empty())
}

fn protocol_known_broken(protocol: ProtocolType) -> bool {
    is_wezterm_or_konsole() && matches!(protocol, ProtocolType::Kitty | ProtocolType::Sixel)
}

fn bucket_dimension(value: u32) -> u32 {
    value.max(1).div_ceil(IMAGE_CACHE_BUCKET_PX) * IMAGE_CACHE_BUCKET_PX
}

fn inner_preview_rect(area: Rect) -> Rect {
    Rect::new(
        area.x.saturating_add(1),
        area.y.saturating_add(1),
        area.width.saturating_sub(2),
        area.height.saturating_sub(2),
    )
}

fn next_protocol(current: ProtocolType) -> ProtocolType {
    let mut next = current.next();
    if protocol_known_broken(next) {
        next = next.next();
    }
    next
}

pub fn contains(rect: Rect, x: u16, y: u16) -> bool {
    x >= rect.x
        && x < rect.x.saturating_add(rect.width)
        && y >= rect.y
        && y < rect.y.saturating_add(rect.height)
}

pub fn context_menu_rect(menu: &ContextMenu) -> Rect {
    let width = 22u16;
    let height = menu.actions.len() as u16 + 2; // +2 for border
    Rect::new(menu.x, menu.y, width, height)
}

pub fn breadcrumb_segments(path: &Path) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    let mut acc = PathBuf::new();

    for comp in path.components() {
        match comp {
            Component::Prefix(prefix) => {
                let label = prefix.as_os_str().to_string_lossy().to_string();
                acc.push(prefix.as_os_str());
                out.push((label, acc.clone()));
            }
            Component::RootDir => {
                acc.push(std::path::MAIN_SEPARATOR.to_string());
                out.push((std::path::MAIN_SEPARATOR.to_string(), acc.clone()));
            }
            Component::Normal(name) => {
                acc.push(name);
                out.push((name.to_string_lossy().to_string(), acc.clone()));
            }
            Component::CurDir => out.push((String::from("."), PathBuf::from("."))),
            Component::ParentDir => {
                acc.push("..");
                out.push((String::from(".."), acc.clone()));
            }
        }
    }

    if out.is_empty() {
        out.push((String::from("."), PathBuf::from(".")));
    }

    out
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var("HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
}

const GREP_MAX_FILE_SIZE: u64 = 10 * 1024 * 1024; // 10 MB

fn escape_regex(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        if r"\.+*?^${}()|[]".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

fn run_grep_search(root: &Path, query: &str, tx: Sender<GrepResult>) {
    use grep_regex::RegexMatcherBuilder;
    use grep_searcher::Searcher;

    let escaped = escape_regex(query);
    let pattern = format!("(?i){escaped}");

    let matcher = match RegexMatcherBuilder::new()
        .case_insensitive(true)
        .build(&pattern)
    {
        Ok(m) => m,
        Err(_) => return,
    };

    let mut searcher = Searcher::new();
    walk_dir_for_grep(root, &matcher, &mut searcher, &tx);
}

fn walk_dir_for_grep(
    dir: &Path,
    matcher: &grep_regex::RegexMatcher,
    searcher: &mut grep_searcher::Searcher,
    tx: &Sender<GrepResult>,
) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let file_type = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };

        let path = entry.path();

        if file_type.is_dir() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with('.')
                || name_str == "node_modules"
                || name_str == "target"
                || name_str == ".git"
            {
                continue;
            }
            walk_dir_for_grep(&path, matcher, searcher, tx);
        } else if file_type.is_file() {
            let metadata = match fs::metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if metadata.len() > GREP_MAX_FILE_SIZE {
                continue;
            }

            let path_clone = path.clone();
            let tx_clone = tx.clone();
            let mut count = 0usize;

            // Use search_path which handles file opening and mmap internally
            let result = searcher.search_path(
                matcher,
                &path,
                grep_searcher::sinks::UTF8(|line_number: u64, line_content: &str| {
                    if count >= 200 {
                        return Ok(false);
                    }
                    let result = GrepResult {
                        path: path_clone.clone(),
                        line_number,
                        line_content: line_content.to_string(),
                    };
                    let _ = tx_clone.send(result);
                    count += 1;
                    Ok(true)
                }),
            );

            // Silently skip binary files or encoding errors
            let _ = result;
        }
    }
}
