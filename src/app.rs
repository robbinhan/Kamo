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
        CommandMode, Entry, HitBox, ImagePreviewMode, ImageRenderState, PreviewData, SortMode,
    },
    preview::{
        DEFAULT_PREVIEW_IMAGE_DIMENSION, Highlighter, PreparedImage, build_preview, is_image_path,
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
    pub highlighter: Highlighter,
    pub pending_open: Option<PendingOpen>,
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
            highlighter: Highlighter::new(),
            pending_open: None,
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

        if !is_image_path(&entry_path) || self.image_mode != ImagePreviewMode::Image {
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

    pub fn open_selected(&mut self) -> Result<()> {
        let Some(entry) = self.selected_entry() else {
            return Ok(());
        };

        let is_dir = entry.is_dir;
        let path = entry.path.clone();
        let name = entry.name.clone();

        if is_dir {
            self.cwd = path;
            self.selected = 0;
            self.list_offset = 0;
            self.search_query.clear();
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
        self.pending_open = Some(PendingOpen { path, target });
        Ok(())
    }

    pub fn take_pending_open(&mut self) -> Option<PendingOpen> {
        self.pending_open.take()
    }

    pub fn set_open_result(&mut self, pending: &PendingOpen, success: bool) {
        self.status = match (&pending.target, success) {
            (OpenTarget::TerminalEditor { editor, detached }, true) => {
                if *detached {
                    format!(
                        "Opened in new editor tab: {editor} | {}",
                        pending.path.display()
                    )
                } else {
                    format!("Returned from {editor}: {}", pending.path.display())
                }
            }
            (OpenTarget::TerminalEditor { editor, .. }, false) => {
                format!("Failed to launch {editor}: {}", pending.path.display())
            }
            (OpenTarget::SystemDefault, true) => {
                format!("Opened in default app: {}", pending.path.display())
            }
            (OpenTarget::SystemDefault, false) => {
                format!("Failed to open in default app: {}", pending.path.display())
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

    pub fn go_parent(&mut self) -> Result<()> {
        if let Some(parent) = self.cwd.parent() {
            self.cwd = parent.to_path_buf();
            self.selected = 0;
            self.list_offset = 0;
            self.search_query.clear();
            self.reload_entries()?;
        }
        Ok(())
    }

    pub fn go_to(&mut self, path: PathBuf) -> Result<()> {
        self.cwd = path;
        self.selected = 0;
        self.list_offset = 0;
        self.search_query.clear();
        self.reload_entries()?;
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
            if is_image_path(&entry.path) {
                self.image_mode = self.image_mode.toggle();
                self.mark_image_dirty(false);
                self.refresh_preview();
                return;
            }
        }
        self.status = String::from("Selected file is not an image");
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

    pub fn handle_normal_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Down | KeyCode::Char('j') => self.select_next(),
            KeyCode::Up | KeyCode::Char('k') => self.select_prev(),
            KeyCode::PageDown => self.page_down(),
            KeyCode::PageUp => self.page_up(),
            KeyCode::Home => self.set_selected(0),
            KeyCode::End => {
                if !self.filtered_indices.is_empty() {
                    self.set_selected(self.filtered_indices.len() - 1)
                }
            }
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => self.open_selected()?,
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
                if contains(self.preview_area, x, y) {
                    self.preview_scroll_down();
                } else {
                    self.select_next();
                }
            }
            MouseEventKind::ScrollUp => {
                if contains(self.preview_area, x, y) {
                    self.preview_scroll_up();
                } else {
                    self.select_prev();
                }
            }
            MouseEventKind::Down(MouseButton::Left) => {
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
