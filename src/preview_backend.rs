use std::{
    io::{self, Write},
    path::Path,
    process::Command,
};

use anyhow::Result;
use base64::{Engine, engine::general_purpose::STANDARD};
use crossterm::{
    cursor::{MoveTo, RestorePosition, SavePosition},
    queue,
};
use image::{
    ExtendedColorType, ImageEncoder,
    codecs::{jpeg::JpegEncoder, png::PngEncoder},
};
use ratatui::layout::Rect;
use serde::Deserialize;

use crate::preview::PreparedImage;

const ESC: &str = "\x1b";
const ST: &str = "\x1b\\";
const KITTY_CHUNK_SIZE: usize = 4096;

static KITTY_DIACRITICS: [char; 297] = [
    '\u{0305}',
    '\u{030D}',
    '\u{030E}',
    '\u{0310}',
    '\u{0312}',
    '\u{033D}',
    '\u{033E}',
    '\u{033F}',
    '\u{0346}',
    '\u{034A}',
    '\u{034B}',
    '\u{034C}',
    '\u{0350}',
    '\u{0351}',
    '\u{0352}',
    '\u{0357}',
    '\u{035B}',
    '\u{0363}',
    '\u{0364}',
    '\u{0365}',
    '\u{0366}',
    '\u{0367}',
    '\u{0368}',
    '\u{0369}',
    '\u{036A}',
    '\u{036B}',
    '\u{036C}',
    '\u{036D}',
    '\u{036E}',
    '\u{036F}',
    '\u{0483}',
    '\u{0484}',
    '\u{0485}',
    '\u{0486}',
    '\u{0487}',
    '\u{0592}',
    '\u{0593}',
    '\u{0594}',
    '\u{0595}',
    '\u{0597}',
    '\u{0598}',
    '\u{0599}',
    '\u{059C}',
    '\u{059D}',
    '\u{059E}',
    '\u{059F}',
    '\u{05A0}',
    '\u{05A1}',
    '\u{05A8}',
    '\u{05A9}',
    '\u{05AB}',
    '\u{05AC}',
    '\u{05AF}',
    '\u{05C4}',
    '\u{0610}',
    '\u{0611}',
    '\u{0612}',
    '\u{0613}',
    '\u{0614}',
    '\u{0615}',
    '\u{0616}',
    '\u{0617}',
    '\u{0657}',
    '\u{0658}',
    '\u{0659}',
    '\u{065A}',
    '\u{065B}',
    '\u{065D}',
    '\u{065E}',
    '\u{06D6}',
    '\u{06D7}',
    '\u{06D8}',
    '\u{06D9}',
    '\u{06DA}',
    '\u{06DB}',
    '\u{06DC}',
    '\u{06DF}',
    '\u{06E0}',
    '\u{06E1}',
    '\u{06E2}',
    '\u{06E4}',
    '\u{06E7}',
    '\u{06E8}',
    '\u{06EB}',
    '\u{06EC}',
    '\u{0730}',
    '\u{0732}',
    '\u{0733}',
    '\u{0735}',
    '\u{0736}',
    '\u{073A}',
    '\u{073D}',
    '\u{073F}',
    '\u{0740}',
    '\u{0741}',
    '\u{0743}',
    '\u{0745}',
    '\u{0747}',
    '\u{0749}',
    '\u{074A}',
    '\u{07EB}',
    '\u{07EC}',
    '\u{07ED}',
    '\u{07EE}',
    '\u{07EF}',
    '\u{07F0}',
    '\u{07F1}',
    '\u{07F3}',
    '\u{0816}',
    '\u{0817}',
    '\u{0818}',
    '\u{0819}',
    '\u{081B}',
    '\u{081C}',
    '\u{081D}',
    '\u{081E}',
    '\u{081F}',
    '\u{0820}',
    '\u{0821}',
    '\u{0822}',
    '\u{0823}',
    '\u{0825}',
    '\u{0826}',
    '\u{0827}',
    '\u{0829}',
    '\u{082A}',
    '\u{082B}',
    '\u{082C}',
    '\u{082D}',
    '\u{0951}',
    '\u{0953}',
    '\u{0954}',
    '\u{0F82}',
    '\u{0F83}',
    '\u{0F86}',
    '\u{0F87}',
    '\u{135D}',
    '\u{135E}',
    '\u{135F}',
    '\u{17DD}',
    '\u{193A}',
    '\u{1A17}',
    '\u{1A75}',
    '\u{1A76}',
    '\u{1A77}',
    '\u{1A78}',
    '\u{1A79}',
    '\u{1A7A}',
    '\u{1A7B}',
    '\u{1A7C}',
    '\u{1B6B}',
    '\u{1B6D}',
    '\u{1B6E}',
    '\u{1B6F}',
    '\u{1B70}',
    '\u{1B71}',
    '\u{1B72}',
    '\u{1B73}',
    '\u{1CD0}',
    '\u{1CD1}',
    '\u{1CD2}',
    '\u{1CDA}',
    '\u{1CDB}',
    '\u{1CE0}',
    '\u{1DC0}',
    '\u{1DC1}',
    '\u{1DC3}',
    '\u{1DC4}',
    '\u{1DC5}',
    '\u{1DC6}',
    '\u{1DC7}',
    '\u{1DC8}',
    '\u{1DC9}',
    '\u{1DCB}',
    '\u{1DCC}',
    '\u{1DD1}',
    '\u{1DD2}',
    '\u{1DD3}',
    '\u{1DD4}',
    '\u{1DD5}',
    '\u{1DD6}',
    '\u{1DD7}',
    '\u{1DD8}',
    '\u{1DD9}',
    '\u{1DDA}',
    '\u{1DDB}',
    '\u{1DDC}',
    '\u{1DDD}',
    '\u{1DDE}',
    '\u{1DDF}',
    '\u{1DE0}',
    '\u{1DE1}',
    '\u{1DE2}',
    '\u{1DE3}',
    '\u{1DE4}',
    '\u{1DE5}',
    '\u{1DE6}',
    '\u{1DFE}',
    '\u{20D0}',
    '\u{20D1}',
    '\u{20D4}',
    '\u{20D5}',
    '\u{20D6}',
    '\u{20D7}',
    '\u{20DB}',
    '\u{20DC}',
    '\u{20E1}',
    '\u{20E7}',
    '\u{20E9}',
    '\u{20F0}',
    '\u{2CEF}',
    '\u{2CF0}',
    '\u{2CF1}',
    '\u{2DE0}',
    '\u{2DE1}',
    '\u{2DE2}',
    '\u{2DE3}',
    '\u{2DE4}',
    '\u{2DE5}',
    '\u{2DE6}',
    '\u{2DE7}',
    '\u{2DE8}',
    '\u{2DE9}',
    '\u{2DEA}',
    '\u{2DEB}',
    '\u{2DEC}',
    '\u{2DED}',
    '\u{2DEE}',
    '\u{2DEF}',
    '\u{2DF0}',
    '\u{2DF1}',
    '\u{2DF2}',
    '\u{2DF3}',
    '\u{2DF4}',
    '\u{2DF5}',
    '\u{2DF6}',
    '\u{2DF7}',
    '\u{2DF8}',
    '\u{2DF9}',
    '\u{2DFA}',
    '\u{2DFB}',
    '\u{2DFC}',
    '\u{2DFD}',
    '\u{2DFE}',
    '\u{2DFF}',
    '\u{A66F}',
    '\u{A67C}',
    '\u{A67D}',
    '\u{A6F0}',
    '\u{A6F1}',
    '\u{A8E0}',
    '\u{A8E1}',
    '\u{A8E2}',
    '\u{A8E3}',
    '\u{A8E4}',
    '\u{A8E5}',
    '\u{A8E6}',
    '\u{A8E7}',
    '\u{A8E8}',
    '\u{A8E9}',
    '\u{A8EA}',
    '\u{A8EB}',
    '\u{A8EC}',
    '\u{A8ED}',
    '\u{A8EE}',
    '\u{A8EF}',
    '\u{A8F0}',
    '\u{A8F1}',
    '\u{AAB0}',
    '\u{AAB2}',
    '\u{AAB3}',
    '\u{AAB7}',
    '\u{AAB8}',
    '\u{AABE}',
    '\u{AABF}',
    '\u{AAC1}',
    '\u{FE20}',
    '\u{FE21}',
    '\u{FE22}',
    '\u{FE23}',
    '\u{FE24}',
    '\u{FE25}',
    '\u{FE26}',
    '\u{10A0F}',
    '\u{10A38}',
    '\u{1D185}',
    '\u{1D186}',
    '\u{1D187}',
    '\u{1D188}',
    '\u{1D189}',
    '\u{1D1AA}',
    '\u{1D1AB}',
    '\u{1D1AC}',
    '\u{1D1AD}',
    '\u{1D242}',
    '\u{1D243}',
    '\u{1D244}',
];

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum NativePreviewBackend {
    WezTerm,
    Kitty,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum NativeRenderFit {
    CellRect,
    WezTermWidth,
    WezTermHeight,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct NativeRenderSpec {
    pub area: Rect,
    pub fit: NativeRenderFit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativePreviewState {
    pub key: u64,
    pub area: Rect,
}

pub struct NativePreviewController {
    backend: NativePreviewBackend,
    shown: Option<NativePreviewState>,
}

impl NativePreviewController {
    pub fn detect() -> Option<Self> {
        detect_native_preview_backend().map(Self::new)
    }

    pub fn new(backend: NativePreviewBackend) -> Self {
        Self {
            backend,
            shown: None,
        }
    }

    pub fn backend(&self) -> NativePreviewBackend {
        self.backend
    }

    pub fn shown(&self) -> Option<&NativePreviewState> {
        self.shown.as_ref()
    }

    pub fn hide(&mut self) -> Result<bool> {
        let Some(shown) = self.shown.take() else {
            return Ok(false);
        };

        erase_area(shown.area)?;
        if matches!(self.backend, NativePreviewBackend::Kitty) {
            delete_kitty_image()?;
        }
        Ok(true)
    }

    pub fn clear(&mut self, area: Rect) -> Result<bool> {
        let had_image = self.shown.take().is_some();
        if area.width > 0 && area.height > 0 {
            erase_area(area)?;
        }
        if matches!(self.backend, NativePreviewBackend::Kitty) && had_image {
            delete_kitty_image()?;
        }
        Ok(had_image)
    }

    pub fn show(&mut self, key: u64, area: Rect, payload: &[u8]) -> Result<bool> {
        if area.width == 0 || area.height == 0 {
            return self.hide();
        }

        if self
            .shown
            .as_ref()
            .is_some_and(|shown| shown.key == key && shown.area == area)
        {
            return Ok(false);
        }

        self.hide()?;
        match self.backend {
            NativePreviewBackend::WezTerm => render_wezterm_payload(area, payload)?,
            NativePreviewBackend::Kitty => render_kitty_payload(area, payload)?,
        }

        self.shown = Some(NativePreviewState { key, area });
        Ok(true)
    }
}

fn detect_native_preview_backend() -> Option<NativePreviewBackend> {
    if std::env::var("WEZTERM_EXECUTABLE").is_ok_and(|value| !value.is_empty())
        || std::env::var("TERM_PROGRAM")
            .map(|value| value.eq_ignore_ascii_case("wezterm"))
            .unwrap_or(false)
    {
        return Some(NativePreviewBackend::WezTerm);
    }

    if std::env::var("KITTY_WINDOW_ID").is_ok_and(|value| !value.is_empty())
        || std::env::var("TERM")
            .map(|value| value == "xterm-kitty")
            .unwrap_or(false)
    {
        return Some(NativePreviewBackend::Kitty);
    }

    None
}

pub fn encode_image_payload(
    backend: NativePreviewBackend,
    image: &PreparedImage,
    spec: NativeRenderSpec,
) -> Result<Vec<u8>> {
    match backend {
        NativePreviewBackend::WezTerm => encode_wezterm_payload(image, spec),
        NativePreviewBackend::Kitty => encode_kitty_payload(image),
    }
}

fn render_wezterm_payload(area: Rect, payload: &[u8]) -> Result<()> {
    with_cursor_lock(area.x, area.y, |stdout| {
        stdout.write_all(payload)?;
        Ok(())
    })
}

fn render_kitty_payload(area: Rect, payload: &[u8]) -> Result<()> {
    let placement = kitty_place(area)?;

    with_cursor_lock(area.x, area.y, |stdout| {
        stdout.write_all(payload)?;
        stdout.write_all(&placement)?;
        Ok(())
    })
}

fn erase_area(area: Rect) -> Result<()> {
    if area.width == 0 || area.height == 0 {
        return Ok(());
    }

    let blank = " ".repeat(area.width as usize);
    with_cursor_lock(0, 0, |stdout| {
        for y in area.top()..area.bottom() {
            queue!(stdout, MoveTo(area.x, y))?;
            write!(stdout, "{blank}")?;
        }
        Ok(())
    })
}

fn delete_kitty_image() -> Result<()> {
    with_cursor_lock(0, 0, |stdout| {
        write!(stdout, "{ESC}_Gq=2,a=d,d=A{ST}")?;
        Ok(())
    })
}

fn with_cursor_lock<F>(x: u16, y: u16, f: F) -> Result<()>
where
    F: FnOnce(&mut io::Stdout) -> Result<()>,
{
    let mut stdout = io::stdout();
    queue!(stdout, SavePosition, MoveTo(x, y))?;
    f(&mut stdout)?;
    queue!(stdout, RestorePosition)?;
    stdout.flush()?;
    Ok(())
}

fn encode_raster_image(image: &PreparedImage) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    if image_has_alpha(image) {
        PngEncoder::new(&mut out).write_image(
            &image.rgba,
            image.preview_dimensions.0,
            image.preview_dimensions.1,
            ExtendedColorType::Rgba8,
        )?;
    } else {
        let rgb = rgba_to_rgb(&image.rgba);
        JpegEncoder::new_with_quality(&mut out, 75).write_image(
            &rgb,
            image.preview_dimensions.0,
            image.preview_dimensions.1,
            ExtendedColorType::Rgb8,
        )?;
    }
    Ok(out)
}

fn encode_wezterm_payload(image: &PreparedImage, spec: NativeRenderSpec) -> Result<Vec<u8>> {
    let raster = encode_raster_image(image)?;
    let encoded = STANDARD.encode(&raster);
    let mut buf = Vec::with_capacity(encoded.len() + 256);

    write!(buf, "{ESC}]1337;File=size={}", raster.len())?;
    match spec.fit {
        NativeRenderFit::WezTermHeight => {
            write!(buf, ";height={}", spec.area.height)?;
        }
        NativeRenderFit::CellRect | NativeRenderFit::WezTermWidth => {
            write!(buf, ";width={}", spec.area.width)?;
        }
    }
    write!(buf, ";inline=1;doNotMoveCursor=1:{encoded}{ST}")?;
    Ok(buf)
}

fn encode_kitty_payload(image: &PreparedImage) -> Result<Vec<u8>> {
    let encoded = STANDARD.encode(&image.rgba);
    let mut chunks = encoded.as_bytes().chunks(KITTY_CHUNK_SIZE).peekable();
    let mut buf = Vec::with_capacity(encoded.len() + 512);
    let id = kitty_image_id();

    if let Some(first) = chunks.next() {
        write!(
            buf,
            "{ESC}_Gq=2,a=T,C=1,U=1,f=32,s={},v={},i={},m={};{}{ST}",
            image.preview_dimensions.0,
            image.preview_dimensions.1,
            id,
            chunks.peek().is_some() as u8,
            std::str::from_utf8(first)?,
        )?;
    }

    while let Some(chunk) = chunks.next() {
        write!(
            buf,
            "{ESC}_Gm={};{}{ST}",
            chunks.peek().is_some() as u8,
            std::str::from_utf8(chunk)?,
        )?;
    }

    Ok(buf)
}

fn kitty_place(area: Rect) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(area.width as usize * area.height as usize * 8 + 128);
    let id = kitty_image_id();
    let (r, g, b) = ((id >> 16) & 0xff, (id >> 8) & 0xff, id & 0xff);
    write!(buf, "{ESC}[38;2;{r};{g};{b}m")?;

    for y in 0..area.height {
        write!(buf, "{ESC}[{};{}H", area.y + y + 1, area.x + 1)?;
        for x in 0..area.width {
            let dy = KITTY_DIACRITICS
                .get(y as usize)
                .copied()
                .unwrap_or(KITTY_DIACRITICS[0]);
            let dx = KITTY_DIACRITICS
                .get(x as usize)
                .copied()
                .unwrap_or(KITTY_DIACRITICS[0]);
            write!(buf, "\u{10EEEE}{dy}{dx}")?;
        }
    }

    write!(buf, "{ESC}[39m")?;
    Ok(buf)
}

fn kitty_image_id() -> u32 {
    std::process::id() % (0x00ff_ffff + 1)
}

fn image_has_alpha(image: &PreparedImage) -> bool {
    image.rgba.chunks_exact(4).any(|chunk| chunk[3] < u8::MAX)
}

fn rgba_to_rgb(rgba: &[u8]) -> Vec<u8> {
    let mut rgb = Vec::with_capacity((rgba.len() / 4) * 3);
    for chunk in rgba.chunks_exact(4) {
        rgb.extend_from_slice(&chunk[..3]);
    }
    rgb
}

pub fn fit_wezterm_render_spec(
    bounds: Rect,
    image_dimensions: (u32, u32),
    cell_size: (u32, u32),
) -> Option<NativeRenderSpec> {
    if bounds.width == 0 || bounds.height == 0 {
        return None;
    }

    let image_width = image_dimensions.0.max(1);
    let image_height = image_dimensions.1.max(1);
    let cell_width = cell_size.0.max(1);
    let cell_height = cell_size.1.max(1);
    let max_cols = u32::from(bounds.width.max(1));
    let max_rows = u32::from(bounds.height.max(1));
    let aspect = image_width as f64 / image_height as f64;

    let width_limited_rows =
        (((max_cols * cell_width) as f64 / aspect) / cell_height as f64).ceil() as u32;

    let (render_cols, render_rows, fit) = if width_limited_rows <= max_rows {
        (
            max_cols,
            width_limited_rows.max(1),
            NativeRenderFit::WezTermWidth,
        )
    } else {
        let height_limited_cols =
            (((max_rows * cell_height) as f64 * aspect) / cell_width as f64).ceil() as u32;
        (
            height_limited_cols.clamp(1, max_cols),
            max_rows,
            NativeRenderFit::WezTermHeight,
        )
    };

    let render_width = render_cols.clamp(1, max_cols) as u16;
    let render_height = render_rows.clamp(1, max_rows) as u16;
    Some(NativeRenderSpec {
        area: Rect::new(
            bounds.x + bounds.width.saturating_sub(render_width) / 2,
            bounds.y + bounds.height.saturating_sub(render_height) / 2,
            render_width,
            render_height,
        ),
        fit,
    })
}

#[derive(Debug, Deserialize)]
struct WezTermCliPaneSize {
    cols: u32,
    rows: u32,
    pixel_width: u32,
    pixel_height: u32,
}

#[derive(Debug, Deserialize)]
struct WezTermCliPane {
    pane_id: u64,
    cwd: String,
    is_active: bool,
    size: WezTermCliPaneSize,
}

pub fn probe_wezterm_cell_size(cwd: &Path) -> Option<(u32, u32)> {
    let pane_id = std::env::var("WEZTERM_PANE")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    let output = Command::new("wezterm")
        .args(["cli", "list", "--format", "json"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let panes: Vec<WezTermCliPane> = serde_json::from_slice(&output.stdout).ok()?;
    let cwd_string = cwd.display().to_string();
    let pane = pane_id
        .and_then(|id| panes.iter().find(|pane| pane.pane_id == id))
        .or_else(|| {
            panes
                .iter()
                .find(|pane| pane.is_active && pane.cwd.ends_with(&cwd_string))
        })
        .or_else(|| panes.iter().find(|pane| pane.is_active))?;

    let cols = pane.size.cols.max(1);
    let rows = pane.size.rows.max(1);
    Some((
        (pane.size.pixel_width / cols).max(1),
        (pane.size.pixel_height / rows).max(1),
    ))
}

#[cfg(test)]
mod tests {
    use super::{NativeRenderFit, fit_wezterm_render_spec};
    use ratatui::layout::Rect;

    #[test]
    fn wezterm_fit_uses_full_width_for_wide_images() {
        let spec = fit_wezterm_render_spec(Rect::new(0, 0, 40, 20), (1600, 900), (9, 22))
            .expect("render spec");
        assert_eq!(spec.fit, NativeRenderFit::WezTermWidth);
        assert_eq!(spec.area.width, 40);
        assert!(spec.area.height < 20);
    }

    #[test]
    fn wezterm_fit_uses_full_height_for_tall_images() {
        let spec = fit_wezterm_render_spec(Rect::new(0, 0, 40, 20), (900, 1600), (9, 22))
            .expect("render spec");
        assert_eq!(spec.fit, NativeRenderFit::WezTermHeight);
        assert_eq!(spec.area.height, 20);
        assert!(spec.area.width < 40);
    }
}
