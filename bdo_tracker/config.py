"""Config schema + JSON load/save (SPEC §6).

Geometry fields (region bbox, row height, icon size, scroll direction) are
populated by the calibration tool or by the mock frame generator — never
hardcoded and never hand-edited. Unknown-until-measured values are None /
"unknown".
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

SCROLL_DIRECTIONS = ("unknown", "entry_at_top", "entry_at_bottom")


@dataclass
class CaptureConfig:
    source: str = "mock"  # "mock" | "dxcam"
    # Which dxcam output the game runs on (Account tab's Game monitor picker;
    # DXGI order, not Windows display numbers — see capture/monitors.py).
    output_idx: int = 0
    # GDI devicename of that monitor (\\.\DISPLAYn), pinned by the picker.
    # DXGI indices reshuffle across reboots/driver updates; the pin lets
    # capture re-map (or fail loud) instead of silently reading another
    # same-size monitor (audit 2026-07-09). None = pre-pin config.
    output_device: str | None = None
    region_bbox: list[int] | None = None  # [x, y, w, h] — written by calibration
    # How region_bbox sits relative to the Item Drop Log's Interface Edit
    # Mode panel: [dx, dy, w, h], region = panel origin + (dx, dy). Measured
    # whenever a calibration flow confirms a region with the panel in view;
    # lets a later detect (widget moved, another user's layout) propose the
    # SAME framing — the toast geometry in `geometry` is region-relative and
    # only survives relocation if the framing is preserved.
    region_panel_offset: list[int] | None = None
    # The Rare Item Drop Log — the SEPARATE widget the game renders rare-grade
    # pickups into (SPEC §B5). Written by `ui.calibrate --rare`, never by hand.
    # None = not calibrated -> the rare-log watch is off.
    rare_region_bbox: list[int] | None = None
    # The enhancement window (SPEC §E7, Part E). Unlike every other region
    # here this one is NOT calibrated per machine: the enhancement UI is a
    # full-screen mode the player cannot reposition (measured 2026-08-15), so
    # at UI scale 100 the geometry is a constant of the game, not of the
    # install. Hence a measured default rather than None -> calibrate. It is
    # deliberately tighter than the full screen (all fields + the action
    # buttons + the failstack-bank row) so the watcher samples ~0.5 MPx
    # instead of 2.07. There is NO enhance_region_panel_offset and there must
    # not be one — see SPEC §E7.1; the window has no panel to anchor to.
    enhance_region_bbox: list[int] = field(
        default_factory=lambda: [480, 470, 950, 570]
    )
    sample_interval_s: float | None = None  # measured against burst rate, not guessed
    # When set, live runs record every frame into a per-run subdirectory here
    # (offline replay/validation evidence); None disables. Old runs beyond
    # record_keep_runs are auto-pruned at the start of each new run.
    record_dir: str | None = None
    record_keep_runs: int = 5


@dataclass
class GeometryConfig:
    # All measured from real 1080p captures (M2) or set by the synthetic
    # generator for mock runs. None = not yet calibrated.
    # The drop log is a bottom-anchored toast widget (SPEC §4.2): rows sit at
    # fixed slots counted upward from row_anchor_y_px in row_height_px steps.
    row_height_px: int | None = None  # slot pitch
    icon_size_px: int | None = None
    max_visible_rows: int | None = None
    scroll_direction: str = "unknown"
    icon_x_px: int | None = None  # region-relative left edge of the icon art
    text_x_px: int | None = None  # region-relative x where the text line starts
    row_anchor_y_px: int | None = None  # region-relative center-y of the bottom slot
    toast_lifetime_s: float | None = None  # measured on-screen duration of one toast

    def require_calibrated(self) -> None:
        missing = [
            f
            for f in (
                "row_height_px",
                "icon_size_px",
                "max_visible_rows",
                "icon_x_px",
                "text_x_px",
                "row_anchor_y_px",
            )
            if getattr(self, f) is None
        ]
        if missing:
            raise ValueError(f"geometry not calibrated: {', '.join(missing)} unset")


@dataclass
class RecognitionConfig:
    hash_match_threshold: int | None = None  # max Hamming distance; tuned in M2
    icon_border: str = "undecided"  # "crop_interior" | "keep" — decided in M2
    tesseract_path: str | None = None  # None = probe PATH + default install dir
    # Masked-NCC accept threshold, shared by the inventory matcher (measured
    # on the A-M1 gate screenshot: true 0.56-0.94, best false ~0.53) and the
    # live toast matcher (measured on the real lamb/fairy fixtures: true
    # 0.78-0.85, wrong item <=0.54, noise <=0.33).
    ncc_threshold: float = 0.58
    # When set, live rows whose icon can't be identified dump their crops
    # here (identity-validation evidence); None disables.
    debug_unknown_dir: str | None = None
    # Inventory-font digit glyph templates (U-M5 corner-count engine —
    # tools/build_inventory_glyphs.py); pinned to the install dir by the
    # launcher like glyph_dir. None falls back to the Tesseract mask path.
    inv_glyph_dir: str | None = "tools/glyph_templates_inventory"
    # Rare-log frames the watch flags as active dump their region crops
    # here — calibration evidence for the first real rare drop (the rare
    # widget's toast geometry is unmeasured until one lands on pixels).
    # Defaults on, like glyph_dir: evidence collection must survive config
    # rewrites by older builds. None disables.
    debug_rare_dir: str | None = "debug/rare_log"
    # Live qty engine: harvested fixed-font glyph matching (~3x faster than
    # a tesseract.exe spawn per row and hallucination-resistant; shadow-
    # validated 2026-07-06 on both recorded runs). Falls back to Tesseract
    # automatically when the template dir is missing. Re-harvest with
    # tools/build_glyph_templates.py if the game's UI scale/font changes.
    glyph_dir: str | None = "tools/glyph_templates"
    # Enhancement-window glyph templates (Part E, SPEC §E7.3). A THIRD set,
    # deliberately separate from glyph_dir and inv_glyph_dir: this surface
    # renders a different font at a different size and needs symbols the
    # other two sets have never carried (% . / + parens). Dropping those
    # into either existing directory would silently re-arm code paths in the
    # LIVE drop-log reader that have never run on real pixels. Re-harvest
    # with tools/build_enh_glyphs.py. None disables the enhancement reads.
    enh_glyph_dir: str | None = "tools/glyph_templates_enh"


@dataclass
class StorageConfig:
    db_path: str = "tracker.sqlite"


@dataclass
class InventoryConfig:
    """Inventory-panel grid geometry (SPEC §A3). All measured from a real
    1920x1080 inventory screenshot — None until measured."""

    origin_x_px: int | None = None  # top-left cell's icon top-left corner
    origin_y_px: int | None = None
    cell_size_px: int | None = None  # icon tile edge
    pitch_x_px: int | None = None  # cell-to-cell spacing
    pitch_y_px: int | None = None
    cols: int | None = None
    rows: int | None = None
    qty_band_h_px: int | None = None  # bottom band of the cell holding the stack count
    empty_cell_std: float | None = None  # occupancy threshold (empty = flat UI tile)
    screenshot_dir: str | None = None  # where --latest looks (Steam screenshots)

    def require_calibrated(self) -> None:
        missing = [
            f
            for f in (
                "origin_x_px",
                "origin_y_px",
                "cell_size_px",
                "pitch_x_px",
                "pitch_y_px",
                "cols",
                "rows",
                "qty_band_h_px",
                "empty_cell_std",
            )
            if getattr(self, f) is None
        ]
        if missing:
            raise ValueError(f"inventory geometry not measured: {', '.join(missing)} unset")


@dataclass
class ItemListConfig:
    """The hunting 'ITEM LIST' loot box (Part B hunting): the static grid the
    game shows on Get-All. It's a fixed-position inventory-style grid, so the
    Part A recognizer reads it directly — and because it shows a whole animal's
    loot at once (no scrolling toasts), per-Get-All counts are exact. Region +
    grid geometry are measured per machine (calibrated, never hand-set);
    ignore_item_ids drops non-loot tokens (the hunting-XP shields). region_bbox
    None = the feature is off (grinding never shows this box)."""

    # RETIRED as the hunting loot source (user decision 2026-08-08, Tyler:
    # "make it exactly like grinding it works good it should be the same
    # way"). The box was meant as a BACKUP read; box-wins semantics made it
    # the primary and hunting got worse than the plain drop-log path it
    # replaced. Hunting now reads toasts exactly like grinding. The reader
    # + monitor stay tested and can be re-enabled by setting this True.
    enabled: bool = False
    region_bbox: list[int] | None = None  # [x, y, w, h] live capture, calibrated
    # Grid geometry, region-relative (origin = top-left of cell 0,0 within the
    # captured region). Same shape the Part A inventory analyzer consumes.
    origin_x_px: int | None = None
    origin_y_px: int | None = None
    cell_size_px: int | None = None
    pitch_x_px: int | None = None
    pitch_y_px: int | None = None
    cols: int | None = None
    max_rows: int | None = None  # rows the box grows to at full loot variety
    qty_band_h_px: int | None = None
    empty_cell_std: float | None = None
    ignore_item_ids: list[int] = field(default_factory=list)  # hunting-XP tokens etc.

    def grid(self) -> "InventoryConfig":
        """The box's geometry as the inventory analyzer's config shape (its
        `rows` is our max_rows — empty cells past the loot are detected out)."""
        return InventoryConfig(
            origin_x_px=self.origin_x_px,
            origin_y_px=self.origin_y_px,
            cell_size_px=self.cell_size_px,
            pitch_x_px=self.pitch_x_px,
            pitch_y_px=self.pitch_y_px,
            cols=self.cols,
            rows=self.max_rows,
            qty_band_h_px=self.qty_band_h_px,
            empty_cell_std=self.empty_cell_std,
        )


@dataclass
class EnhanceConfig:
    """Enhancement-window sub-field geometry (SPEC §E7.2, Part E).

    All boxes are [x, y, w, h] RELATIVE to capture.enhance_region_bbox, the
    same contract ItemListConfig uses. Every value here was measured off the
    2026-08-15 evidence (debug/fixtures/enh-20260815/ — a 60 fps recording
    plus three stills) at 1920x1080, UI scale 100; the rulered crops the
    numbers were read from are reproducible with tools/measure_enh_geometry.py.

    Why these are DEFAULTS and not None-until-calibrated, unlike every other
    geometry block in this file: the enhancement window is a full-screen mode
    that cannot be moved or resized (measured, SPEC §E7.1). There is no
    per-install variation to calibrate away at a given UI scale, so shipping
    None here would force every user through a calibration ritual for a
    window that physically cannot be anywhere else. Rule 3 still holds — the
    numbers live in config, came from measurement, and any user whose client
    disagrees can override them. UI scale != 100 is the one thing that WOULD
    move these (SPEC §E7.1 defers it until a real user has one).

    enabled ships False: SPEC §E1 requires the capture surface to stay
    visibly off until its own milestone lands. E-M3 only reads frames from
    disk; E-M4 is what turns this on.
    """

    enabled: bool = False
    # --- value fields -----------------------------------------------------
    # The verdict banner is the PRIMARY outcome channel (SPEC §E7.3): the
    # game states "Enhancement success." / "Enhancement failed." in words.
    verdict_bbox: list[int] = field(default_factory=lambda: [360, 18, 240, 32])
    # Pity / Essence of Agris counter, rendered "n / N" in the ring's corner
    # circle. N varies per item and level (5, 7, 8 and 17 all observed in one
    # session), which is why it is read rather than assumed (SPEC §E4b).
    pity_bbox: list[int] = field(default_factory=lambda: [564, 100, 62, 36])
    # The two contributor readouts above the failstack total. Read for
    # cross-check only; the ledger's failstack is fs_total_bbox.
    fs_bonus_bbox: list[int] = field(default_factory=lambda: [705, 162, 185, 32])
    # "367 (+75.8824%)" — the active failstack and its bonus. Centre-grown,
    # so the box is generous on both sides of x-centre 1269 absolute.
    fs_total_bbox: list[int] = field(default_factory=lambda: [660, 188, 260, 42])
    # The item art itself, for identity. Inset to the INNER frame: the slot
    # is three nested borders deep and including them adds a constant bright
    # rim the DB art does not have, which measurably depresses NCC. Icon art
    # renders ~40x40 here (measured against the material slot, whose sizing
    # is identical) versus the 44x44 the item DB stores.
    # Measured 2026-08-15 off the real-gear stills: the art sits inside the
    # violet GRADE BORDER at x 946-986, y 676-714 absolute. The border is
    # excluded deliberately — it encodes grade, not identity, and its colour
    # changes with the piece.
    item_art_bbox: list[int] = field(default_factory=lambda: [466, 205, 40, 40])
    # The enhancement level, drawn over the art ("+15", or a roman numeral on
    # families that use one). Distinguishes FAIL from DOWNGRADE (SPEC §E7.4).
    level_bbox: list[int] = field(default_factory=lambda: [444, 206, 76, 42])
    # The game's own success chance. Four decimals when exact, whole when
    # capped — both forms must parse (SPEC §E7.9).
    chance_bbox: list[int] = field(default_factory=lambda: [396, 258, 168, 40])
    # Shared status BLOCK, not a line. Measured 2026-08-15 on the real-gear
    # stills: it carries one line ("Max Durability -5 upon failure (Current
    # Durability: 90)", or "Enhancement Guaranteed", or nothing) OR TWO, the
    # second being "100% chance for Enhancement Level to drop upon failure".
    # The block is vertically CENTRED, so a one-line message sits where
    # neither of the two-line messages does — a fixed single-line box reads
    # the wrong thing. The box spans both slots and the parser splits.
    # That second line matters out of proportion to its size: it states the
    # DOWNGRADE PROBABILITY outright, so §E7.4's downgrade row can be known
    # from the panel instead of waiting on a cronless failure to observe.
    status_bbox: list[int] = field(default_factory=lambda: [220, 436, 520, 52])
    mat_slot_bbox: list[int] = field(default_factory=lambda: [20, 190, 66, 66])
    # The Protection slot — only rendered when cron protection is in play,
    # which is why it is absent from the 02:38 recording entirely and present
    # in every real-gear still (the user never enhances without crons). Its
    # quantity overlay is the cron STOCK — 3650 on the 2026-08-15 stills
    # (user-corrected; a zoomed read of the same crop said "x850", which is
    # a standing reminder that this field needs the matcher, not an eyeball)
    # — so the ledger can both confirm an attempt was cron-protected and
    # track crons remaining.
    prot_slot_bbox: list[int] = field(default_factory=lambda: [168, 120, 48, 44])
    prot_qty_bbox: list[int] = field(default_factory=lambda: [165, 134, 54, 24])

    # Measured attempt cadence, for E-M4's stability gate. The 2026-08-15
    # recording was deliberately spammed with Skip Animation on and its
    # closest two attempts resolved 0.65 s apart; anything slower than
    # ~1.6 Hz would have merged them into one. 8 Hz leaves 5x headroom on a
    # cadence the user says is already faster than real play.
    min_attempt_gap_s: float = 0.65
    sample_hz: float = 8.0

    def field_boxes(self) -> dict[str, list[int]]:
        """Every region-relative value box, by name."""
        return {
            n: getattr(self, n + "_bbox")
            for n in (
                "verdict", "pity", "fs_bonus", "fs_total",
                "item_art", "level", "chance", "status", "mat_slot",
                "prot_slot", "prot_qty",
            )
        }

    def absolute_boxes(self, region_bbox: list[int]) -> dict[str, list[int]]:
        """The same boxes in screen coordinates, given the capture region."""
        rx, ry = region_bbox[0], region_bbox[1]
        return {n: [rx + b[0], ry + b[1], b[2], b[3]] for n, b in self.field_boxes().items()}

    def require_calibrated(self) -> None:
        missing = [n for n, b in self.field_boxes().items() if not b or len(b) != 4]
        if missing:
            raise ValueError(f"enhance geometry not calibrated: {', '.join(missing)} unset")


@dataclass
class PricingConfig:
    region: str = "na"
    cache_ttl_hours: float = 24.0
    # Central Market tax components (verified vs bdolytics tax calculator):
    # collected = price * 0.65 * (1 + 0.30*VP + 0.05*ring + fame_bonus).
    # Vendor sales are untaxed.
    value_pack: bool = False
    rich_merchant_ring: bool = False
    family_fame: int = 0
    # ── Published price snapshot (bdo_tracker/pricing/snapshot.py) ──
    # One publisher fetches arsha once for everybody; every install downloads a
    # ~16 KB file instead of making up to 1,264 per-item calls at 6 workers
    # (DB measured 2026-08-16: 1,264 of 1,630 items are market-priced). Off
    # switches the whole path back to per-item fetching — the snapshot is an
    # optimisation layered over market.py, never a replacement for it.
    snapshot_enabled: bool = True
    # Public releases repo, "stable tag whose asset is refreshed in place"
    # (the pattern tools/build_release.py --pin-installer already documents):
    # the cron republishes the same asset name under the `prices` tag, so the
    # URL never changes and a conditional GET can 304. {region} is substituted
    # with pricing.region; a URL without the placeholder is used verbatim, which
    # is how a tester points at a local file server.
    snapshot_url: str = (
        "https://github.com/goldstargamingtv-droid/bdo-tracker-releases"
        "/releases/download/prices/price_snapshot_{region}.json.gz"
    )
    # Past this age the snapshot is flagged stale and the app stops treating it
    # as the answer, falling back to direct per-item arsha fetches. Larger than
    # cache_ttl_hours on purpose: the per-item TTL decides whether ONE price
    # needs refetching, this decides whether the whole publish pipeline has
    # stopped. It must exceed the publish interval or a healthy daily cron
    # would read as broken; 72 h tolerates two missed publishes plus slop.
    snapshot_max_age_hours: float = 72.0

    def collection_rate(self) -> float:
        fame = (
            0.0
            if self.family_fame < 1000
            else 0.005
            if self.family_fame < 4000
            else 0.01
            if self.family_fame < 7000
            else 0.015
        )
        return 0.65 * (
            1.0
            + (0.30 if self.value_pack else 0.0)
            + (0.05 if self.rich_merchant_ring else 0.0)
            + fame
        )


@dataclass
class SessionConfig:
    character: str = ""
    # The class being grinded (bdo_tracker/classes.py roster) + its spec
    # (Succession/Awakening/Ascension/Talent). Stamped on saved grind
    # sessions for Home's per-class stats; the Grind tab refuses to start
    # a live session while grind_class is empty (user 2026-08-07).
    grind_class: str = ""
    grind_class_spec: str = ""
    default_grind_spot: str = ""  # Grind tab's preselected spot
    default_hunting_spot: str = ""  # Hunting tab's preselected spot
    default_cooking_spot: str = ""  # Cooking tab's preselected recipe
    default_alchemy_spot: str = ""  # Alchemy tab's preselected recipe
    # Production planners: the user's real in-game seconds per craft — gear
    # dependent (base 10s; utensil/tool, clothes and buffs reduce it, floor
    # 1s), typed once next to the plan and remembered here, per lifeskill.
    cook_time_seconds: float = 10.0
    alchemy_time_seconds: float = 10.0
    # Personal-best gate: sessions shorter than this can neither SET nor
    # BEAT a silver/hr record — a lucky 4-minute burst extrapolates to an
    # unbeatable fake rate (records divide by tiny denominators).
    record_min_minutes: int = 10


@dataclass
class UIConfig:
    always_on_top_readout: bool = False
    auto_open_readout: bool = True  # pop the live window when tracking starts
    # Widget look: no title bar on the live window (drag the body to move,
    # corner grip to resize — the frame's affordances have to live somewhere).
    frameless_readout: bool = False
    # After Stop, re-run the full pipeline over the session's recording (no
    # live-time pressure, every buffered frame included) and replace the
    # report with the refined pass before saving. RETIRED as a default
    # 2026-08-06 (user decision): the in-app pass cost minutes of CPU per
    # session and its learning only reached the one machine — recordings
    # now feed the dev-side replay tools (um2_gate/phantom_forensics),
    # whose results ship as reference data for every install. The refine
    # machinery (RefineWorker, name assist, selfheal) stays for that
    # dev-side use and for anyone flipping this back on by hand.
    auto_refine: bool = False
    # Unknown-drop / read-warning diagnostics read as breakage to testers
    # (user 2026-07-09), so the readout rows, report notes, and Loot Card
    # footnotes hide them by default. Presentation only — rule 5 holds:
    # every run with diagnostics writes the full details to a per-run file
    # (storage/diagnostics.py) and the saved counts stay in the DB/History.
    hide_diagnostics: bool = True
    # Dashboard window chrome: in-client title bar (logo + min/max/close) on
    # a caption-less frame — snap/shadow/resize stay native (ui/titlebar.py).
    # Escape hatch: false restores the stock Windows title bar.
    custom_titlebar: bool = True
    # Boss-timer strip above the dashboard tabs (user 2026-08-07): pure
    # clock math over bdo_tracker/bosses.py reference data, no network.
    show_boss_timer: bool = True
    boss_region: str = "na"  # bosses.REGIONS entry ('na' | 'eu')
    # Manual tab (screenshot analysis + guided F12 flow): hidden from the
    # tab row by default (user 2026-07-10 — the product story is AUTO
    # tracking; testers shouldn't meet a tab that needs a manual). The
    # machinery stays constructed and one flag away: it is the ground-truth
    # instrument (inventory-diff validation, e.g. Quint Hill) and the only
    # F12 flow that can MEASURE cooking's consumed mats.
    show_manual_tab: bool = False


# §E3 cron presets: crons are not marketplace items. The vendor figure is
# the NPC price; the outfit figure is an APPROXIMATION that moves with
# pearl prices — the UI labels it as such, never as measured.
CRON_COST_PRESETS = {"vendor": 3_000_000, "outfit": 2_200_000}


@dataclass
class LedgerConfig:
    """Enhancement Ledger (SPEC §E). Accounting/display only — capture
    geometry for the enhancement window (E-M3+) lives under capture.*
    when it exists, not here."""

    cron_cost: int = CRON_COST_PRESETS["vendor"]
    # Which preset cron_cost came from ('vendor' | 'outfit' | 'custom') —
    # display provenance for the §E3 label.
    cron_cost_preset: str = "vendor"
    # §E4 minimum-evidence gate: below this many attempts the luck panel
    # says "not enough attempts to read", never a percentile.
    luck_min_attempts: int = 20


@dataclass
class MockConfig:
    """Mock capture source parameters. The mock defines its own synthetic
    geometry; it does not pretend to know real in-game values."""

    frames: int = 30
    region_size: list[int] = field(default_factory=lambda: [320, 240])  # [w, h]
    seed: int = 0
    # Frames a synthetic toast stays visible; None = never expires (the
    # original M0 behavior — entries only leave by overflowing the top).
    lifetime_frames: int | None = None


@dataclass
class Config:
    capture: CaptureConfig = field(default_factory=CaptureConfig)
    geometry: GeometryConfig = field(default_factory=GeometryConfig)
    recognition: RecognitionConfig = field(default_factory=RecognitionConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    inventory: InventoryConfig = field(default_factory=InventoryConfig)
    itemlist: ItemListConfig = field(default_factory=ItemListConfig)
    enhance: EnhanceConfig = field(default_factory=EnhanceConfig)
    pricing: PricingConfig = field(default_factory=PricingConfig)
    session: SessionConfig = field(default_factory=SessionConfig)
    ui: UIConfig = field(default_factory=UIConfig)
    ledger: LedgerConfig = field(default_factory=LedgerConfig)
    mock: MockConfig = field(default_factory=MockConfig)

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(
            capture=CaptureConfig(**raw.get("capture", {})),
            geometry=GeometryConfig(**raw.get("geometry", {})),
            recognition=RecognitionConfig(**raw.get("recognition", {})),
            storage=StorageConfig(**raw.get("storage", {})),
            inventory=InventoryConfig(**raw.get("inventory", {})),
            itemlist=ItemListConfig(**raw.get("itemlist", {})),
            enhance=EnhanceConfig(**raw.get("enhance", {})),
            pricing=PricingConfig(**raw.get("pricing", {})),
            session=SessionConfig(**raw.get("session", {})),
            ui=UIConfig(**raw.get("ui", {})),
            ledger=LedgerConfig(**raw.get("ledger", {})),
            mock=MockConfig(**raw.get("mock", {})),
        )

    def save(self, path: str | Path) -> None:
        """Atomic write (temp + replace): the Account tab saves on every
        control change, so a crash mid-write must never leave a truncated
        config.json behind — Config.load would raise at next launch and
        every setting would be lost (audit 2026-07-09)."""
        path = Path(path)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(asdict(self), indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)

    def validate(self) -> None:
        if self.geometry.scroll_direction not in SCROLL_DIRECTIONS:
            raise ValueError(
                f"scroll_direction must be one of {SCROLL_DIRECTIONS}, "
                f"got {self.geometry.scroll_direction!r}"
            )
        if self.capture.source not in ("mock", "dxcam"):
            raise ValueError(f"unknown capture source {self.capture.source!r}")
