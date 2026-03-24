import os
import sys
import json
import string
import numpy as np
import pandas as pd
import openpyxl
from datetime import datetime, date
from threading import Thread

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QTabWidget, QCheckBox, QProgressBar,
    QTextEdit, QFileDialog, QDateEdit, QFrame,
    QGridLayout, QSizePolicy, QDialog,
)
from PySide6.QtCore import QDate, Signal, QObject
from PySide6.QtGui import QFont, QColor, QIcon, QTextCharFormat, QTextCursor


def resource_path(filename):
    """Return correct path whether running as script or PyInstaller exe."""
    base = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, filename)


# ═══════════════════════════════════════════════════════════════════════════
# Site / pipe configuration
# ═══════════════════════════════════════════════════════════════════════════
SITES_AND_PIPES = {
    "MLM": ["MA1", "MA2", "MB1", "MB2", "MC1", "MC2", "MD1", "MD2",
            "MACS", "SG1", "SG2", "SG3", "MC_biomass"],
    "CMC": ["CA1", "CA2", "CB1", "CB2", "CACS"],
    "Q_Line": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6",
               "Q7", "Q8", "Q9", "Q10", "Q11", "Q12"],
    "SBW": ["NA12", "NA24", "NB24", "NB31", "NC21", "NC22",
            "NL2-22", "NL2-23", "NL2-43", "NPK37", "SACS", "ND", "NE", "Forest NA"],
    "WT_Marudi": [
        "SSD1_1",  "SSD1_2",  "SSD2_1",  "SSD2_2",  "SSD3_1",  "SSD3_2",
        "SSD8_1",  "SSD8_2",
        "SSD10_1", "SSD10_2", "SSD11_1", "SSD11_2", "SSD12_1", "SSD12_2",
        "SSD13_1", "SSD13_2", "SSD14_1", "SSD14_2", "SSD15_1", "SSD15_2",
    ],
}

# ═══════════════════════════════════════════════════════════════════════════
# Google Drive auto-detection & portable path helpers
# ═══════════════════════════════════════════════════════════════════════════
_GDRIVE_REL_DIR = os.path.join("Hydrology Research", "Manual WT Google Sheet", "Split MWT")
_GDRIVE_DEFAULT_FILE = "2025-08-19-P14_WT_Manual.xlsx"
_MY_DRIVE = "My Drive"


def detect_google_drive_roots():
    roots = []
    home = os.path.expanduser("~")
    for sub in [_MY_DRIVE,
                os.path.join("Google Drive", _MY_DRIVE),
                os.path.join("Google Drive Stream", _MY_DRIVE),
                os.path.join("GoogleDrive", _MY_DRIVE)]:
        candidate = os.path.join(home, sub)
        if os.path.isdir(candidate):
            roots.append(candidate)
    for letter in string.ascii_uppercase:
        candidate = f"{letter}:\\{_MY_DRIVE}"
        if os.path.isdir(candidate) and candidate not in roots:
            roots.append(candidate)
    return roots


def resolve_gdrive_path(rel_path, hint_root=None):
    roots = detect_google_drive_roots()
    if hint_root and hint_root in roots:
        roots.remove(hint_root)
        roots.insert(0, hint_root)
    for root in roots:
        full = os.path.join(root, rel_path)
        if os.path.exists(full):
            return full
    return None


def to_gdrive_relative(abs_path):
    abs_norm = os.path.normpath(abs_path)
    for root in detect_google_drive_roots():
        root_norm = os.path.normpath(root)
        if abs_norm.lower().startswith(root_norm.lower() + os.sep) or abs_norm.lower() == root_norm.lower():
            rel = os.path.relpath(abs_norm, root_norm)
            return rel, root_norm
    return None, None


# ═══════════════════════════════════════════════════════════════════════════
# Preferences (JSON config stored in %APPDATA%\Hydro_path)
# ═══════════════════════════════════════════════════════════════════════════
_appdata = os.environ.get("APPDATA", os.path.expanduser("~"))
_prefs_dir = os.path.join(_appdata, "Hydro_path")
os.makedirs(_prefs_dir, exist_ok=True)
_config_file = os.path.join(_prefs_dir, "config_116.json")


def load_config():
    if os.path.exists(_config_file):
        try:
            with open(_config_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_config(cfg):
    with open(_config_file, "w") as f:
        json.dump(cfg, f, indent=2)


# ═══════════════════════════════════════════════════════════════════════════
# Resolve initial paths
# ═══════════════════════════════════════════════════════════════════════════
_cfg = load_config()
_hint = _cfg.get("gdrive_root")

_resolved_file = None
if _cfg.get("rel_file"):
    _resolved_file = resolve_gdrive_path(_cfg["rel_file"], hint_root=_hint)
if not _resolved_file and _cfg.get("abs_file") and os.path.exists(_cfg["abs_file"]):
    _resolved_file = _cfg["abs_file"]
if not _resolved_file:
    _default_rel = os.path.join(_GDRIVE_REL_DIR, _GDRIVE_DEFAULT_FILE)
    _resolved_file = resolve_gdrive_path(_default_rel, hint_root=_hint)

_init_file = _resolved_file or ""

_resolved_outdir = None
if _cfg.get("rel_outdir"):
    _resolved_outdir = resolve_gdrive_path(_cfg["rel_outdir"], hint_root=_hint)
if not _resolved_outdir and _cfg.get("abs_outdir") and os.path.isdir(_cfg["abs_outdir"]):
    _resolved_outdir = _cfg["abs_outdir"]
if not _resolved_outdir and _init_file:
    _resolved_outdir = os.path.dirname(_init_file)

_init_outdir = _resolved_outdir or ""


# ═══════════════════════════════════════════════════════════════════════════
# Thread-safe signal bridge
# ═══════════════════════════════════════════════════════════════════════════
class WorkerSignals(QObject):
    log       = Signal(str, str)
    status    = Signal(str)
    current   = Signal(str)
    progress  = Signal(int, int)      # value, maximum
    duration  = Signal(str)
    finished  = Signal(int, str)      # error_count, error_msg


# ═══════════════════════════════════════════════════════════════════════════
# Generate checkbox tick icon (temp file used by QSS)
# ═══════════════════════════════════════════════════════════════════════════
import tempfile as _tmpmod
_tick_path = os.path.join(_tmpmod.gettempdir(), "wt_extractor_tick.svg")
with open(_tick_path, "w") as _f:
    _f.write(
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16">'
        '<path d="M3.5 8.5 L6.5 11.5 L12.5 4.5" fill="none" '
        'stroke="white" stroke-width="2.2" stroke-linecap="round" '
        'stroke-linejoin="round"/></svg>'
    )
_tick_url = _tick_path.replace("\\", "/")

_arrow_path = os.path.join(_tmpmod.gettempdir(), "wt_extractor_arrow.svg")
with open(_arrow_path, "w") as _f:
    _f.write(
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 12 12">'
        '<path d="M3 4.5 L6 8 L9 4.5" fill="none" '
        'stroke="#00b4d8" stroke-width="2" stroke-linecap="round" '
        'stroke-linejoin="round"/></svg>'
    )
_arrow_url = _arrow_path.replace("\\", "/")

# ═══════════════════════════════════════════════════════════════════════════
# QSS Stylesheet
# ═══════════════════════════════════════════════════════════════════════════
STYLESHEET = """
/* ── Global ─────────────────────────────────────────── */
QMainWindow, QWidget#central {
    background-color: #0f1923;
}

QWidget#body {
    background-color: #0f1923;
}

/* ── Header ─────────────────────────────────────────── */
QFrame#header {
    background-color: #091520;
    border-bottom: 2px solid #162a3a;
}

/* ── Cards ──────────────────────────────────────────── */
QFrame[class="card"] {
    background-color: #162230;
    border: 1px solid #253545;
    border-radius: 8px;
}

QWidget[class="tab-page"] {
    background-color: #162230;
}

/* ── Section titles ─────────────────────────────────── */
QLabel[class="section-title"] {
    color: #00b4d8;
    font-size: 12px;
    font-weight: bold;
    font-family: "Segoe UI";
}

QLabel[class="section-accent"] {
    background-color: #00b4d8;
    border-radius: 2px;
}

/* ── Labels ─────────────────────────────────────────── */
QLabel {
    color: #d4dce6;
    font-family: "Segoe UI";
    font-size: 11px;
}

QLabel[class="dim"] {
    color: #8fa8be;
    font-size: 9px;
}

QLabel[class="file-name"] {
    color: #00b4d8;
    font-size: 11px;
    font-weight: bold;
}

QLabel[class="current-item"] {
    color: #ff6ec7;
    font-size: 14px;
    font-style: italic;
    font-family: "Segoe UI";
}

QLabel[class="badge-ok"] {
    color: #00e676;
    font-size: 10px;
}

QLabel[class="badge-warn"] {
    color: #ffb347;
    font-size: 10px;
}

/* ── Buttons (base) ─────────────────────────────────── */
QPushButton {
    background-color: #1589c9;
    color: #ffffff;
    border: 1px solid #3ab0e8;
    border-radius: 5px;
    padding: 5px 14px;
    font-family: "Segoe UI";
    font-size: 10px;
    font-weight: bold;
}
QPushButton:hover {
    background-color: #22a2e4;
    border: 1px solid #66ccff;
}
QPushButton:pressed {
    background-color: #0e6fa3;
    border: 1px solid #1589c9;
    padding: 6px 14px 4px 14px;
}

/* ── Start Processing ──────────────────────────────── */
QPushButton#btn-start {
    background-color: #00a86b;
    color: #ffffff;
    font-size: 12px;
    font-weight: bold;
    padding: 7px 28px;
    border-radius: 6px;
    border: 1px solid #33cc88;
}
QPushButton#btn-start:hover {
    background-color: #00cc82;
    border: 1px solid #55eea0;
}
QPushButton#btn-start:pressed {
    background-color: #007a4d;
    border: 1px solid #00a86b;
    padding: 8px 28px 6px 28px;
}

/* ── Abort ─────────────────────────────────────────── */
QPushButton#btn-abort {
    background-color: #d43535;
    color: #ffffff;
    font-size: 11px;
    font-weight: bold;
    padding: 7px 20px;
    border-radius: 6px;
    border: 1px solid #ee6666;
}
QPushButton#btn-abort:hover {
    background-color: #ee4c4c;
    border: 1px solid #ff8888;
}
QPushButton#btn-abort:pressed {
    background-color: #a82222;
    border: 1px solid #d43535;
    padding: 8px 20px 6px 20px;
}

/* ── Small buttons (Browse, Change, tab Select/Deselect) */
QPushButton[class="small-btn"] {
    background-color: #2080aa;
    color: #ffffff;
    font-size: 9px;
    padding: 3px 10px;
    border-radius: 4px;
    border: 1px solid #38a0cc;
}
QPushButton[class="small-btn"]:hover {
    background-color: #2da0cc;
    border: 1px solid #55ccee;
}
QPushButton[class="small-btn"]:pressed {
    background-color: #186088;
    border: 1px solid #2080aa;
    padding: 4px 10px 2px 10px;
}

/* ── Select / Deselect All Sites ───────────────────── */
QPushButton#btn-select-all, QPushButton#btn-deselect-all {
    background-color: #2080aa;
    color: #ffffff;
    font-size: 11px;
    font-weight: bold;
    padding: 7px 18px;
    border-radius: 6px;
    border: 1px solid #38a0cc;
}
QPushButton#btn-select-all:hover, QPushButton#btn-deselect-all:hover {
    background-color: #2da0cc;
    border: 1px solid #55ccee;
}
QPushButton#btn-select-all:pressed, QPushButton#btn-deselect-all:pressed {
    background-color: #186088;
    border: 1px solid #2080aa;
    padding: 8px 18px 6px 18px;
}

/* ── Tabs ───────────────────────────────────────────── */
QTabWidget::pane {
    background-color: #162230;
    border: 1px solid #253545;
    border-top: 2px solid #253545;
}
QTabBar {
    qproperty-drawBase: 0;
}
QTabBar::tab {
    background-color: #1a2d3d;
    color: #7a95ad;
    font-family: "Segoe UI";
    font-size: 11px;
    font-weight: bold;
    padding: 7px 20px;
    border: 1px solid #253545;
    border-bottom: none;
    border-top-left-radius: 6px;
    border-top-right-radius: 6px;
    margin-right: 1px;
}
QTabBar::tab:selected {
    background-color: #162230;
    color: #00e676;
    border: 1px solid #00e676;
    border-bottom: 2px solid #162230;
}
QTabBar::tab:hover:!selected {
    background-color: #213848;
    color: #c8d6e2;
    border-color: #3a5a75;
}

/* ── Checkboxes ─────────────────────────────────────── */
QCheckBox {
    color: #d4dce6;
    font-family: "Segoe UI";
    font-size: 11px;
    spacing: 6px;
}
QCheckBox::indicator {
    width: 16px;
    height: 16px;
    border: 2px solid #3a5068;
    border-radius: 3px;
    background-color: #0f1f2e;
}
QCheckBox::indicator:checked {
    background-color: #00b4d8;
    border-color: #00cef0;
    image: url(__TICK_URL__);
}
QCheckBox::indicator:hover {
    border-color: #00b4d8;
    background-color: #152a3a;
}
QCheckBox::indicator:checked:hover {
    background-color: #00d0f0;
    border-color: #33eeff;
}
QCheckBox:hover {
    color: #ffffff;
}

/* ── DateEdit ───────────────────────────────────────── */
QDateEdit {
    background-color: #1c2e3f;
    color: #d4dce6;
    border: 1px solid #3a5068;
    border-radius: 6px;
    padding: 5px 10px;
    font-family: "Segoe UI";
    font-size: 11px;
    min-width: 120px;
}
QDateEdit:focus {
    border-color: #00b4d8;
}
QDateEdit::drop-down {
    subcontrol-origin: padding;
    subcontrol-position: top right;
    width: 28px;
    border-left: 1px solid #3a5068;
    border-top-right-radius: 6px;
    border-bottom-right-radius: 6px;
    background-color: #253545;
}
QDateEdit::down-arrow {
    image: url(__ARROW_URL__);
    width: 12px;
    height: 12px;
}

/* Calendar popup */
QCalendarWidget {
    background-color: #162230;
    border: 1px solid #253545;
    border-radius: 8px;
}
QCalendarWidget QToolButton {
    color: #d4dce6;
    background-color: #0077b6;
    border: none;
    border-radius: 4px;
    padding: 4px 8px;
    font-weight: bold;
}
QCalendarWidget QToolButton:hover { background-color: #0096d6; }
QCalendarWidget QMenu {
    background-color: #162230;
    color: #d4dce6;
}
QCalendarWidget QSpinBox {
    background-color: #1c2e3f;
    color: #d4dce6;
    border: 1px solid #3a5068;
    border-radius: 4px;
}
QCalendarWidget QAbstractItemView {
    background-color: #162230;
    color: #d4dce6;
    selection-background-color: #00b4d8;
    selection-color: #ffffff;
    alternate-background-color: #1a2d3d;
}
QCalendarWidget QWidget#qt_calendar_navigationbar {
    background-color: #0077b6;
    border-radius: 6px 6px 0 0;
}

/* ── Progress bar ───────────────────────────────────── */
QProgressBar {
    background-color: #1c2e3f;
    border: 1px solid #253545;
    border-radius: 6px;
    text-align: center;
    color: #d4dce6;
    font-family: "Segoe UI";
    font-size: 10px;
    font-weight: bold;
    min-height: 18px;
}
QProgressBar::chunk {
    background-color: qlineargradient(x1:0, y1:0, x2:1, y2:0,
        stop:0 #00895e, stop:1 #00e676);
    border-radius: 5px;
}

/* ── Log text ───────────────────────────────────────── */
QTextEdit#log {
    background-color: #0a1520;
    color: #d4dce6;
    border: 1px solid #253545;
    border-radius: 8px;
    padding: 8px;
    font-family: "Cascadia Code", "Consolas", monospace;
    font-size: 10px;
    selection-background-color: #0077b6;
}

/* ── Scrollbars ─────────────────────────────────────── */
QScrollBar:vertical {
    background-color: #0f1923;
    width: 10px;
    border-radius: 5px;
}
QScrollBar::handle:vertical {
    background-color: #3a5068;
    min-height: 30px;
    border-radius: 5px;
}
QScrollBar::handle:vertical:hover { background-color: #4a6a88; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }

/* ── Tooltip ────────────────────────────────────────── */
QToolTip {
    background-color: #253545;
    color: #d4dce6;
    border: 1px solid #3a5068;
    border-radius: 4px;
    padding: 4px 8px;
    font-family: "Segoe UI";
    font-size: 10px;
}
""".replace("__TICK_URL__", _tick_url).replace("__ARROW_URL__", _arrow_url)


# ═══════════════════════════════════════════════════════════════════════════
# Main window
# ═══════════════════════════════════════════════════════════════════════════
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Water Table Data Extractor  v1.1.6")
        try:
            self.setWindowIcon(QIcon(resource_path("wticon.ico")))
        except Exception:
            pass
        self.setMinimumSize(780, 700)

        self.full_filename = _init_file
        self.output_dir = _init_outdir
        self.stop_requested = False
        self.processing_running = False
        self.start_time = None
        self.site_checkboxes = {}
        self.signals = WorkerSignals()

        # Connect signals
        self.signals.log.connect(self._append_log)
        self.signals.status.connect(self._set_status)
        self.signals.current.connect(self._set_current)
        self.signals.progress.connect(self._set_progress)
        self.signals.duration.connect(self._set_duration)
        self.signals.finished.connect(self._on_finished)

        self._build_ui()
        self._initial_log()

    # ── Build the entire UI ──────────────────────────────────────────────
    def _build_ui(self):
        central = QWidget()
        central.setObjectName("central")
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        # ── Header ───────────────────────────────────────────────────────
        header = QFrame()
        header.setObjectName("header")
        header.setFixedHeight(40)
        hl = QHBoxLayout(header)
        hl.setContentsMargins(16, 0, 16, 0)

        title = QLabel("Water Table Data Extractor")
        title.setStyleSheet("font-size: 15px; font-weight: bold; color: #ffffff;")
        hl.addWidget(title)

        ver = QLabel("v1.1.6")
        ver.setStyleSheet("font-size: 11px; color: #00b4d8; margin-left: 4px;")
        hl.addWidget(ver)

        hl.addStretch()

        gdrive_roots = detect_google_drive_roots()
        gd_text = f"Google Drive: {gdrive_roots[0]}" if gdrive_roots else "Google Drive: not detected"
        gd_cls = "badge-ok" if gdrive_roots else "badge-warn"
        gd_label = QLabel(gd_text)
        gd_label.setProperty("class", gd_cls)
        hl.addWidget(gd_label)

        root_layout.addWidget(header)

        # ── Body (no scroll — fits in one screen) ────────────────────────
        body = QWidget()
        body.setObjectName("body")
        body_layout = QVBoxLayout(body)
        body_layout.setContentsMargins(12, 6, 12, 6)
        body_layout.setSpacing(6)

        # ── Combined I/O + Date card ─────────────────────────────────────
        io_card = self._make_section("INPUT / OUTPUT", body_layout)
        io_inner = QVBoxLayout()
        io_inner.setSpacing(4)
        io_card.layout().addLayout(io_inner)

        # File row — full path as tooltip
        file_row = QHBoxLayout()
        file_row.addWidget(self._dim_label("Manual WT File", 95))
        self.label_file = QLabel(os.path.basename(self.full_filename) or "(no file selected)")
        self.label_file.setProperty("class", "file-name")
        self.label_file.setToolTip(self.full_filename or "(no file selected)")
        self.label_file.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        file_row.addWidget(self.label_file)
        btn_browse = QPushButton("Browse")
        btn_browse.setProperty("class", "small-btn")
        btn_browse.setToolTip("Browse the latest downloaded Google Sheet manual water table file.")
        btn_browse.clicked.connect(self.select_file)
        file_row.addWidget(btn_browse)
        io_inner.addLayout(file_row)

        # Output dir row
        out_row = QHBoxLayout()
        out_row.addWidget(self._dim_label("Output Directory", 95))
        self.label_outdir = QLabel(self.output_dir or "(no directory selected)")
        self.label_outdir.setProperty("class", "file-name")
        self.label_outdir.setStyleSheet("font-weight: normal; color: #00b4d8; font-size: 10px;")
        self.label_outdir.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        out_row.addWidget(self.label_outdir)
        btn_outdir = QPushButton("Change")
        btn_outdir.setProperty("class", "small-btn")
        btn_outdir.clicked.connect(self.select_outdir)
        out_row.addWidget(btn_outdir)
        io_inner.addLayout(out_row)

        # Date range row (inside same card)
        _today = date.today()
        dt_row = QHBoxLayout()
        dt_row.addWidget(self._dim_label("Date Range", 95))
        dt_row.addWidget(QLabel("Start"))
        self.date_start = QDateEdit()
        self.date_start.setCalendarPopup(True)
        self.date_start.setDate(QDate(_today.year, 1, 1))
        self.date_start.setDisplayFormat("yyyy-MM-dd")
        self.date_start.setToolTip("First timestamp of the continuous 30-min grid")
        dt_row.addWidget(self.date_start)
        dt_row.addSpacing(12)
        dt_row.addWidget(QLabel("End"))
        self.date_end = QDateEdit()
        self.date_end.setCalendarPopup(True)
        self.date_end.setDate(QDate(_today.year, _today.month, _today.day))
        self.date_end.setDisplayFormat("yyyy-MM-dd")
        self.date_end.setToolTip("Last timestamp of the continuous 30-min grid")
        dt_row.addWidget(self.date_end)
        dt_row.addStretch()
        io_inner.addLayout(dt_row)

        # ── Site / Pipe tabs ─────────────────────────────────────────────
        self.tabs = QTabWidget()
        for site, pipes in SITES_AND_PIPES.items():
            page = QWidget()
            page.setProperty("class", "tab-page")
            page_layout = QVBoxLayout(page)
            page_layout.setContentsMargins(8, 4, 8, 4)
            page_layout.setSpacing(2)

            # Select / Deselect buttons
            btn_row = QHBoxLayout()
            sa = QPushButton("Select All")
            sa.setProperty("class", "small-btn")
            sa.clicked.connect(lambda checked=False, s=site: self.check_all(s))
            btn_row.addWidget(sa)
            da = QPushButton("Deselect All")
            da.setProperty("class", "small-btn")
            da.clicked.connect(lambda checked=False, s=site: self.uncheck_all(s))
            btn_row.addWidget(da)
            btn_row.addStretch()
            page_layout.addLayout(btn_row)

            # Checkboxes grid
            grid = QGridLayout()
            grid.setSpacing(2)
            grid.setContentsMargins(0, 0, 0, 0)
            self.site_checkboxes[site] = {}
            cols = 4 if len(pipes) > 8 else 2
            for i, pipe in enumerate(pipes):
                cb = QCheckBox(pipe)
                grid.addWidget(cb, i // cols, i % cols)
                self.site_checkboxes[site][pipe] = cb
            page_layout.addLayout(grid)
            page_layout.addStretch()

            self.tabs.addTab(page, f"  {site}  ")

        body_layout.addWidget(self.tabs)

        # ── Action buttons ───────────────────────────────────────────────
        action_row = QHBoxLayout()
        action_row.setSpacing(6)

        btn_sa = QPushButton("Select All Sites")
        btn_sa.setObjectName("btn-select-all")
        btn_sa.clicked.connect(lambda: [self.check_all(s) for s in SITES_AND_PIPES])
        action_row.addWidget(btn_sa)

        btn_da = QPushButton("Deselect All Sites")
        btn_da.setObjectName("btn-deselect-all")
        btn_da.clicked.connect(lambda: [self.uncheck_all(s) for s in SITES_AND_PIPES])
        action_row.addWidget(btn_da)

        action_row.addSpacing(12)

        self.btn_start = QPushButton("  Start Processing  ")
        self.btn_start.setObjectName("btn-start")
        self.btn_start.clicked.connect(self.start_processing)
        action_row.addWidget(self.btn_start)

        self.btn_abort = QPushButton("  Abort  ")
        self.btn_abort.setObjectName("btn-abort")
        self.btn_abort.clicked.connect(self.abort_processing)
        action_row.addWidget(self.btn_abort)

        action_row.addStretch()
        body_layout.addLayout(action_row)

        # ── Progress + status (compact) ──────────────────────────────────
        prog_row = QHBoxLayout()
        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("%v / %m")
        self.progress_bar.setFixedHeight(18)
        prog_row.addWidget(self.progress_bar, 1)
        self.duration_label = QLabel("Duration: 0 sec")
        self.duration_label.setProperty("class", "dim")
        prog_row.addWidget(self.duration_label)
        body_layout.addLayout(prog_row)

        status_row = QHBoxLayout()
        self.status_label = QLabel("Ready.")
        status_row.addWidget(self.status_label)
        status_row.addStretch()
        self.current_item_label = QLabel("")
        self.current_item_label.setProperty("class", "current-item")
        self.current_item_label.setStyleSheet("font-size: 11px; font-style: italic; color: #ff6ec7;")
        status_row.addWidget(self.current_item_label)
        body_layout.addLayout(status_row)

        # ── Log ──────────────────────────────────────────────────────────
        log_header = QHBoxLayout()
        accent_bar = QLabel()
        accent_bar.setFixedSize(4, 14)
        accent_bar.setStyleSheet("background-color: #00b4d8; border-radius: 2px;")
        log_header.addWidget(accent_bar)
        lt = QLabel("PROCESSING LOG")
        lt.setProperty("class", "section-title")
        log_header.addWidget(lt)
        log_header.addStretch()
        btn_clear = QPushButton("Clear Log")
        btn_clear.setProperty("class", "small-btn")
        btn_clear.clicked.connect(self.clear_log)
        log_header.addWidget(btn_clear)
        body_layout.addLayout(log_header)

        self.log_text = QTextEdit()
        self.log_text.setObjectName("log")
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(80)
        body_layout.addWidget(self.log_text, 1)

        root_layout.addWidget(body, 1)
        self._apply_button_styles()

    # ── UI helpers ───────────────────────────────────────────────────────
    def _button_qss(self, normal_bg, hover_bg, pressed_bg, border_color,
                    hover_border, pressed_border, font_size, padding):
        return f"""
            QPushButton {{
                background-color: {normal_bg};
                color: #ffffff;
                border: 1px solid {border_color};
                border-radius: 6px;
                font-size: {font_size}px;
                font-weight: bold;
                padding: {padding};
            }}
            QPushButton:hover {{
                background-color: {hover_bg};
                border: 1px solid {hover_border};
            }}
            QPushButton:pressed {{
                background-color: {pressed_bg};
                border: 1px solid {pressed_border};
            }}
        """

    def _apply_button_styles(self):
        self.btn_start.setStyleSheet(self._button_qss(
            "#00a86b", "#00cc82", "#007a4d",
            "#33cc88", "#55eea0", "#00a86b",
            12, "7px 28px"
        ))
        self.btn_abort.setStyleSheet(self._button_qss(
            "#d43535", "#ee4c4c", "#a82222",
            "#ee6666", "#ff8888", "#d43535",
            11, "7px 20px"
        ))
        for btn in self.findChildren(QPushButton, "btn-select-all"):
            btn.setStyleSheet(self._button_qss(
                "#2080aa", "#2da0cc", "#186088",
                "#38a0cc", "#55ccee", "#2080aa",
                11, "7px 18px"
            ))
        for btn in self.findChildren(QPushButton, "btn-deselect-all"):
            btn.setStyleSheet(self._button_qss(
                "#2080aa", "#2da0cc", "#186088",
                "#38a0cc", "#55ccee", "#2080aa",
                11, "7px 18px"
            ))

    def _dim_label(self, text, width=120):
        lbl = QLabel(text)
        lbl.setProperty("class", "dim")
        lbl.setFixedWidth(width)
        return lbl

    def _make_section(self, title, parent_layout):
        # Section header
        hdr = QHBoxLayout()
        accent_bar = QLabel()
        accent_bar.setFixedSize(4, 18)
        accent_bar.setProperty("class", "section-accent")
        accent_bar.setStyleSheet("background-color: #00b4d8; border-radius: 2px;")
        hdr.addWidget(accent_bar)
        lbl = QLabel(title)
        lbl.setProperty("class", "section-title")
        hdr.addWidget(lbl)
        hdr.addStretch()
        parent_layout.addLayout(hdr)

        # Card frame
        card = QFrame()
        card.setProperty("class", "card")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(10, 8, 10, 8)
        card_layout.setSpacing(4)
        parent_layout.addWidget(card)
        return card

    # ── Custom styled message dialog ────────────────────────────────────
    _MSG_ICONS = {"info": "#00b4d8", "warning": "#ffb347", "error": "#ff5252"}

    def _show_msg(self, title, message, level="info"):
        """Show a fully dark-themed message dialog (replaces QMessageBox)."""
        accent = self._MSG_ICONS.get(level, "#00b4d8")
        dlg = QDialog(self)
        dlg.setWindowTitle(title)
        dlg.setMinimumWidth(380)
        dlg.setStyleSheet(f"""
            QDialog {{
                background-color: #162230;
                border: 1px solid #253545;
            }}
            QLabel#msg-icon {{
                color: {accent};
                font-size: 28px;
                font-weight: bold;
            }}
            QLabel#msg-title {{
                color: #ffffff;
                font-size: 14px;
                font-weight: bold;
                font-family: "Segoe UI";
            }}
            QLabel#msg-body {{
                color: #d4dce6;
                font-size: 12px;
                font-family: "Segoe UI";
            }}
            QPushButton {{
                background-color: {accent};
                color: #ffffff;
                border: none;
                border-radius: 6px;
                padding: 8px 32px;
                font-family: "Segoe UI";
                font-size: 11px;
                font-weight: bold;
                min-width: 80px;
            }}
            QPushButton:hover {{
                background-color: #ffffff;
                color: #162230;
            }}
            QPushButton:pressed {{
                background-color: #0a1520;
                color: #ffffff;
            }}
        """)

        layout = QVBoxLayout(dlg)
        layout.setContentsMargins(24, 20, 24, 20)
        layout.setSpacing(12)

        # Icon + title row
        top = QHBoxLayout()
        icon_map = {"info": "\u2139", "warning": "\u26A0", "error": "\u2716"}
        icon_lbl = QLabel(icon_map.get(level, "\u2139"))
        icon_lbl.setObjectName("msg-icon")
        top.addWidget(icon_lbl)
        title_lbl = QLabel(title)
        title_lbl.setObjectName("msg-title")
        top.addWidget(title_lbl)
        top.addStretch()
        layout.addLayout(top)

        # Body
        body_lbl = QLabel(message)
        body_lbl.setObjectName("msg-body")
        body_lbl.setWordWrap(True)
        layout.addWidget(body_lbl)

        layout.addSpacing(8)

        # OK button
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(dlg.accept)
        btn_row.addWidget(ok_btn)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        dlg.exec()

    # ── Log ──────────────────────────────────────────────────────────────
    LOG_COLORS = {
        "info":  "#00b4d8",
        "ok":    "#00e676",
        "warn":  "#ffb347",
        "err":   "#ff5252",
        "white": "#d4dce6",
    }

    def _append_log(self, msg, color="white"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        fmt = QTextCharFormat()
        fmt.setForeground(QColor(self.LOG_COLORS.get(color, "#d4dce6")))
        cursor = self.log_text.textCursor()
        cursor.movePosition(QTextCursor.End)
        cursor.insertText(f"[{timestamp}] {msg}\n", fmt)
        self.log_text.setTextCursor(cursor)
        self.log_text.ensureCursorVisible()

    def log(self, msg, color="white"):
        self.signals.log.emit(msg, color)

    def clear_log(self):
        self.log_text.clear()

    # ── Slot helpers ─────────────────────────────────────────────────────
    def _set_status(self, text):
        self.status_label.setText(text)

    def _set_current(self, text):
        self.current_item_label.setText(text)

    def _set_progress(self, value, maximum):
        self.progress_bar.setMaximum(maximum)
        self.progress_bar.setValue(value)

    def _set_duration(self, text):
        self.duration_label.setText(text)

    def _on_finished(self, error_count, error_msg):
        self.processing_running = False
        self.current_item_label.setText("")
        if self.stop_requested:
            self.status_label.setText("Processing aborted.")
        elif error_count:
            self.status_label.setText(f"Done with {error_count} error(s). See log for details.")
            self._show_msg("Completed with Errors",
                          f"{error_count} pipe(s) had errors:\n\n{error_msg}", "warning")
        else:
            self.status_label.setText("All selected pipes processed successfully.")
            self.log("All done!", "ok")
            self._show_msg("Processing Complete",
                          "All selected pipes have been processed successfully.", "info")

    # ── Initial log ──────────────────────────────────────────────────────
    def _initial_log(self):
        self._append_log("Water Table Data Extractor v1.1.6 ready.", "info")
        gdrive_roots = detect_google_drive_roots()
        if gdrive_roots:
            self._append_log(f"Google Drive found: {gdrive_roots[0]}", "ok")
        else:
            self._append_log("Google Drive not detected — use Browse to select files manually.", "warn")
        if self.full_filename and os.path.exists(self.full_filename):
            self._append_log(f"Input file : {self.full_filename}", "white")
        else:
            self._append_log("No input file selected — click Browse to choose one.", "warn")
        self._append_log(f"Output dir : {self.output_dir or '(none)'}", "white")

    # ── File / dir selection ─────────────────────────────────────────────
    def _persist_paths(self):
        cfg = load_config()
        rel, gdroot = to_gdrive_relative(self.full_filename)
        if rel:
            cfg["rel_file"] = rel
            cfg["gdrive_root"] = gdroot
        else:
            cfg.pop("rel_file", None)
        cfg["abs_file"] = self.full_filename
        rel_out, gdroot_out = to_gdrive_relative(self.output_dir)
        if rel_out:
            cfg["rel_outdir"] = rel_out
            cfg["gdrive_root"] = gdroot_out
        else:
            cfg.pop("rel_outdir", None)
        cfg["abs_outdir"] = self.output_dir
        save_config(cfg)

    def select_file(self):
        init_dir = os.path.dirname(self.full_filename) if self.full_filename and os.path.exists(self.full_filename) else ""
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Manual WT Excel File", init_dir,
            "Excel files (*.xlsx);;All files (*.*)")
        if not path:
            return
        self.full_filename = path
        self.label_file.setText(os.path.basename(path))
        self.label_file.setToolTip(path)
        self.output_dir = os.path.dirname(path)
        self.label_outdir.setText(self.output_dir)
        self._persist_paths()

    def select_outdir(self):
        init_dir = self.output_dir if self.output_dir and os.path.isdir(self.output_dir) else ""
        path = QFileDialog.getExistingDirectory(self, "Select Output Directory", init_dir)
        if not path:
            return
        self.output_dir = path
        self.label_outdir.setText(self.output_dir)
        self._persist_paths()

    # ── Checkbox helpers ─────────────────────────────────────────────────
    def check_all(self, site):
        for cb in self.site_checkboxes[site].values():
            cb.setChecked(True)

    def uncheck_all(self, site):
        for cb in self.site_checkboxes[site].values():
            cb.setChecked(False)

    # ── Processing ───────────────────────────────────────────────────────
    def start_processing(self):
        """Validate on the main thread, then spawn worker if OK."""
        if self.processing_running:
            return

        # ── Validation (main thread → safe to show dialogs) ──
        if not self.full_filename or not os.path.exists(self.full_filename):
            self._show_msg("No Input File",
                          "Please select a valid Manual WT Excel file before processing.", "error")
            return
        if not self.output_dir or not os.path.isdir(self.output_dir):
            self._show_msg("No Output Directory",
                          "Please select a valid output directory before processing.", "error")
            return

        selections = [
            (site, pipe)
            for site, chks in self.site_checkboxes.items()
            for pipe, cb in chks.items()
            if cb.isChecked()
        ]
        if not selections:
            self._show_msg("Selection Required",
                          "Please select at least one pipe before starting.", "warning")
            return

        try:
            ds = self.date_start.date()
            de = self.date_end.date()
            date_start = pd.Timestamp(ds.year(), ds.month(), ds.day())
            date_end   = pd.Timestamp(de.year(), de.month(), de.day())
        except Exception:
            self._show_msg("Invalid Date",
                          "Please enter valid dates in YYYY-MM-DD format.", "error")
            return

        # ── All OK — start worker ──
        self.start_time = datetime.now()
        self.processing_running = True
        self.stop_requested = False
        self.signals.progress.emit(0, len(selections))
        self.signals.status.emit("Starting processing...")

        Thread(target=self._process_worker,
               args=(selections, date_start, date_end),
               daemon=True).start()

    def abort_processing(self):
        self.stop_requested = True
        self.log("Abort requested by user.", "warn")

    def _process_worker(self, selections, date_start, date_end):
        self.signals.progress.emit(0, len(selections))
        self.signals.status.emit("Starting processing...")
        self.log(f"Processing {len(selections)} pipe(s)  |  "
                 f"{date_start.date()} -> {date_end.date()}", "info")

        errors = []

        for idx, (site, pipe) in enumerate(selections, 1):
            if self.stop_requested:
                self.signals.status.emit("Processing aborted.")
                self.log("Processing aborted.", "warn")
                break

            # Update duration
            total = int((datetime.now() - self.start_time).total_seconds())
            if total < 60:
                dur = f"Duration: {total:02d} sec"
            elif total < 3600:
                dur = f"Duration: {total // 60:02d} min {total % 60:02d} sec"
            else:
                h = total // 3600
                dur = f"Duration: {h:02d} hr {(total % 3600) // 60:02d} min {total % 60:02d} sec"
            self.signals.duration.emit(dur)

            self.signals.current.emit(f"  {site}  ->  {pipe}")
            self.signals.status.emit(f"Processing: {site} -> {pipe}  ({idx}/{len(selections)})")
            self.log(f"Processing: {pipe}")

            try:
                out_path = os.path.join(self.output_dir, f"{site}_{pipe}.xlsx")

                with pd.ExcelFile(self.full_filename) as xls:
                    df = pd.read_excel(xls, sheet_name=site)

                date_range = pd.date_range(start=date_start, end=date_end, freq='30min')
                df1 = pd.DataFrame({'Timestamp': date_range})

                df = df.dropna(subset=['Time', 'Date'])

                drop_cols = [
                    'Year', 'Month', 'Cable Length', 'WS', 'PH', 'Logger Type',
                    'Remark', 'Diver S/N', 'Unnamed: 12', 'Muhaini_Remarks',
                    'Check_WT_M', 'Remark 2 (Rain gauge)', 'Remark 3 (Diver)',
                    'Day', 'Station', 'Pipe', 'TimeRaw',
                ]
                df = df.drop(columns=[c for c in drop_cols if c in df.columns])

                df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                df.to_excel(out_path, index=False)
                df = pd.read_excel(out_path)
                df['Date'] = pd.to_datetime(df['Date'])
                df['Timestamp'] = df['Date'] + pd.to_timedelta(df['Time'])
                df = df.drop(['Date', 'Time'], axis=1)
                df.to_excel(out_path, index=False)

                df = df[df['Site'] == pipe]
                df['Timestamp'], df['Site'] = df['Site'], df['Timestamp']
                df = df.rename(columns={'Timestamp': 'Temp', 'Site': 'Timestamp'})
                df = df.rename(columns={'Temp': 'Site'})
                df.to_excel(out_path, index=True)

                merged = pd.concat([df1, df], ignore_index=True)
                merged = merged.drop(['Site'], axis=1)
                merged = merged.sort_values('Timestamp')
                merged['Time Difference'] = merged['Timestamp'].diff().dt.total_seconds() / 60
                merged.to_excel(out_path, index=False)
                merged = pd.read_excel(out_path)

                merged['Merge_WTM'] = np.nan
                values, flag = [], False
                for i, row in merged.iterrows():
                    if (pd.notna(row.get('WT_M'))
                            and pd.notna(row['Time Difference'])
                            and row['Time Difference'] <= 30):
                        values.append(float(row['WT_M']))
                        flag = True
                    nxt = merged.at[i + 1, 'Time Difference'] if i + 1 < len(merged) else None
                    if flag and nxt is not None and row['Time Difference'] < nxt:
                        merged.at[i - 1, 'Merge_WTM'] = sum(values) if i > 0 else np.nan
                        values, flag = [], False
                    if flag and nxt is not None and row['Time Difference'] >= nxt:
                        merged.at[i + 1, 'Merge_WTM'] = sum(values)
                        values, flag = [], False

                merged = merged[merged['WT_M'].isna()]
                merged = merged.drop(['Time Difference', 'WT_M'], axis=1)
                merged = merged.rename(columns={'Merge_WTM': pipe})
                merged.to_excel(out_path, index=False)

                wb = openpyxl.load_workbook(out_path)
                ws_xl = wb.active
                for col in ws_xl.columns:
                    max_len = max((len(str(c.value)) for c in col if c.value is not None), default=0)
                    ws_xl.column_dimensions[
                        openpyxl.utils.get_column_letter(col[0].column)
                    ].width = (max_len + 2) * 1.5
                wb.save(out_path)
                wb.close()

                self.log(f"  OK  {pipe}  ->  saved", "ok")

            except Exception as exc:
                errors.append((site, pipe, str(exc)))
                self.log(f"  FAIL  {pipe}  ERROR: {exc}", "err")

            self.signals.progress.emit(idx, len(selections))

        # Final duration
        total = int((datetime.now() - self.start_time).total_seconds())
        if total < 60:
            dur = f"Duration: {total:02d} sec"
        elif total < 3600:
            dur = f"Duration: {total // 60:02d} min {total % 60:02d} sec"
        else:
            h = total // 3600
            dur = f"Duration: {h:02d} hr {(total % 3600) // 60:02d} min {total % 60:02d} sec"
        self.signals.duration.emit(dur)

        err_msg = "\n".join(f"{s}/{p}: {e}" for s, p, e in errors)
        self.signals.finished.emit(len(errors), err_msg)


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setStyleSheet(STYLESHEET)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())
