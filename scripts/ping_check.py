"""
╔══════════════════════════════════════════════════════╗
║           🦐 NOC MONITOR  · by shrimp               ║
║       Terminal network monitoring dashboard          ║
╚══════════════════════════════════════════════════════╝
"""
from __future__ import annotations
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Deque, Dict, Optional, Tuple
import requests
from rich.box import MINIMAL, SIMPLE_HEAD
from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import rule

# ── suppress verbose urllib3 warnings ────────────────────────────────────────
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

REFRESH_INTERVAL: float = 2.0
MAX_HISTORY: int = 12
MAX_WORKERS: int = 20
HTTP_TIMEOUT: float = 3.0
ICMP_TIMEOUT: float = 1.5
SPARKLINE = "▁▂▃▄▅▆▇█"
SHRIMP_ART = r"""
⠀⠀⠀⠀⠀⠀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣀⣤⣤⣀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢀⣀⡙⠻⢶⣶⣦⣴⣶⣶⣶⠾⠛⠛⠋⠉⠉⠉⠉⠙⠃⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠉⠉⠙⠛⠛⠋⠉⠉⠡⣤⣴⣶⣶⣾⣿⣿⣿⣛⣩⣤⡤⠖⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⢠⣴⣾⠂⣴⣦⠈⣿⣿⣿⣿⣿⣿⠿⠛⣋⠁⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢀⣼⣿⣶⣄⡉⠻⣧⣌⣁⣴⣿⣿⣿⣿⣿⣿⡿⠛⠁⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣾⣿⣿⣿⣿⣿⣦⡈⢻⣿⣿⣿⣿⡿⠿⠛⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⡀⢻⣿⣿⣿⣿⣿⣿⣿⡄⠙⠛⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢠⣷⣄⡉⠻⢿⣿⣿⣿⠏⠠⢶⣄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢸⣿⣿⣿⣶⣤⣈⠙⠁⠰⣦⣀⠉⠻⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠘⢿⣿⣿⣿⣿⣿⡇⠠⣦⣄⠉⠳⣤⠈⠛⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⢠⣌⣉⡉⠉⣉⡁⠀⠀⠙⠗⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠹⢿⣿⣿⣿⣿⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠙⠻⣿⣿⠟⢀⣤⡀⠀⠀⠀⠀⠀⠀⣀⣀⣠⣤⣤⣤⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠛⠿⠿⡿⠂⣀⣠⣤⣤⣤⣀⣉⣉⠉⠉⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠉⠙⠛⠛⠛⠛⠋⠉⠉⠁⠀⠀⠀⠀

              SHRIMP NODE
         NETWORK OBSERVATION
"""
# ── colour palette ────────────────────────────────────────────────────────
# ── ULTRA BRIGHT CYBER PALETTE ────────────────────────────────────────────

C_OK      = "bright_green"
C_WARN    = "bright_yellow"
C_SLOW    = "bright_cyan"
C_ERROR   = "bright_red"
C_TIMEOUT = "bright_magenta"
C_DIM     = "bright_white"
C_TITLE   = "bright_cyan"
C_BORDER  = "bright_blue"
C_TIME    = "bright_white"

# ── latency thresholds (ms) ───────────────────────────────────────────────
LAT_FAST   = 80
LAT_MEDIUM = 200

# ═══════════════════════════════════════════════════════════════════════════
# DATA MODEL
# ═══════════════════════════════════════════════════════════════════════════

class Status(str, Enum):
    ONLINE    = "ONLINE"
    LIMITED   = "LIMITED"
    FAIL      = "FAIL"
    TIMEOUT   = "TIMEOUT"
    ERROR     = "ERROR"

STATUS_LABEL: Dict[Status, Tuple[str, str]] = {
    #            rich markup colour   display text
    Status.ONLINE:   (C_OK,      "● ONLINE"),
    Status.LIMITED:  (C_WARN,    "◐ LIMITED"),
    Status.FAIL:     (C_ERROR,   "✕ FAIL"),
    Status.TIMEOUT:  (C_TIMEOUT, "⧗ TIMEOUT"),
    Status.ERROR:    (C_DIM,     "? ERROR"),
}

@dataclass
class CheckResult:
    name:    str
    address: str
    status:  Status
    latency: Optional[float]   # ms, None if unreachable
    checked_at: str = field(default_factory=lambda: datetime.now().strftime("%H:%M:%S"))

@dataclass
class HostStats:
    ok:    int = 0
    total: int = 0
    history: Deque[float] = field(default_factory=lambda: deque(maxlen=MAX_HISTORY))

    @property
    def uptime(self) -> float:
        return (self.ok / self.total * 100) if self.total else 0.0

# ═══════════════════════════════════════════════════════════════════════════
# MONITORING TARGETS
# ═══════════════════════════════════════════════════════════════════════════

HOSTS: Dict[str, Tuple[str, str]] = {
    "Google DNS":    ("8.8.8.8",                  "icmp"),
    "Cloudflare DNS":("1.1.1.1",                  "icmp"),
    "YouTube":       ("https://youtube.com",       "http"),
    "TikTok":        ("https://tiktok.com",        "http"),
    "Telegram":      ("https://telegram.org",      "http"),
    "Instagram":     ("https://instagram.com",     "http"),
    "Spotify":       ("https://spotify.com",       "http"),
    "Yandex Music":  ("https://music.yandex.ru",   "http"),
    "OpenAI":        ("https://openai.com",        "http"),
    "GitHub":        ("https://github.com",        "http"),
    "Wikipedia":     ("https://wikipedia.org",     "http"),
    "Amazon AWS":    ("https://aws.amazon.com",    "http"),
}

# ═══════════════════════════════════════════════════════════════════════════
# PROBE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def _probe_icmp(host: str) -> Tuple[Optional[float], Status]:
    try:
        from ping3 import ping  # lazy import — optional dependency
        raw = ping(host, timeout=ICMP_TIMEOUT, unit="ms")
        if raw is None:
            return None, Status.TIMEOUT
        return round(raw, 1), Status.ONLINE
    except ImportError:
        # fall back to HTTP if ping3 is unavailable
        return _probe_http(f"http://{host}")
    except Exception:
        return None, Status.ERROR

def _probe_http(url: str) -> Tuple[Optional[float], Status]:
    try:
        start = time.monotonic()
        response = requests.get(
            url,
            timeout=HTTP_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": "NOC-Monitor/2.0 (shrimp)"},
            verify=False,
        )
        latency = round((time.monotonic() - start) * 1000, 1)

        if response.status_code < 300:
            return latency, Status.ONLINE
        if response.status_code in (401, 403):
            return latency, Status.LIMITED
        return latency, Status.FAIL

    except requests.exceptions.Timeout:
        return None, Status.TIMEOUT
    except requests.exceptions.ConnectionError:
        return None, Status.ERROR
    except Exception:
        return None, Status.ERROR

def probe(name: str, address: str, mode: str) -> CheckResult:
    """Dispatch to the right probe and return a normalised result."""
    if mode == "icmp":
        latency, status = _probe_icmp(address)
    else:
        latency, status = _probe_http(address)

    return CheckResult(name=name, address=address, status=status, latency=latency)

# ═══════════════════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════════════════

host_stats: Dict[str, HostStats] = defaultdict(HostStats)

def record(result: CheckResult) -> None:
    s = host_stats[result.name]
    s.total += 1
    if result.status == Status.ONLINE:
        s.ok += 1
    if result.latency is not None:
        s.history.append(result.latency)

# ═══════════════════════════════════════════════════════════════════════════
# RENDERING HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _latency_text(latency: Optional[float]) -> Text:
    if latency is None:
        return Text("—", style=C_DIM)
    if latency < LAT_FAST:
        colour = C_OK
    elif latency < LAT_MEDIUM:
        colour = C_SLOW
    else:
        colour = C_ERROR
    return Text(f"{latency:>7.1f} ms", style=colour)

def _status_text(status: Status) -> Text:
    colour, label = STATUS_LABEL[status]
    return Text(label, style=f"bold {colour}" if status == Status.ONLINE else colour)

def _sparkline(history: Deque[float]) -> str:
    if not history:
        return Text("", style=C_DIM)
    mn, mx = min(history), max(history)
    span = mx - mn or 1
    bars = [SPARKLINE[int((v - mn) / span * (len(SPARKLINE) - 1))] for v in history]
    return " ".join(bars)

def _uptime_text(value: float) -> Text:
    if value >= 99:
        return Text(f"{value:>5.1f}%", style=C_OK)
    if value >= 90:
        return Text(f"{value:>5.1f}%", style=C_SLOW)
    return Text(f"{value:>5.1f}%", style=C_ERROR)

# ═══════════════════════════════════════════════════════════════════════════
# TABLE BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def _run_checks() -> list[CheckResult]:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(probe, name, addr, mode): name
            for name, (addr, mode) in HOSTS.items()
        }
        results = []
        for fut in as_completed(futures):
            result = fut.result()
            record(result)
            results.append(result)

    # preserve original order
    order = list(HOSTS.keys())
    results.sort(key=lambda r: order.index(r.name))
    return results

def build_status_table(results: list[CheckResult]) -> Table:
    table = Table(
        box=SIMPLE_HEAD,
        border_style=C_BORDER,
        header_style=f"bold {C_TITLE}",
        show_edge=True,
        pad_edge=True,
        expand=True,
    )

    table.add_column("TIME",    style=C_TIME,  no_wrap=True, width=10)
    table.add_column("SERVICE",               no_wrap=True, min_width=14)
    table.add_column("ADDRESS", style=C_DIM,  no_wrap=True, min_width=22)
    table.add_column("STATUS",                no_wrap=True, justify="left",  width=14)
    table.add_column("LATENCY",               no_wrap=True, justify="right", width=12)
    table.add_column("UPTIME",                no_wrap=True, justify="right", width=8)

    for r in results:
        s = host_stats[r.name]
        table.add_row(
            r.checked_at,
            r.name,
            r.address,
            _status_text(r.status),
            _latency_text(r.latency),
            _uptime_text(s.uptime),
        )

    return table

# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY BAR
# ═══════════════════════════════════════════════════════════════════════════

def build_summary(results: list[CheckResult]) -> Text:
    total   = len(results)
    online  = sum(1 for r in results if r.status == Status.ONLINE)
    limited = sum(1 for r in results if r.status == Status.LIMITED)
    failing = total - online - limited
    avg_lat = None
    lats = [r.latency for r in results if r.latency is not None]
    if lats:
        avg_lat = sum(lats) / len(lats)
    t = Text()
    t.append(f" {online}/{total} ", style=f"bold {C_OK}")
    t.append("online  ", style=C_DIM)
    if limited:
        t.append(f"{limited} limited  ", style=C_WARN)
    if failing:
        t.append(f"{failing} failing  ", style=C_ERROR)
    if avg_lat is not None:
        t.append(f"avg {avg_lat:.0f} ms", style=C_SLOW)
    t.append("  🦐", style="")
    return t

def build_graphs(results: list[CheckResult]) -> Text:
    """Render full-width sparkline graph panel — one row per service."""
    SPARK = "▁▂▃▄▅▆▇█"
    NAME_W  = 15   # fixed name column width
    BAR_GAP = "" # breathing room between bars
    t = Text()
    for i, r in enumerate(results):
        hist = host_stats[r.name].history
        # ── name column (fixed width, right-padded) ───────────────────────
        name = r.name[:NAME_W]
        t.append(f"  {name:<{NAME_W}}", style=C_DIM)
        t.append(" │ ", style=C_BORDER)
        # ── sparkline bars ────────────────────────────────────────────────
        if hist:
            mn, mx = min(hist), max(hist)
            span = mx - mn or 1
            for j, v in enumerate(hist):
                idx  = int((v - mn) / span * (len(SPARK) - 1))
                char = SPARK[max(0, min(idx, len(SPARK) - 1))]
                # colour each bar individually by its own latency value
                if v < LAT_FAST:
                    bar_colour = C_OK
                elif v < LAT_MEDIUM:
                    bar_colour = C_SLOW
                else:
                    bar_colour = C_ERROR
                t.append(char, style=bar_colour)
                t.append(BAR_GAP, style="")  # air between bars
            # ── latest value on the right ─────────────────────────────────
            latest = hist[-1]
            t.append("  ")
            t.append(_latency_text(latest))
        else:
            t.append("─ no data yet ─", style=C_DIM)
        # newline between rows (no trailing newline after last)
        if i < len(results) - 1:
            t.append("\n\n", style="")   # double newline = breathing room
    return t

# ═══════════════════════════════════════════════════════════════════════════
# FULL LAYOUT  ·  columns 2 : 1  (top)  +  graphs 3  (bottom)
# ═══════════════════════════════════════════════════════════════════════════
#
#   ┌─────────────────────────── header ─────────────────────────────┐
#   │       table (ratio 2)       │      shrimp art (ratio 1)        │
#   │  ─ ─ ─ ─ ─ ─ ─ ─ ─ summary bar ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │
#   │                      graphs (ratio 3)                           │
#   └────────────────────────────────────────────────────────────────┘

def build_layout(results: list[CheckResult]) -> Layout:
    root = Layout()

    root.split_column(
        Layout(name="header",  size=3),
        Layout(name="top",     ratio=2),
        Layout(name="summary", size=3),
        Layout(name="graphs",  ratio=3),
    )

    # ── header ────────────────────────────────────────────────────────────
    ts = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    header_text = Text(justify="center")
    header_text.append("  🌐  NOC MONITOR  ", style=f"bold {C_TITLE}")
    header_text.append(f"│  {ts}  │  ",        style=C_DIM)
    header_text.append("press Ctrl+C to exit", style=C_DIM)

    root["header"].update(
        Panel(header_text, border_style=C_BORDER, padding=(0, 2))
    )

    # ── top row: table (2) | shrimp art (1) ──────────────────────────────
    root["top"].split_row(
        Layout(name="table",  ratio=2),
        Layout(name="shrimp", ratio=1),
    )

    root["top"]["table"].update(
        Panel(
            build_status_table(results),
            border_style=C_BORDER,
            padding=(0, 1),
        )
    )

    shrimp_text = Text(
    SHRIMP_ART,
    style="bright_white",
    justify="center",
)
    root["top"]["shrimp"].update(
        Panel(
            shrimp_text,
            title=f"[{C_DIM}]watermark[/{C_DIM}]",
            border_style=C_BORDER,
            padding=(1, 2),
        )
    )

    # ── summary bar ───────────────────────────────────────────────────────
    root["summary"].update(
        Panel(
            build_summary(results),
            border_style=C_BORDER,
            padding=(0, 2),
        )
    )

    # ── graphs (full width) ───────────────────────────────────────────────
    root["graphs"].update(
        Panel(
            build_graphs(results),
            title=f"[bold {C_TITLE}]LATENCY HISTORY[/bold {C_TITLE}]",
            border_style=C_BORDER,
            padding=(1, 3),
        )
    )

    return root

# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    console = Console()
    console.print(f"\n[{C_TITLE}]  🦐  NOC Monitor initialising …[/{C_TITLE}]\n")
    time.sleep(0.4)
    # first probe pass before entering live mode
    results = _run_checks()
    try:
        with Live(
            build_layout(results),
            console=console,
            refresh_per_second=2,
            screen=True,
        ) as live:
            while True:
                results = _run_checks()
                live.update(build_layout(results))
                time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt:
        console.print(f"\n[{C_DIM}]  🦐  Monitor stopped.[/{C_DIM}]\n")

if __name__ == "__main__":
    main()
