"""
╔══════════════════════════════════════════════════════╗
║           🦐 NOC MONITOR  · by shrimp               ║
║       Terminal network monitoring dashboard         ║
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

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.box import SIMPLE_HEAD

# ── suppress urllib warnings ──────────────────────────────────────────────

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

REFRESH_INTERVAL = 2.0
MAX_HISTORY = 10
MAX_WORKERS = 30

HTTP_TIMEOUT = 3.0

SPARKLINE = "▁▂▃▄▅▆▇█"

# ── colours ───────────────────────────────────────────────────────────────

C_OK = "bright_green"
C_WARN = "bright_yellow"
C_SLOW = "bright_cyan"
C_ERROR = "bright_red"
C_TIMEOUT = "bright_magenta"

C_DIM = "grey70"
C_BORDER = "bright_blue"
C_TITLE = "bright_cyan"

C_LINK = "#ff9ad5"

# ── latency thresholds ────────────────────────────────────────────────────

LAT_FAST = 80
LAT_MEDIUM = 200

# ═══════════════════════════════════════════════════════════════════════════
# ASCII
# ═══════════════════════════════════════════════════════════════════════════

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
⠀⠀⠀⠀⠀⠀⠙⠻⣿⣿⠟⢀⣤⡀⠀⠀⠀⠀⠀⣀⣀⣠⣤⣤⣤⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠛⠿⠿⡿⠂⣀⣠⣤⣤⣤⣀⣉⣉⠉⠉⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠉⠙⠛⠛⠛⠛⠋⠉⠉⠁⠀⠀⠀⠀

              SHRIMP NODE
         NETWORK OBSERVATION
"""

# ═══════════════════════════════════════════════════════════════════════════
# STATUS
# ═══════════════════════════════════════════════════════════════════════════

class Status(str, Enum):
    ONLINE = "ONLINE"
    LIMITED = "LIMITED"
    FAIL = "FAIL"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"


STATUS_LABEL = {
    Status.ONLINE:   (C_OK, "● ONLINE"),
    Status.LIMITED:  (C_WARN, "◐ LIMITED"),
    Status.FAIL:     (C_ERROR, "✕ FAIL"),
    Status.TIMEOUT:  (C_TIMEOUT, "⧗ TIMEOUT"),
    Status.ERROR:    (C_DIM, "? ERROR"),
}

# ═══════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class CheckResult:
    name: str
    address: str
    status: Status
    latency: Optional[float]
    checked_at: str = field(
        default_factory=lambda: datetime.now().strftime("%H:%M:%S")
    )


@dataclass
class HostStats:
    ok: int = 0
    total: int = 0
    history: Deque[float] = field(
        default_factory=lambda: deque(maxlen=MAX_HISTORY)
    )

    @property
    def uptime(self) -> float:
        return (self.ok / self.total * 100) if self.total else 0.0

# ═══════════════════════════════════════════════════════════════════════════
# HOSTS
# ═══════════════════════════════════════════════════════════════════════════

HOSTS: Dict[str, Tuple[str, str, str]] = {

    # ── SOCIAL ────────────────────────────────────────────────────────────

    "YouTube": (
        "https://youtube.com",
        "https://youtube.com",
        "http"
    ),

    "TikTok": (
        "https://tiktok.com",
        "https://tiktok.com",
        "http"
    ),

    "Instagram": (
        "https://instagram.com",
        "https://instagram.com",
        "http"
    ),

    "Twitter/X": (
        "https://x.com",
        "https://x.com",
        "http"
    ),

    "Facebook": (
        "https://facebook.com",
        "https://facebook.com",
        "http"
    ),

    "Reddit": (
        "https://reddit.com",
        "https://reddit.com",
        "http"
    ),

    # ── MESSAGING ────────────────────────────────────────────────────────

    "Telegram": (
        "https://telegram.org",
        "https://telegram.org",
        "http"
    ),

    "Discord": (
        "https://discord.com",
        "https://discord.com",
        "http"
    ),

    "WhatsApp": (
        "https://web.whatsapp.com",
        "https://web.whatsapp.com",
        "http"
    ),

    # ── STREAMING ────────────────────────────────────────────────────────

    "Spotify": (
        "https://spotify.com",
        "https://spotify.com",
        "http"
    ),

    "Yandex Music": (
        "https://music.yandex.ru",
        "https://music.yandex.ru",
        "http"
    ),

    "Netflix": (
        "https://netflix.com",
        "https://netflix.com",
        "http"
    ),

    "Twitch": (
        "https://twitch.tv",
        "https://twitch.tv",
        "http"
    ),

    "Steam": (
        "https://store.steampowered.com",
        "https://store.steampowered.com",
        "http"
    ),

    # ── CLOUD / DEV ──────────────────────────────────────────────────────

    "OpenAI": (
        "https://openai.com",
        "https://openai.com",
        "http"
    ),

    "Anthropic": (
        "https://anthropic.com",
        "https://anthropic.com",
        "http"
    ),

    "GitHub": (
        "https://github.com",
        "https://github.com",
        "http"
    ),

    "GitLab": (
        "https://gitlab.com",
        "https://gitlab.com",
        "http"
    ),

    "Cloudflare": (
        "https://cloudflare.com",
        "https://cloudflare.com",
        "http"
    ),
}

# ═══════════════════════════════════════════════════════════════════════════
# GRAPH GROUPS
# ═══════════════════════════════════════════════════════════════════════════

GRAPH_GROUPS = {
    "SOCIAL": [
        "YouTube",
        "TikTok",
        "Instagram",
        "Twitter/X",
        "Facebook",
        "Reddit",
    ],

    "MESSAGING": [
        "Telegram",
        "Discord",
        "WhatsApp",
    ],

    "STREAMING": [
        "Spotify",
        "Yandex Music",
        "Netflix",
        "Twitch",
        "Steam",
    ],

    "CLOUD / DEV": [
        "OpenAI",
        "Anthropic",
        "GitHub",
        "GitLab",
        "Cloudflare",
    ],
}

# ═══════════════════════════════════════════════════════════════════════════
# PROBE
# ═══════════════════════════════════════════════════════════════════════════

def _probe_http(url: str) -> Tuple[Optional[float], Status]:

    try:

        start = time.monotonic()

        response = requests.get(
            url,
            timeout=HTTP_TIMEOUT,
            allow_redirects=True,
            headers={
                "User-Agent": "NOC-Monitor/2.0"
            },
            verify=False,
        )

        latency = round(
            (time.monotonic() - start) * 1000,
            1
        )

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


def probe(
    name: str,
    display_addr: str,
    probe_addr: str,
    mode: str
) -> CheckResult:

    latency, status = _probe_http(probe_addr)

    return CheckResult(
        name=name,
        address=display_addr,
        status=status,
        latency=latency,
    )

# ═══════════════════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════════════════

host_stats: Dict[str, HostStats] = defaultdict(HostStats)

def record(result: CheckResult) -> None:

    stats = host_stats[result.name]

    stats.total += 1

    if result.status == Status.ONLINE:
        stats.ok += 1

    if result.latency is not None:
        stats.history.append(result.latency)

# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
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

    return Text(
        f"{latency:>7.1f} ms",
        style=colour
    )


def _status_text(status: Status) -> Text:

    colour, label = STATUS_LABEL[status]

    return Text(
        label,
        style=f"bold {colour}"
    )


def _uptime_text(value: float) -> Text:

    if value >= 99:
        style = C_OK

    elif value >= 90:
        style = C_SLOW

    else:
        style = C_ERROR

    return Text(
        f"{value:>5.1f}%",
        style=style
    )

# ═══════════════════════════════════════════════════════════════════════════
# CHECKS
# ═══════════════════════════════════════════════════════════════════════════

def _run_checks() -> list[CheckResult]:

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:

        futures = {
            pool.submit(
                probe,
                name,
                display_addr,
                probe_addr,
                mode
            ): name

            for name, (
                display_addr,
                probe_addr,
                mode
            ) in HOSTS.items()
        }

        results = []

        for future in as_completed(futures):

            result = future.result()

            record(result)

            results.append(result)

    order = list(HOSTS.keys())

    results.sort(
        key=lambda r: order.index(r.name)
    )

    return results

# ═══════════════════════════════════════════════════════════════════════════
# TABLE
# ═══════════════════════════════════════════════════════════════════════════

def build_status_table(results: list[CheckResult]) -> Table:

    table = Table(
        box=SIMPLE_HEAD,
        border_style=C_BORDER,
        header_style=f"bold {C_TITLE}",
        show_edge=True,
        expand=True,
    )

    table.add_column(
        "TIME",
        width=10,
        no_wrap=True,
    )

    table.add_column(
        "SERVICE",
        min_width=14,
    )

    table.add_column(
        "ADDRESS",
        style=C_LINK,
        min_width=34,
        no_wrap=True,
    )

    table.add_column(
        "STATUS",
        width=14,
    )

    table.add_column(
        "LATENCY",
        width=12,
        justify="right",
    )

    table.add_column(
        "UPTIME",
        width=8,
        justify="right",
    )

    for result in results:

        stats = host_stats[result.name]

        table.add_row(
            result.checked_at,
            result.name,
            result.address,
            _status_text(result.status),
            _latency_text(result.latency),
            _uptime_text(stats.uptime),
        )

    return table

# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════

def build_summary(results: list[CheckResult]) -> Text:

    total = len(results)

    online = sum(
        1 for r in results
        if r.status == Status.ONLINE
    )

    limited = sum(
        1 for r in results
        if r.status == Status.LIMITED
    )

    failing = total - online - limited

    latencies = [
        r.latency
        for r in results
        if r.latency is not None
    ]

    avg = (
        sum(latencies) / len(latencies)
        if latencies
        else 0
    )

    text = Text()

    text.append(
        f" {online}/{total} ",
        style=f"bold {C_OK}"
    )

    text.append(
        "online  ",
        style=C_DIM
    )

    if limited:
        text.append(
            f"{limited} limited  ",
            style=C_WARN
        )

    if failing:
        text.append(
            f"{failing} failing  ",
            style=C_ERROR
        )

    text.append(
        f"avg {avg:.0f} ms",
        style=C_SLOW
    )

    text.append("  🦐")

    return text

# ───────────────────────────────────────────────────────────────────────────
# COLOURS
# ───────────────────────────────────────────────────────────────────────────

C_OK = "bright_green"
C_WARN = "bright_yellow"
C_SLOW = "bright_cyan"
C_ERROR = "bright_red"
C_TIMEOUT = "bright_magenta"

# brighter UI
C_DIM = "bright_white"

# vivid borders
C_BORDER = "bright_blue"

C_TITLE = "bright_cyan"

# links
C_LINK = "#ff9ad5"

# ───────────────────────────────────────────────────────────────────────────
# GRAPH PANELS
# ───────────────────────────────────────────────────────────────────────────

def build_graph_panel(
    names: list[str],
    title: str
) -> Panel:

    text = Text()

    for idx, name in enumerate(names):

        history = host_stats[name].history

        short = name[:11]

        text.append(
            f"{short:<11}",
            style="bold bright_white"
        )

        text.append(
            " ",
            style="bright_blue"
        )

        if history:

            mn = min(history)
            mx = max(history)

            span = mx - mn or 1

            for value in history:

                spark_idx = int(
                    (value - mn) / span * (len(SPARKLINE) - 1)
                )

                char = SPARKLINE[spark_idx]

                colour = (
                    C_OK
                    if value < LAT_FAST
                    else C_SLOW
                    if value < LAT_MEDIUM
                    else C_ERROR
                )

                text.append(
                    char,
                    style=f"bold {colour}"
                )

            latest_colour = (
                C_OK
                if history[-1] < LAT_FAST
                else C_SLOW
                if history[-1] < LAT_MEDIUM
                else C_ERROR
            )

            text.append(
                f" {history[-1]:>4.0f}ms",
                style=f"bold {latest_colour}"
            )

        else:

            text.append(
                "──────────",
                style="bright_red"
            )

        if idx != len(names) - 1:
            text.append("\n")

    return Panel(
        text,
        title=f"[bold {C_TITLE}]{title}[/bold {C_TITLE}]",
        border_style=C_BORDER,
        padding=(1, 1),
    )

# ───────────────────────────────────────────────────────────────────────────
# LAYOUT
# ───────────────────────────────────────────────────────────────────────────

def build_layout(results: list[CheckResult]) -> Layout:

    root = Layout()

    root.split_column(
        Layout(name="header", size=3),
        Layout(name="top", ratio=3),
        Layout(name="summary", size=3),
        Layout(name="graphs", ratio=3),
    )

    # ── HEADER ────────────────────────────────────────────────────────────

    timestamp = datetime.now().strftime(
        "%Y-%m-%d  %H:%M:%S"
    )

    header = Text(justify="center")

    header.append(
        " 🌐 SHRIMP NOC MONITOR ",
        style=f"bold {C_TITLE}"
    )

    header.append(
        f"│ {timestamp} │ ",
        style="bright_white"
    )

    header.append(
        "Ctrl+C to exit",
        style="bright_white"
    )

    root["header"].update(
        Panel(
            header,
            border_style=C_BORDER,
        )
    )

    # ── TOP ───────────────────────────────────────────────────────────────

    root["top"].split_row(
        Layout(name="table", ratio=2),
        Layout(name="art", ratio=1),
    )

    root["top"]["table"].update(
        Panel(
            build_status_table(results),
            border_style=C_BORDER,
            padding=(0, 1),
        )
    )

    shrimp = Text(
        SHRIMP_ART,
        justify="center",
        style="bright_white",
    )

    root["top"]["art"].update(
        Panel(
            shrimp,
            title="[bright_cyan]watermark[/bright_cyan]",
            border_style=C_BORDER,
        )
    )

    # ── SUMMARY ───────────────────────────────────────────────────────────

    root["summary"].update(
        Panel(
            build_summary(results),
            border_style=C_BORDER,
            padding=(0, 2),
        )
    )

    # ── GRAPHS GRID ──────────────────────────────────────────────────────

    root["graphs"].split_row(
        Layout(name="left"),
        Layout(name="right"),
    )

    # LEFT

    root["graphs"]["left"].split_column(
        Layout(name="g1"),
        Layout(name="spacer1", size=1),
        Layout(name="g2"),
    )

    # RIGHT

    root["graphs"]["right"].split_column(
        Layout(name="g3"),
        Layout(name="spacer2", size=1),
        Layout(name="g4"),
    )

    # spacers

    root["graphs"]["left"]["spacer1"].update(
        Text(" ")
    )

    root["graphs"]["right"]["spacer2"].update(
        Text(" ")
    )

    groups = list(GRAPH_GROUPS.items())

    root["graphs"]["left"]["g1"].update(
        build_graph_panel(
            groups[0][1],
            groups[0][0]
        )
    )

    root["graphs"]["left"]["g2"].update(
        build_graph_panel(
            groups[1][1],
            groups[1][0]
        )
    )

    root["graphs"]["right"]["g3"].update(
        build_graph_panel(
            groups[2][1],
            groups[2][0]
        )
    )

    root["graphs"]["right"]["g4"].update(
        build_graph_panel(
            groups[3][1],
            groups[3][0]
        )
    )

    return root

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:

    console = Console()

    console.print(
        f"\n[{C_TITLE}]🦐 Initialising Shrimp Node…[/{C_TITLE}]\n"
    )

    time.sleep(0.4)

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

                live.update(
                    build_layout(results)
                )

                time.sleep(
                    REFRESH_INTERVAL
                )

    except KeyboardInterrupt:

        console.print(
            f"\n[{C_DIM}]🦐 Monitor stopped.[/{C_DIM}]\n"
        )

# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()
