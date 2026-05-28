from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.box import ROUNDED
import requests
import time
from collections import deque, defaultdict

console = Console()

# 🌐 цели мониторинга
HOSTS = {
    "Google DNS": ("8.8.8.8", "icmp"),
    "Cloudflare DNS": ("1.1.1.1", "icmp"),

    "YouTube": ("https://youtube.com", "http"),
    "TikTok": ("https://tiktok.com", "http"),
    "Telegram": ("https://telegram.org", "http"),
    "Instagram": ("https://instagram.com", "http"),
    "Spotify": ("https://spotify.com", "http"),
    "Yandex Music": ("https://music.yandex.ru", "http"),
    "OpenAI": ("https://openai.com", "http"),
    "GitHub": ("https://github.com", "http"),
    "Wikipedia": ("https://wikipedia.org", "http"),
    "Amazon AWS": ("https://aws.amazon.com", "http")
}

# 📊 история задержек
latency_history = defaultdict(lambda: deque(maxlen=10))

# 📈 статистика
stats = defaultdict(lambda: {"ok": 0, "total": 0})


# 🟣 ICMP
def icmp_check(host):
    try:
        from ping3 import ping
        latency = ping(host, timeout=1)

        if latency is None:
            return None, "ТАЙМАУТ"

        return round(latency * 1000, 1), "ОНЛАЙН"

    except Exception:
        return None, "ОШИБКА"


# 🔵 HTTP
def http_check(url):
    try:
        start = time.time()
        r = requests.get(url, timeout=1)
        latency = round((time.time() - start) * 1000, 1)

        if r.status_code < 300:
            return latency, "ОНЛАЙН"
        elif r.status_code in (401, 403):
            return latency, "ОГРАНИЧЕН"
        else:
            return latency, "СБОЙ"

    except requests.exceptions.Timeout:
        return None, "ТАЙМАУТ"
    except Exception:
        return None, "ОШИБКА"


# 📊 график (с воздухом)
def render_graph():
    spark = "▁▂▃▄▅▆▇█"
    lines = []

    for name, hist in latency_history.items():
        if not hist:
            continue

        max_v = max(hist)
        min_v = min(hist) if min(hist) > 0 else 1

        bars = []
        for v in hist:
            if max_v == min_v:
                i = 0
            else:
                i = int((v - min_v) / (max_v - min_v) * (len(spark) - 1))

            i = max(0, min(i, len(spark) - 1))
            bars.append(spark[i])

        # 🎯 ВАЖНО: пробелы между символами + читаемость
        line = f"{name:<15} │ {' '.join(bars)}"
        lines.append(line)

        # 🌬️ ВОЗДУХ между строками
        lines.append("")

    return "\n".join(lines) if lines else "Нет данных..."


# 🧠 проверка сервиса
def check_host(name, target):
    addr, mode = target
    now = datetime.now().strftime("%H:%M:%S")

    if mode == "icmp":
        latency, status = icmp_check(addr)
    else:
        latency, status = http_check(addr)

    stats[name]["total"] += 1
    if status == "ОНЛАЙН":
        stats[name]["ok"] += 1

    uptime = round((stats[name]["ok"] / stats[name]["total"]) * 100, 1)

    if latency is not None:
        latency_history[name].append(latency)

    status_map = {
        "ОНЛАЙН": "[bold green]ОНЛАЙН[/bold green]",
        "ОГРАНИЧЕН": "[yellow]ОГРАНИЧЕН[/yellow]",
        "СБОЙ": "[magenta]СБОЙ[/magenta]",
        "ТАЙМАУТ": "[red]ТАЙМАУТ[/red]",
        "ОШИБКА": "[bold red]ОШИБКА[/bold red]"
    }

    status_ui = status_map.get(status, status)

    if latency is None:
        latency_ui = "[dim]—[/dim]"
    elif latency < 60:
        latency_ui = f"[green]{latency} мс[/]"
    elif latency < 150:
        latency_ui = f"[yellow]{latency} мс[/]"
    else:
        latency_ui = f"[red]{latency} мс[/]"

    return [now, name, addr, status_ui, latency_ui, f"{uptime}%"]


# 📊 таблица
def generate_table():
    table = Table(
        title="🌐 МОНИТОРИНГ СЕТИ (LIVE NOC)",
        border_style="cyan"
    )

    table.add_column("ВРЕМЯ", style="yellow")
    table.add_column("СЕРВИС")
    table.add_column("АДРЕС")
    table.add_column("СТАТУС", justify="center")
    table.add_column("ЗАДЕРЖКА", justify="right")
    table.add_column("АПТАЙМ", justify="right")

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(lambda x: check_host(x[0], x[1]), HOSTS.items())

        for row in results:
            table.add_row(*row)

    return table


# 🧱 layout (главный фикс)
def build_layout():
    layout = Layout()

    layout.split(
        Layout(name="top", ratio=3),
        Layout(name="bottom", ratio=2)
    )

    layout["top"].update(
        Panel(generate_table(), title="Статус сервисов", box=ROUNDED, padding=(1, 2))
    )

    layout["bottom"].update(
        Panel(render_graph(), title="График задержек", box=ROUNDED, padding=(1, 2))
    )

    return layout


# 🚀 запуск
console.print("\n[cyan]Запуск NOC мониторинга...[/cyan]\n")

with Live(build_layout(), refresh_per_second=1, screen=True) as live:
    while True:
        live.update(build_layout())
        time.sleep(1)
