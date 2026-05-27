import subprocess
from datetime import datetime

#Список хостов для проверки
HOSTS = [
    "8.8.8.8",
    "google.com",
    "192.168.1.1"
]

def ping_host(host):
    """
    Проверка доступности хостов ICMP пакетом.
    """
    result = subprocess.run(
        ["ping", "-c", "1", "-W", "2", host],
        capture_output=True,
        text=True
    )

    current_time = datetime.now().strftime("%H:%M:%S")

    if result.returncode == 0:
        status = "UP"
    else:
        status = "DOWN"
    print(f"[{current_time}]  {host:<15}  STATUS: {status}")
print("\n========== СОСТОЯНИЕ СЕТИ ==========\n")

for host in HOSTS:
    ping_host(host)
print("\n=============== КОНЕЦ ================\n")
