import subprocess

hosts = ["8.8.8.8", "google.com", "192.168.1.1"]

for host in hosts:
    result = subprocess.run(["ping", "-c", "1", host], capture_output=True)
    
    if result.returncode == 0:
        print(f"{host} is UP")
    else:
        print(f"{host} is DOWN")
