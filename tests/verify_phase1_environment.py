"""
Phase 1: Environment & Connectivity Verification Script
======================================================
This diagnostic script tests your local system environment, toolchains,
network connectivity to Open-Meteo, and local services before running the pipeline.
"""

import sys
import os
import socket
import subprocess
from pathlib import Path

def print_status(component: str, is_ok: bool, message: str):
    symbol = "[PASS]" if is_ok else "[FAIL]"
    print(f"{symbol:8} | {component:<25} | {message}")

def check_python():
    v = sys.version_info
    version_str = f"{v.major}.{v.minor}.{v.micro}"
    # Spark 3.5 officially supports Python 3.8 - 3.12
    if 3 <= v.major and 8 <= v.minor <= 12:
        print_status("Python Version", True, f"Python {version_str} (Compatible with PySpark 3.5)")
        return True
    else:
        print_status(
            "Python Version", 
            False, 
            f"Python {version_str} detected. WARNING: PySpark 3.5 requires Python 3.8 to 3.12! (Python 3.13+ causes bytecode incompatibilities)."
        )
        return False

def check_dependencies():
    packages = [
        ("requests", "requests"),
        ("dotenv", "python-dotenv"),
        ("kafka", "kafka-python-ng or kafka-python"),
        ("pyspark", "pyspark"),
        ("psycopg2", "psycopg2-binary"),
    ]
    all_ok = True
    for module_name, pip_name in packages:
        try:
            __import__(module_name)
            print_status(f"Package: {module_name}", True, f"Installed ({pip_name})")
        except ImportError:
            print_status(f"Package: {module_name}", False, f"Missing. Install with: pip install {pip_name}")
            all_ok = False
    return all_ok

def check_java():
    java_home = os.environ.get("JAVA_HOME")
    try:
        res = subprocess.run(["java", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out = res.stderr or res.stdout
        first_line = out.splitlines()[0] if out else "Unknown Java version"
        print_status("Java Runtime (JDK)", True, f"{first_line} (JAVA_HOME={java_home or 'Not Set'})")
        return True
    except FileNotFoundError:
        print_status("Java Runtime (JDK)", False, "Java is not installed or not in system PATH. Required: OpenJDK 11 or 17 LTS.")
        return False

def check_port(host: str, port: int, service_name: str):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    try:
        s.connect((host, port))
        s.close()
        print_status(service_name, True, f"Accessible on {host}:{port}")
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        print_status(service_name, False, f"Not running or unreachable on {host}:{port}")
        return False

def check_open_meteo_api():
    try:
        import requests
        url = "https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current=temperature_2m,relative_humidity_2m"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            curr = data.get("current", {})
            temp = curr.get("temperature_2m")
            humidity = curr.get("relative_humidity_2m")
            print_status("Open-Meteo REST API", True, f"HTTP 200 OK | Sample Temp: {temp}C, Humidity: {humidity}%")
            return True
        else:
            print_status("Open-Meteo REST API", False, f"HTTP Status {resp.status_code}: {resp.text}")
            return False
    except Exception as e:
        print_status("Open-Meteo REST API", False, f"Connection error: {e}")
        return False

def check_project_structure():
    required_dirs = ["config", "producer", "spark", "sql", "tests", "checkpoints"]
    all_ok = True
    base = Path(__file__).resolve().parent.parent
    for d in required_dirs:
        dir_path = base / d
        if dir_path.is_dir():
            print_status(f"Directory: {d}/", True, "Exists")
        else:
            print_status(f"Directory: {d}/", False, "Missing")
            all_ok = False
    return all_ok

if __name__ == "__main__":
    print("=" * 80)
    print(" REAL-TIME WEATHER PIPELINE: PHASE 1 DIAGNOSTIC & VERIFICATION TEST")
    print("=" * 80)
    
    print("\n[1] PROJECT STRUCTURE CHECK:")
    check_project_structure()
    
    print("\n[2] PYTHON ENVIRONMENT & RUNTIME CHECK:")
    check_python()
    
    print("\n[3] PYTHON DEPENDENCIES CHECK:")
    check_dependencies()
    
    print("\n[4] JAVA (JDK) CHECK (Required for Apache Spark & Kafka):")
    check_java()
    
    print("\n[5] LOCAL PORTS & SERVICES CHECK (Optional for Phase 1, required for later phases):")
    check_port("localhost", 9092, "Apache Kafka Broker")
    check_port("localhost", 5432, "PostgreSQL Database")
    
    print("\n[6] EXTERNAL DATA SOURCE CHECK:")
    check_open_meteo_api()
    
    print("\n" + "=" * 80)
    print("Verification complete. Review any [FAIL] markers before running pipelines.")
    print("=" * 80)
