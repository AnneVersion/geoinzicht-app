"""
GeoInzicht Server met live data-refresh.
=========================================
Serveert de app + biedt een /api/refresh endpoint om GeoJSON data
te vernieuwen vanuit CBS/PDOK/DWH bronnen.

Gebruik:
    python serve.py
    python serve.py --port 8091
"""
import http.server
import socketserver
import socket
import sys
import os
import json
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

PORT = int(sys.argv[sys.argv.index('--port') + 1]) if '--port' in sys.argv else 8080
APP_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(APP_DIR)

# Refresh state (thread-safe)
_refresh_lock = threading.Lock()
_refresh_running = False
_refresh_log = []
_refresh_last = None
_refresh_error = None


def get_data_freshness():
    """Check wanneer GeoJSON bestanden voor het laatst verrijkt zijn."""
    meta_file = os.path.join(APP_DIR, 'gemeenten_2024.geojson')
    if not os.path.exists(meta_file):
        return None
    try:
        with open(meta_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        meta = data.get('metadata', {})
        return meta.get('enriched_at')
    except Exception:
        return None


def run_refresh():
    """Draai het complete refresh-pipeline in een achtergrondthread."""
    global _refresh_running, _refresh_log, _refresh_last, _refresh_error
    _refresh_log = []
    _refresh_error = None

    scripts = [
        {'naam': 'CBS Kerncijfers (PDOK WFS)', 'cmd': [sys.executable, 'build_geojson.py']},
        {'naam': 'Bodemgebruik + Landbouw (CBS API)', 'cmd': [sys.executable, 'enrich_from_sql.py']},
        {'naam': 'Flora & Fauna (GBIF API)', 'cmd': [sys.executable, 'enrich_flora_fauna.py']},
        {'naam': 'Zorgkosten + Criminaliteit (DWH)', 'cmd': [sys.executable, 'enrich_from_dwh.py']},
    ]

    try:
        for i, script in enumerate(scripts):
            step = f"[{i+1}/{len(scripts)}] {script['naam']}"
            _refresh_log.append(f"{datetime.now().strftime('%H:%M:%S')} | START {step}")
            print(f"  REFRESH {step}")

            script_path = script['cmd'][1]
            if not os.path.exists(os.path.join(APP_DIR, script_path)):
                _refresh_log.append(f"  OVERGESLAGEN: {script_path} niet gevonden")
                continue

            try:
                result = subprocess.run(
                    script['cmd'],
                    cwd=APP_DIR,
                    capture_output=True,
                    text=True,
                    timeout=600  # Max 10 min per script
                )
                if result.returncode == 0:
                    _refresh_log.append(f"  OK ({script['naam']})")
                else:
                    _refresh_log.append(f"  FOUT (exit {result.returncode}): {result.stderr[-200:]}")
            except subprocess.TimeoutExpired:
                _refresh_log.append(f"  TIMEOUT na 600s: {script['naam']}")
            except Exception as e:
                _refresh_log.append(f"  FOUT: {e}")

        _refresh_last = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        _refresh_log.append(f"\n{datetime.now().strftime('%H:%M:%S')} | REFRESH KLAAR")
    except Exception as e:
        _refresh_error = str(e)
        _refresh_log.append(f"FATALE FOUT: {e}")
    finally:
        _refresh_running = False


class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache')
        super().end_headers()

    def log_message(self, format, *args):
        # Verberg /api/status polling uit de logs
        if '/api/status' not in (args[0] if args else ''):
            print(f"  {self.address_string()} - {format % args}")

    def do_GET(self):
        # API endpoints
        if self.path == '/api/status':
            return self._json_response({
                'freshness': get_data_freshness(),
                'refresh_running': _refresh_running,
                'refresh_last': _refresh_last,
                'refresh_error': _refresh_error,
            })

        if self.path == '/api/refresh':
            return self._start_refresh()

        if self.path == '/api/refresh/log':
            return self._json_response({
                'running': _refresh_running,
                'log': _refresh_log,
                'last': _refresh_last,
                'error': _refresh_error,
            })

        # Normale statische bestanden
        return super().do_GET()

    def do_POST(self):
        if self.path == '/api/refresh':
            return self._start_refresh()
        self.send_error(404)

    def _start_refresh(self):
        global _refresh_running
        with _refresh_lock:
            if _refresh_running:
                return self._json_response({'status': 'already_running', 'log': _refresh_log})
            _refresh_running = True

        t = threading.Thread(target=run_refresh, daemon=True)
        t.start()
        return self._json_response({'status': 'started'})

    def _json_response(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        self.wfile.write(body)


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"


# Use ThreadingTCPServer for concurrent requests (refresh + UI)
class ThreadedServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


with ThreadedServer(("0.0.0.0", PORT), Handler) as httpd:
    local_ip = get_local_ip()
    freshness = get_data_freshness()
    print("=" * 50)
    print("  GeoInzicht Server (met live refresh)")
    print("=" * 50)
    print(f"\n  App:     http://localhost:{PORT}")
    print(f"  Netwerk: http://{local_ip}:{PORT}")
    print(f"  Refresh: http://localhost:{PORT}/api/refresh")
    print(f"  Status:  http://localhost:{PORT}/api/status")
    if freshness:
        print(f"\n  Data laatst verrijkt: {freshness}")
    print(f"\n  Ctrl+C om te stoppen\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server gestopt.")
