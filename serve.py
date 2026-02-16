"""
Mini-server om GeoInzicht standalone te serveren op je lokale netwerk.
Start deze, open http://<jouw-ip>:8080 op je iPad.

Gebruik:
    python serve.py
    python serve.py --port 8080
"""
import http.server
import socketserver
import socket
import sys
import os

PORT = int(sys.argv[sys.argv.index('--port') + 1]) if '--port' in sys.argv else 8080

os.chdir(os.path.dirname(os.path.abspath(__file__)))

class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        # CORS headers nodig voor CBS/PDOK API calls
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache')
        super().end_headers()

    def log_message(self, format, *args):
        print(f"  {self.address_string()} - {format % args}")

# Vind lokaal IP
def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "localhost"

with socketserver.TCPServer(("0.0.0.0", PORT), Handler) as httpd:
    local_ip = get_local_ip()
    print("=" * 50)
    print("  GeoInzicht Standalone Server")
    print("=" * 50)
    print(f"\n  Lokaal:  http://localhost:{PORT}")
    print(f"  Netwerk: http://{local_ip}:{PORT}")
    print(f"\n  Open bovenstaand netwerk-adres op je iPad!")
    print(f"  (iPad en PC moeten op hetzelfde WiFi zitten)\n")
    print("  Ctrl+C om te stoppen\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server gestopt.")
