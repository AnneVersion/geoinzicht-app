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
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path

PORT = int(sys.argv[sys.argv.index('--port') + 1]) if '--port' in sys.argv else 8091
APP_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(APP_DIR)

# Refresh state (thread-safe)
_refresh_lock = threading.Lock()
_refresh_running = False
_refresh_log = []
_refresh_last = None
_refresh_error = None

# === BAG API Individuele Bevragingen (Kadaster) ===
# Key: env BAG_API_KEY of apibag.txt (twee mappen omhoog, of naast serve.py).
# De key blijft server-side; hij komt NOOIT in index.html of het git-repo terecht.
BAG_IB_BASE = 'https://api.bag.kadaster.nl/lvbag/individuelebevragingen/v2'
_bag_lvc_cache = {}


def _load_bag_api_key():
    key = os.environ.get('BAG_API_KEY', '').strip()
    if key:
        return key
    for p in (os.path.join(APP_DIR, 'apibag.txt'),
              os.path.join(APP_DIR, '..', '..', 'apibag.txt')):
        try:
            with open(p, 'r', encoding='utf-8') as f:
                key = f.read().strip()
            if key:
                return key
        except OSError:
            continue
    return ''


def _bag_object_pad(identificatie):
    """Bepaal het API-pad op basis van het objecttype in de BAG-identificatie (positie 5-6)."""
    t = identificatie[4:6] if len(identificatie) >= 6 else ''
    return {'01': 'verblijfsobjecten', '10': 'panden', '20': 'nummeraanduidingen',
            '02': 'ligplaatsen', '03': 'standplaatsen'}.get(t, 'verblijfsobjecten')


def bag_lvc(identificatie):
    """Haal de levenscyclus (alle voorkomens incl. brondocument) op bij de Kadaster BAG API."""
    if identificatie in _bag_lvc_cache:
        return _bag_lvc_cache[identificatie]
    key = _load_bag_api_key()
    if not key:
        return {'error': 'Geen BAG API key gevonden (BAG_API_KEY of apibag.txt).'}
    pad = _bag_object_pad(identificatie)
    url = f"{BAG_IB_BASE}/{pad}/{urllib.parse.quote(identificatie)}/lvc?geheleObject=true"
    req = urllib.request.Request(url, headers={
        'X-Api-Key': key,
        'Accept': 'application/hal+json',
        'Accept-Crs': 'epsg:28992',
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        return {'error': f'BAG API {e.code}: {e.reason}'}
    except Exception as e:
        return {'error': f'BAG API niet bereikbaar: {e}'}

    # Voorkomens kunnen genest zitten; parse defensief.
    emb = data.get('_embedded', {})
    ruwe = emb.get('voorkomens') or emb.get(pad) or []
    versies = []
    for item in ruwe:
        obj = item.get('verblijfsobject') or item.get('pand') or item.get('nummeraanduiding') \
            or item.get('ligplaats') or item.get('standplaats') or item
        vk = obj.get('voorkomen', {}) if isinstance(obj, dict) else {}
        gebruiksdoelen = obj.get('gebruiksdoelen') or ([] if not obj.get('gebruiksdoel') else [obj.get('gebruiksdoel')])
        versies.append({
            'beginGeldigheid': vk.get('beginGeldigheid') or obj.get('beginGeldigheid'),
            'eindGeldigheid': vk.get('eindGeldigheid') or obj.get('eindGeldigheid'),
            'status': obj.get('status', ''),
            'gebruiksdoel': ', '.join(gebruiksdoelen) if gebruiksdoelen else '',
            'oppervlakte': obj.get('oppervlakte'),
            'documentdatum': obj.get('documentdatum') or vk.get('documentdatum'),
            'documentnummer': obj.get('documentnummer') or vk.get('documentnummer'),
            'voorkomen': vk.get('voorkomenidentificatie'),
        })
    versies.sort(key=lambda v: (v.get('beginGeldigheid') or ''))
    resultaat = {'bron': 'Kadaster BAG API', 'objecttype': pad, 'versies': versies}
    if not versies:
        resultaat['error'] = 'Geen voorkomens gevonden voor dit object.'
    _bag_lvc_cache[identificatie] = resultaat
    return resultaat


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
        {'naam': 'Zorgkosten (DWH)', 'cmd': [sys.executable, 'enrich_from_dwh.py', '--domains', 'zorgkosten']},
        {'naam': 'Criminaliteit (Politie API)', 'cmd': [sys.executable, 'enrich_from_politie_api.py']},
        {'naam': 'Gezondheid (RIVM API)', 'cmd': [sys.executable, 'enrich_from_rivm.py']},
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


# === Services: status, startknoppen en autostart (config in services.json) ===
SERVICES_FILE = os.path.join(APP_DIR, 'services.json')
_service_procs = {}


def _load_services():
    try:
        with open(SERVICES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f).get('services', [])
    except Exception:
        return []


def _port_open(port):
    if not port:
        return None
    s = socket.socket()
    s.settimeout(0.4)
    try:
        s.connect(('127.0.0.1', int(port)))
        s.close()
        return True
    except Exception:
        return False


def services_status():
    out = [{'name': 'serve', 'label': 'GeoInzicht server', 'port': PORT, 'running': True, 'startbaar': False}]
    for svc in _load_services():
        out.append({
            'name': svc.get('name'),
            'label': svc.get('label') or svc.get('name'),
            'port': svc.get('port'),
            'running': _port_open(svc.get('port')),
            'startbaar': bool(svc.get('cmd')),
        })
    return {'services': out}


def service_start(name):
    for svc in _load_services():
        if svc.get('name') == name:
            if not svc.get('cmd'):
                return {'error': f'Geen startcommando geconfigureerd voor {name}; vul cmd/cwd in services.json in.'}
            if _port_open(svc.get('port')):
                return {'status': 'al actief'}
            try:
                cwd = svc.get('cwd') or APP_DIR
                if cwd == '.':
                    cwd = APP_DIR
                p = subprocess.Popen(svc['cmd'], cwd=cwd,
                                     creationflags=getattr(subprocess, 'CREATE_NEW_CONSOLE', 0))
                _service_procs[name] = p.pid
                return {'status': 'gestart', 'pid': p.pid}
            except Exception as e:
                return {'error': f'Start mislukt: {e}'}
    return {'error': f'Onbekende service: {name}'}


def services_autostart():
    """Start bij het opstarten van serve.py alle services met autostart=true die nog niet draaien."""
    for svc in _load_services():
        if svc.get('autostart') and svc.get('cmd') and not _port_open(svc.get('port')):
            r = service_start(svc.get('name'))
            print(f"  AUTOSTART {svc.get('name')}: {r}")


# === Overheid Catalogus: data.overheid.nl (CKAN package_search) ===
_ovh_cache = {}


def overheidcatalogus_zoek(q, sort, rows):
    """Zoek datasets op data.overheid.nl; sortering standaard op peildatum (metadata_modified)."""
    key = f'{q}|{sort}|{rows}'
    if key in _ovh_cache:
        return _ovh_cache[key]
    params = urllib.parse.urlencode({'q': q, 'rows': rows, 'sort': sort})
    url = 'https://data.overheid.nl/data/api/3/action/package_search?' + params
    req = urllib.request.Request(url, headers={'Accept': 'application/json', 'User-Agent': 'GeoInzicht-app'})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        return {'error': f'data.overheid.nl niet bereikbaar: {e}'}
    _ovh_cache[key] = data
    return data


# === Officiële bekendmakingen (Gemeenteblad e.d.) via KOOP SRU API ===
BEKENDMAKINGEN_SRU = 'https://repository.overheid.nl/sru'
_bekendmakingen_cache = {}


def bekendmakingen_zoek(adres):
    """Zoek officiële bekendmakingen (o.a. Gemeenteblad) op een adres via de open SRU-API."""
    if adres in _bekendmakingen_cache:
        return _bekendmakingen_cache[adres]
    schoon = adres.replace('"', ' ').strip()
    query = f'(cql.textAndIndexes="{schoon}") AND c.product-area=="officielepublicaties" sortBy dt.modified/sort.descending'
    params = urllib.parse.urlencode({
        'operation': 'searchRetrieve',
        'version': '2.0',
        'query': query,
        'maximumRecords': '15',
        'startRecord': '1',
    })
    req = urllib.request.Request(BEKENDMAKINGEN_SRU + '?' + params,
                                 headers={'Accept': 'application/xml', 'User-Agent': 'GeoInzicht-app'})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            xml_data = resp.read()
    except Exception as e:
        return {'error': f'SRU-API niet bereikbaar: {e}'}
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        return {'error': f'SRU-antwoord niet leesbaar: {e}'}
    items = []
    for rec in root.iter():
        if not rec.tag.endswith('recordData'):
            continue
        titel = datum = url_ = bron = ''
        for el in rec.iter():
            tag = el.tag.split('}')[-1].lower()
            tekst = (el.text or '').strip()
            if not tekst:
                continue
            if tag == 'title' and not titel:
                titel = tekst
            elif tag in ('issued', 'modified', 'date', 'available') and not datum:
                datum = tekst[:10]
            elif tag in ('preferredurl', 'itemurl') and not url_:
                url_ = tekst
            elif tag == 'identifier' and not url_ and tekst.startswith('http'):
                url_ = tekst
            elif tag in ('publicationname', 'creator') and not bron:
                bron = tekst
        if titel or url_:
            items.append({'titel': titel, 'datum': datum, 'url': url_, 'bron': bron})
    resultaat = {'adres': adres, 'aantal': len(items), 'bekendmakingen': items}
    _bekendmakingen_cache[adres] = resultaat
    return resultaat


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

        if self.path.startswith('/api/services/status'):
            return self._json_response(services_status())

        if self.path.startswith('/api/services/start'):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            naam = (qs.get('name') or [''])[0].strip()
            if not naam:
                return self._json_response({'error': 'Geef een service mee via ?name='}, 400)
            return self._json_response(service_start(naam))

        if self.path.startswith('/api/overheidcatalogus'):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            q = (qs.get('q') or [''])[0]
            sort = (qs.get('sort') or ['metadata_modified desc'])[0]
            rows = (qs.get('rows') or ['25'])[0]
            return self._json_response(overheidcatalogus_zoek(q, sort, rows))

        if self.path.startswith('/api/bekendmakingen'):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            adres = (qs.get('q') or [''])[0].strip()
            if not adres:
                return self._json_response({'error': 'Geef een adres mee via ?q='}, 400)
            return self._json_response(bekendmakingen_zoek(adres))

        if self.path.startswith('/api/bag/lvc'):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            identificatie = (qs.get('id') or [''])[0].strip()
            if not identificatie or not identificatie.isdigit():
                return self._json_response({'error': 'Geef een geldige BAG-identificatie mee via ?id='}, 400)
            return self._json_response(bag_lvc(identificatie))

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
    services_autostart()
    print(f"\n  Ctrl+C om te stoppen\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server gestopt.")
