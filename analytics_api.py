"""
GeoInzicht Analytics API
========================
Lokale Flask API die bezoekersdata opslaat in SQL Server.
Start met: python analytics_api.py

De frontend stuurt POST requests naar deze API.
CORS is ingeschakeld zodat GitHub Pages de API kan bereiken.

Vereisten:
    pip install flask flask-cors pyodbc
"""

import datetime
import json
import os
import sys

import pyodbc
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Sta cross-origin requests toe (GitHub Pages -> localhost)

# ---------------------------------------------------------------------------
# Database configuratie
# ---------------------------------------------------------------------------
DB_SERVER = os.environ.get('DB_SERVER', 'localhost')
DB_NAME = os.environ.get('DB_NAME', 'CBS_Buurtdata')
DB_DRIVER = '{ODBC Driver 17 for SQL Server}'

def get_conn():
    """Maak verbinding met SQL Server (Windows Authentication)."""
    return pyodbc.connect(
        f'DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes;',
        autocommit=True
    )

def ensure_tables():
    """Maak ontbrekende tabellen aan."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'analytics_uploads')
            BEGIN
                CREATE TABLE analytics_uploads (
                    id              INT IDENTITY(1,1) PRIMARY KEY,
                    upload_id       VARCHAR(100) NOT NULL,
                    visitor_id      VARCHAR(100) DEFAULT '',
                    session_id      VARCHAR(100) DEFAULT '',
                    name            NVARCHAR(500) DEFAULT '',
                    description     NVARCHAR(2000) DEFAULT '',
                    filename        NVARCHAR(500) DEFAULT '',
                    icon            VARCHAR(50) DEFAULT 'location_on',
                    color           VARCHAR(20) DEFAULT '#e11d48',
                    label_column    NVARCHAR(100) DEFAULT '',
                    columns_json    NVARCHAR(MAX) DEFAULT '[]',
                    data_json       NVARCHAR(MAX) DEFAULT '[]',
                    row_count       INT DEFAULT 0,
                    uploaded_at     DATETIME2 DEFAULT GETUTCDATE()
                );
                CREATE INDEX IX_uploads_upload_id ON analytics_uploads(upload_id);
                CREATE INDEX IX_uploads_uploaded_at ON analytics_uploads(uploaded_at);
                PRINT 'Tabel analytics_uploads aangemaakt';
            END
            -- Kolommen toevoegen als ze nog niet bestaan (bestaande tabellen)
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='analytics_uploads' AND COLUMN_NAME='name')
                ALTER TABLE analytics_uploads ADD name NVARCHAR(500) DEFAULT '';
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='analytics_uploads' AND COLUMN_NAME='description')
                ALTER TABLE analytics_uploads ADD description NVARCHAR(2000) DEFAULT '';
        """)
        # Nieuwe metadata kolommen toevoegen aan analytics_sessions
        new_cols = [
            # Performance & Web Vitals
            ("page_load_ms", "INT"),
            ("dns_ms", "INT"),
            ("tcp_ms", "INT"),
            ("ttfb_ms", "INT"),
            ("dom_interactive_ms", "INT"),
            ("resources_count", "INT"),
            ("transfer_size_kb", "INT"),
            # Netwerk uitbreiding
            ("downlink_mbps", "FLOAT"),
            ("rtt_ms", "INT"),
            ("save_data", "BIT DEFAULT 0"),
            ("online", "BIT DEFAULT 1"),
            # Browser & OS details
            ("browser_name", "VARCHAR(100) DEFAULT ''"),
            ("browser_version", "VARCHAR(50) DEFAULT ''"),
            ("os_name", "VARCHAR(100) DEFAULT ''"),
            ("os_version", "VARCHAR(50) DEFAULT ''"),
            ("dark_mode", "BIT DEFAULT 0"),
            ("reduced_motion", "BIT DEFAULT 0"),
            ("do_not_track", "BIT DEFAULT 0"),
            ("pdf_viewer", "BIT DEFAULT 0"),
            ("cookies_enabled", "BIT DEFAULT 1"),
            ("ad_blocker", "BIT DEFAULT 0"),
            # Device uitbreiding
            ("max_touch_points", "INT DEFAULT 0"),
            ("orientation", "VARCHAR(20) DEFAULT ''"),
            ("battery_level", "FLOAT"),
            ("battery_charging", "BIT"),
            # Storage & Capabilities
            ("storage_quota_mb", "INT"),
            ("storage_used_mb", "INT"),
            ("indexeddb_support", "BIT DEFAULT 1"),
            ("webgl_version", "VARCHAR(10) DEFAULT ''"),
            ("webgpu_support", "BIT DEFAULT 0"),
            ("wasm_support", "BIT DEFAULT 0"),
        ]
        cursor2 = conn.cursor()
        for col_name, col_type in new_cols:
            cursor2.execute(f"""
                IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                               WHERE TABLE_NAME='analytics_sessions' AND COLUMN_NAME='{col_name}')
                    ALTER TABLE analytics_sessions ADD {col_name} {col_type};
            """)
        conn.close()
    except Exception as e:
        print(f"  Tabel-check fout (niet kritiek): {e}")


def test_connection():
    """Test de database verbinding bij opstarten."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM analytics_sessions")
        count = cursor.fetchone()[0]
        conn.close()
        print(f"  Database OK: {count} sessies in analytics_sessions")
        return True
    except Exception as e:
        print(f"  Database FOUT: {e}")
        return False

# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    try:
        conn = get_conn()
        conn.close()
        return jsonify({'status': 'ok', 'database': DB_NAME, 'server': DB_SERVER})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/session', methods=['POST'])
def create_session():
    """Registreer een nieuwe bezoekersessie."""
    try:
        d = request.get_json(force=True)
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO analytics_sessions (
                visitor_id, session_id, ip_address, city, region, country, isp,
                ip_lat, ip_lng, user_agent, platform, language, languages,
                screen_w, screen_h, viewport_w, viewport_h, pixel_ratio,
                color_depth, connection_type, cores, memory_gb, touch_device,
                referrer, page_url, timezone, canvas_hash, gpu, visit_count,
                started_at,
                page_load_ms, dns_ms, tcp_ms, ttfb_ms, dom_interactive_ms,
                resources_count, transfer_size_kb,
                downlink_mbps, rtt_ms, save_data, online,
                browser_name, browser_version, os_name, os_version,
                dark_mode, reduced_motion, do_not_track, pdf_viewer,
                cookies_enabled, ad_blocker,
                max_touch_points, orientation, battery_level, battery_charging,
                storage_quota_mb, storage_used_mb, indexeddb_support,
                webgl_version, webgpu_support, wasm_support
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                      ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            d.get('visitor_id',''), d.get('session_id',''),
            d.get('ip_address',''), d.get('city',''), d.get('region',''),
            d.get('country',''), d.get('isp',''),
            d.get('ip_lat'), d.get('ip_lng'),
            (d.get('user_agent',''))[:500], d.get('platform',''),
            d.get('language',''), d.get('languages',''),
            d.get('screen_w'), d.get('screen_h'),
            d.get('viewport_w'), d.get('viewport_h'),
            d.get('pixel_ratio'), d.get('color_depth'),
            d.get('connection_type',''), d.get('cores'),
            d.get('memory_gb'), d.get('touch_device', False),
            (d.get('referrer',''))[:500], (d.get('page_url',''))[:500],
            d.get('timezone',''), d.get('canvas_hash',''),
            (d.get('gpu',''))[:200], d.get('visit_count', 1),
            d.get('started_at', datetime.datetime.utcnow().isoformat()),
            # Performance
            d.get('page_load_ms'), d.get('dns_ms'), d.get('tcp_ms'),
            d.get('ttfb_ms'), d.get('dom_interactive_ms'),
            d.get('resources_count'), d.get('transfer_size_kb'),
            # Netwerk
            d.get('downlink_mbps'), d.get('rtt_ms'),
            d.get('save_data', False), d.get('online', True),
            # Browser & OS
            (d.get('browser_name',''))[:100], (d.get('browser_version',''))[:50],
            (d.get('os_name',''))[:100], (d.get('os_version',''))[:50],
            d.get('dark_mode', False), d.get('reduced_motion', False),
            d.get('do_not_track', False), d.get('pdf_viewer', False),
            d.get('cookies_enabled', True), d.get('ad_blocker', False),
            # Device
            d.get('max_touch_points', 0), (d.get('orientation',''))[:20],
            d.get('battery_level'), d.get('battery_charging'),
            # Storage & Capabilities
            d.get('storage_quota_mb'), d.get('storage_used_mb'),
            d.get('indexeddb_support', True),
            (d.get('webgl_version',''))[:10],
            d.get('webgpu_support', False), d.get('wasm_support', False),
        ))
        conn.close()
        return jsonify({'status': 'ok'}), 201
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/event', methods=['POST'])
def create_event():
    """Registreer een event."""
    try:
        d = request.get_json(force=True)
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO analytics_events (
                visitor_id, session_id, event_type, event_detail,
                zoom, center_lat, center_lng, timestamp
            ) VALUES (?,?,?,?,?,?,?,?)
        """, (
            d.get('visitor_id',''), d.get('session_id',''),
            d.get('event_type',''), (d.get('event_detail',''))[:2000],
            d.get('zoom'), d.get('center_lat'), d.get('center_lng'),
            d.get('timestamp', datetime.datetime.utcnow().isoformat())
        ))
        conn.close()
        return jsonify({'status': 'ok'}), 201
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/events', methods=['POST'])
def create_events_batch():
    """Registreer meerdere events tegelijk (batch)."""
    try:
        events = request.get_json(force=True)
        if not isinstance(events, list):
            events = [events]
        conn = get_conn()
        cursor = conn.cursor()
        for d in events:
            cursor.execute("""
                INSERT INTO analytics_events (
                    visitor_id, session_id, event_type, event_detail,
                    zoom, center_lat, center_lng, timestamp
                ) VALUES (?,?,?,?,?,?,?,?)
            """, (
                d.get('visitor_id',''), d.get('session_id',''),
                d.get('event_type',''), (d.get('event_detail',''))[:2000],
                d.get('zoom'), d.get('center_lat'), d.get('center_lng'),
                d.get('timestamp', datetime.datetime.utcnow().isoformat())
            ))
        conn.close()
        return jsonify({'status': 'ok', 'count': len(events)}), 201
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/session-end', methods=['POST'])
def session_end():
    """Update sessie met eindtijd en duur."""
    try:
        d = request.get_json(force=True)
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE analytics_sessions
            SET ended_at = ?, duration_seconds = ?
            WHERE session_id = ?
        """, (
            d.get('ended_at', datetime.datetime.utcnow().isoformat()),
            d.get('duration_seconds', 0),
            d.get('session_id', '')
        ))
        conn.close()
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/email', methods=['POST'])
def save_email():
    """Sla een e-mailadres op."""
    try:
        d = request.get_json(force=True)
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO analytics_emails (
                visitor_id, session_id, email, session_duration_min, collected_at
            ) VALUES (?,?,?,?,?)
        """, (
            d.get('visitor_id',''), d.get('session_id',''),
            d.get('email',''), d.get('session_duration_min', 0),
            d.get('collected_at', datetime.datetime.utcnow().isoformat())
        ))
        conn.close()
        return jsonify({'status': 'ok'}), 201
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/feedback', methods=['POST'])
def save_feedback():
    """Sla feedback op."""
    try:
        d = request.get_json(force=True)
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO analytics_feedback (
                visitor_id, session_id, feedback_text, email,
                year, session_duration_min, submitted_at
            ) VALUES (?,?,?,?,?,?,?)
        """, (
            d.get('visitor_id',''), d.get('session_id',''),
            d.get('text',''), d.get('email',''),
            d.get('year'), d.get('session_duration_min', 0),
            d.get('submitted_at', datetime.datetime.utcnow().isoformat())
        ))
        conn.close()
        return jsonify({'status': 'ok'}), 201
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ---------------------------------------------------------------------------
# Admin Endpoints (alleen voor dashboard)
# ---------------------------------------------------------------------------

@app.route('/api/admin/sessions', methods=['GET'])
def get_sessions():
    """Haal sessies op (nieuwste eerst)."""
    try:
        limit = request.args.get('limit', 100, type=int)
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT TOP {limit} * FROM analytics_sessions ORDER BY started_at DESC")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        # Serialiseer datetime objecten
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime.datetime):
                    row[k] = v.isoformat()
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/events', methods=['GET'])
def get_events():
    """Haal events op (nieuwste eerst)."""
    try:
        limit = request.args.get('limit', 200, type=int)
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT TOP {limit} * FROM analytics_events ORDER BY timestamp DESC")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime.datetime):
                    row[k] = v.isoformat()
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/emails', methods=['GET'])
def get_emails():
    """Haal e-mails op."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM analytics_emails ORDER BY collected_at DESC")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime.datetime):
                    row[k] = v.isoformat()
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/stats', methods=['GET'])
def get_stats():
    """Samenvattende statistieken."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        stats = {}

        cursor.execute("SELECT COUNT(DISTINCT visitor_id) FROM analytics_sessions")
        stats['unique_visitors'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM analytics_sessions")
        stats['total_sessions'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM analytics_sessions WHERE CAST(started_at AS DATE) = CAST(GETDATE() AS DATE)")
        stats['today_sessions'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM analytics_emails")
        stats['total_emails'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM analytics_events")
        stats['total_events'] = cursor.fetchone()[0]

        cursor.execute("SELECT AVG(duration_seconds) FROM analytics_sessions WHERE duration_seconds IS NOT NULL")
        avg_dur = cursor.fetchone()[0]
        stats['avg_duration_seconds'] = int(avg_dur) if avg_dur else 0

        # Top landen
        cursor.execute("SELECT TOP 10 country, COUNT(*) as cnt FROM analytics_sessions WHERE country != '' GROUP BY country ORDER BY cnt DESC")
        stats['top_countries'] = [{'country': r[0], 'count': r[1]} for r in cursor.fetchall()]

        # Top steden
        cursor.execute("SELECT TOP 10 city, COUNT(*) as cnt FROM analytics_sessions WHERE city != '' GROUP BY city ORDER BY cnt DESC")
        stats['top_cities'] = [{'city': r[0], 'count': r[1]} for r in cursor.fetchall()]

        # Top browsers
        cursor.execute("""
            SELECT TOP 5
                CASE
                    WHEN user_agent LIKE '%Edg/%' THEN 'Edge'
                    WHEN user_agent LIKE '%OPR/%' OR user_agent LIKE '%Opera%' THEN 'Opera'
                    WHEN user_agent LIKE '%Firefox%' THEN 'Firefox'
                    WHEN user_agent LIKE '%Chrome%' THEN 'Chrome'
                    WHEN user_agent LIKE '%Safari%' THEN 'Safari'
                    ELSE 'Overig'
                END as browser,
                COUNT(*) as cnt
            FROM analytics_sessions
            GROUP BY
                CASE
                    WHEN user_agent LIKE '%Edg/%' THEN 'Edge'
                    WHEN user_agent LIKE '%OPR/%' OR user_agent LIKE '%Opera%' THEN 'Opera'
                    WHEN user_agent LIKE '%Firefox%' THEN 'Firefox'
                    WHEN user_agent LIKE '%Chrome%' THEN 'Chrome'
                    WHEN user_agent LIKE '%Safari%' THEN 'Safari'
                    ELSE 'Overig'
                END
            ORDER BY cnt DESC
        """)
        stats['top_browsers'] = [{'browser': r[0], 'count': r[1]} for r in cursor.fetchall()]

        # Sessies per dag (laatste 30 dagen)
        cursor.execute("""
            SELECT CAST(started_at AS DATE) as dag, COUNT(*) as cnt
            FROM analytics_sessions
            WHERE started_at >= DATEADD(day, -30, GETDATE())
            GROUP BY CAST(started_at AS DATE)
            ORDER BY dag DESC
        """)
        stats['daily'] = [{'date': r[0].isoformat(), 'count': r[1]} for r in cursor.fetchall()]

        # Nieuwe metadata statistieken (alleen als kolommen bestaan)
        try:
            # Top OS
            cursor.execute("""
                SELECT TOP 5 os_name, COUNT(*) as cnt FROM analytics_sessions
                WHERE os_name IS NOT NULL AND os_name != ''
                GROUP BY os_name ORDER BY cnt DESC
            """)
            stats['top_os'] = [{'os': r[0], 'count': r[1]} for r in cursor.fetchall()]

            # Top browser (nieuwe kolom)
            cursor.execute("""
                SELECT TOP 5 browser_name, COUNT(*) as cnt FROM analytics_sessions
                WHERE browser_name IS NOT NULL AND browser_name != ''
                GROUP BY browser_name ORDER BY cnt DESC
            """)
            stats['top_browsers_parsed'] = [{'browser': r[0], 'count': r[1]} for r in cursor.fetchall()]

            # Performance gemiddelden
            cursor.execute("""
                SELECT AVG(page_load_ms), AVG(ttfb_ms), AVG(dom_interactive_ms),
                       AVG(transfer_size_kb), AVG(resources_count)
                FROM analytics_sessions
                WHERE page_load_ms IS NOT NULL
            """)
            perf = cursor.fetchone()
            stats['perf_avg'] = {
                'page_load_ms': int(perf[0]) if perf[0] else None,
                'ttfb_ms': int(perf[1]) if perf[1] else None,
                'dom_interactive_ms': int(perf[2]) if perf[2] else None,
                'transfer_size_kb': int(perf[3]) if perf[3] else None,
                'resources_count': int(perf[4]) if perf[4] else None,
            }

            # Dark mode percentage
            cursor.execute("SELECT COUNT(*) FROM analytics_sessions WHERE dark_mode = 1")
            dm = cursor.fetchone()[0]
            stats['dark_mode_pct'] = round(dm / max(stats['total_sessions'], 1) * 100)

            # Touch vs non-touch
            cursor.execute("SELECT COUNT(*) FROM analytics_sessions WHERE touch_device = 1")
            touch = cursor.fetchone()[0]
            stats['touch_pct'] = round(touch / max(stats['total_sessions'], 1) * 100)

            # Ad blocker percentage
            cursor.execute("SELECT COUNT(*) FROM analytics_sessions WHERE ad_blocker = 1")
            ab = cursor.fetchone()[0]
            stats['ad_blocker_pct'] = round(ab / max(stats['total_sessions'], 1) * 100)

            # Uploads count
            cursor.execute("SELECT COUNT(*) FROM analytics_uploads")
            stats['total_uploads'] = cursor.fetchone()[0]

        except Exception:
            pass  # Kolommen bestaan nog niet, geen probleem

        conn.close()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/feedback', methods=['GET'])
def get_feedback():
    """Haal feedback op."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM analytics_feedback ORDER BY submitted_at DESC")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime.datetime):
                    row[k] = v.isoformat()
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# Upload Endpoints (gebruikers data-uploads)
# ---------------------------------------------------------------------------

@app.route('/api/upload', methods=['POST'])
def save_upload():
    """Sla een gebruikers-upload op (metadata + data als JSON + bestand)."""
    try:
        d = request.get_json(force=True)
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO analytics_uploads (
                upload_id, visitor_id, session_id, name, description,
                filename, icon, color, label_column, columns_json,
                data_json, row_count, uploaded_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            d.get('upload_id', ''), d.get('visitor_id', ''),
            d.get('session_id', ''), d.get('name', ''),
            d.get('description', ''), d.get('filename', ''),
            d.get('icon', 'location_on'), d.get('color', '#e11d48'),
            d.get('label_column', ''), d.get('columns_json', '[]'),
            d.get('data_json', '[]'), d.get('row_count', 0),
            d.get('uploaded_at', datetime.datetime.utcnow().isoformat())
        ))
        conn.close()

        # Ook als bestand opslaan voor persistentie
        visitor_id = d.get('visitor_id', 'unknown')
        upload_id = d.get('upload_id', 'unknown')
        upload_dir = os.path.join(os.path.dirname(__file__), 'uploads', visitor_id)
        os.makedirs(upload_dir, exist_ok=True)
        filepath = os.path.join(upload_dir, f'{upload_id}.json')
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(d, f, ensure_ascii=False)

        return jsonify({'status': 'ok'}), 201
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/uploads/<visitor_id>', methods=['GET'])
def get_visitor_uploads(visitor_id):
    """Haal alle uploads op voor een specifieke bezoeker."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT upload_id, name, description, filename, icon, color,
                   label_column, columns_json, data_json, row_count, uploaded_at
            FROM analytics_uploads
            WHERE visitor_id = ?
            ORDER BY uploaded_at DESC
        """, (visitor_id,))
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime.datetime):
                    row[k] = v.isoformat()
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/uploads', methods=['GET'])
def get_uploads():
    """Haal alle uploads op (admin)."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM analytics_uploads ORDER BY uploaded_at DESC")
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime.datetime):
                    row[k] = v.isoformat()
        return jsonify(rows)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/upload/<upload_id>', methods=['DELETE'])
def delete_upload(upload_id):
    """Verwijder een upload (admin)."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM analytics_uploads WHERE upload_id = ?", (upload_id,))
        conn.close()
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    print("=" * 50)
    print("  GeoInzicht Analytics API")
    print("=" * 50)
    print(f"  Database: {DB_SERVER}/{DB_NAME}")
    test_connection()
    ensure_tables()
    print(f"  API draait op http://localhost:5000")
    print(f"  Admin dashboard: voeg ?admin toe aan GeoInzicht URL")
    print("=" * 50)
    # BEVEILIGINGSTIP: host='127.0.0.1' = alleen lokaal bereikbaar
    # Gebruik '0.0.0.0' alleen als je de API van buitenaf wilt bereiken
    app.run(host='127.0.0.1', port=5000, debug=True)
