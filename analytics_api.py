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
                started_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
            d.get('started_at', datetime.datetime.utcnow().isoformat())
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
# Start
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    print("=" * 50)
    print("  GeoInzicht Analytics API")
    print("=" * 50)
    print(f"  Database: {DB_SERVER}/{DB_NAME}")
    test_connection()
    print(f"  API draait op http://localhost:5000")
    print(f"  Admin dashboard: voeg ?admin toe aan GeoInzicht URL")
    print("=" * 50)
    app.run(host='0.0.0.0', port=5000, debug=True)
