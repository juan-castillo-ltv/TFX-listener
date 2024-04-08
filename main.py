from flask import Flask, request, jsonify
import psycopg2
import os
from config import DB_CREDENTIALS
from apscheduler.schedulers.blocking import BlockingScheduler

app = Flask(__name__)
def get_db_connection():
    return psycopg2.connect(
        host=DB_CREDENTIALS['host'],
        port=DB_CREDENTIALS['port'],
        dbname=DB_CREDENTIALS['database'],
        user=DB_CREDENTIALS['user'],
        password=DB_CREDENTIALS['password'],
        sslmode=DB_CREDENTIALS['sslmode']  # Get password from an environment variable
    )
@app.route('/track', methods=['POST'])

def track_event():
    event_data = request.json
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO your_table_name (event_type, event_name, user_id, utm_source, utm_medium, utm_campaign, utm_content, time_of_event)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            event_data.get('eventType'),
            event_data.get('eventName'),
            event_data.get('userId'),
            event_data.get('utmSource'),
            event_data.get('utmMedium'),
            event_data.get('utmCampaign'),
            event_data.get('utmContent'),
            event_data.get('timeOfEvent')  # Ensure this is formatted correctly for PostgreSQL
        ))
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
        return jsonify({"error": "Failed to insert event data"}), 500
    finally:
        cur.close()
        conn.close()
    return jsonify({"success": "Event tracked successfully"}), 200
if __name__ == '__main__':
    app.run(debug=True)