from flask import Flask, request, jsonify
import psycopg2
import logging
from flask_cors import CORS  # Import CORS
from config import DB_CREDENTIALS

app = Flask(__name__)
CORS(app)  # Enable CORS on the Flask app

def get_db_connection():
    return psycopg2.connect(
        host=DB_CREDENTIALS['host'],
        port=DB_CREDENTIALS['port'],
        dbname=DB_CREDENTIALS['database'],
        user=DB_CREDENTIALS['user'],
        password=DB_CREDENTIALS['password'],
        sslmode=DB_CREDENTIALS['sslmode']  
    )
@app.route('/track', methods=['POST'])

def track_event():
    event_data = request.json
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO tfx_listener (event_type, event_name, user_id, utm_source, utm_medium, utm_campaign, utm_content, time_of_event,event_url,app)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            event_data.get('eventType'),
            event_data.get('eventName'),
            event_data.get('userId'),
            event_data.get('utmSource'),
            event_data.get('utmMedium'),
            event_data.get('utmCampaign'),
            event_data.get('utmContent'),
            event_data.get('timeOfEvent'), # Ensure this is formatted correctly for PostgreSQL
            event_data.get('eventUrl'),
            "TFX"  
        ))
        logging.info("Event tracked successfully")
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
        logging.error(f"Failed to insert event data: {e}")
        return jsonify({"error": "Failed to insert event data"}), 500
    finally:
        cur.close()
        conn.close()
        logging.info("Event tracked successfully. Connection closed")
    return jsonify({"success": "Event tracked successfully"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000,debug=True)