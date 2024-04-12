from flask import Flask, request, jsonify
import psycopg2
import psycopg2.pool
import logging
from flask_cors import CORS
from config import DB_CREDENTIALS

app = Flask(__name__)
CORS(app)  # Enable CORS on the Flask app
logging.basicConfig(level=logging.INFO)

# Set up a connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 30,  # minconn, maxconn
                                                     host=DB_CREDENTIALS['host'],
                                                     port=DB_CREDENTIALS['port'],
                                                     dbname=DB_CREDENTIALS['database'],
                                                     user=DB_CREDENTIALS['user'],
                                                     password=DB_CREDENTIALS['password'],
                                                     sslmode=DB_CREDENTIALS['sslmode'])

@app.route('/track', methods=['POST'])
def track_event():
    event_data = request.get_json()
    if not event_data:
        return jsonify({"error": "Invalid data"}), 400

    conn = connection_pool.getconn()
    if conn is None:
        logging.error("Failed to connect to the database")
        return jsonify({"error": "Database connection error"}), 500
    
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO app_click_events (event_type, event_name, user_id, utm_source, utm_medium, utm_campaign, utm_content, time_of_event, event_url, app)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            event_data.get('eventType'),
            event_data.get('eventName'),
            event_data.get('userId'),
            event_data.get('utmSource'),
            event_data.get('utmMedium'),
            event_data.get('utmCampaign'),
            event_data.get('utmContent'),
            event_data.get('timeOfEvent'),
            event_data.get('eventUrl'),
            event_data.get('appName')
        ))
        conn.commit()
        logging.info("Event tracked successfully")
    except Exception as e:
        logging.error(f"Failed to insert event data: {e}")
        conn.rollback()
        return jsonify({"error": "Failed to insert event data"}), 500
    finally:
        cur.close()
        connection_pool.putconn(conn)
    
    return jsonify({"success": "Event tracked successfully"}), 200

@app.route('/pageview', methods=['POST'])
def track_pageview():
    pageview_data = request.get_json()
    if not pageview_data:
        return jsonify({"error": "Invalid data"}), 400

    conn = connection_pool.getconn()
    if conn is None:
        logging.error("Failed to connect to the database")
        return jsonify({"error": "Database connection error"}), 500

    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO app_pageviews_tracking (event_type, event_name, user_id, utm_source, utm_medium, utm_campaign, utm_content, time_of_event, event_url, app)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            pageview_data.get('eventType'),
            pageview_data.get('eventName'),
            pageview_data.get('userId'),
            pageview_data.get('utmSource'),
            pageview_data.get('utmMedium'),
            pageview_data.get('utmCampaign'),
            pageview_data.get('utmContent'),
            pageview_data.get('timeOfEvent'),
            pageview_data.get('eventUrl'),
            pageview_data.get('appName')
        ))
        conn.commit()
        logging.info("Pageview tracked successfully")
    except Exception as e:
        logging.error(f"Failed to insert pageview data: {e}")
        conn.rollback()
        return jsonify({"error": "Failed to insert pageview data"}), 500
    finally:
        cur.close()
        connection_pool.putconn(conn)

    return jsonify({"success": "Pageview tracked successfully"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)