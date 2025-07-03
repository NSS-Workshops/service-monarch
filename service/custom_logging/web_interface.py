"""
Web interface for viewing logs stored in Valkey.
Provides a Flask web server with API endpoints and HTML interface.
"""
import time
from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
import structlog

# Configure logger
logger = structlog.get_logger()

# Create Flask app
app = Flask(__name__)

# Global variable to store the Valkey client
# This will be set when the app is initialized in the main service
valkey_client = None
log_retriever = None
service_start_time = time.time()  # Track when the service started

def init_app(client):
    """
    Initialize the Flask app with a Valkey client.

    Args:
        client: Valkey client instance
    """
    global valkey_client, log_retriever
    valkey_client = client
    # Import LogRetriever lazily to avoid circular imports
    from service.custom_logging import get_log_retriever
    LogRetriever = get_log_retriever()
    log_retriever = LogRetriever(valkey_client)
    app.config['service_start_time'] = service_start_time
    logger.info("Log web interface initialized")

@app.route('/')
def index():
    """Main page with log viewer"""
    return render_template('custom_logging/index.html')

@app.route('/health')
def health_check():
    """Basic health check endpoint"""
    try:
        # Check Valkey connection
        valkey_ping = False
        if valkey_client:
            valkey_ping = valkey_client.ping()

        # Get last message time if available
        last_message_time = 0
        message_freshness = float('inf')
        try:
            if valkey_client:
                last_message_time = float(valkey_client.get("monarch:last_message_time") or 0)
                if last_message_time > 0:
                    message_freshness = time.time() - last_message_time
        except Exception:
            pass

        # Determine status based on checks
        is_healthy = valkey_ping
        status = {
            "status": "healthy" if is_healthy else "degraded",
            "valkey_connected": bool(valkey_ping),
            "last_message_seconds_ago": message_freshness,
            "uptime_seconds": time.time() - app.config.get('service_start_time', time.time())
        }

        return jsonify(status), 200 if is_healthy else 503
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 503

@app.route('/api/logs')
def get_logs():
    """API endpoint to get logs with filtering"""
    if not log_retriever:
        return jsonify({"error": "Log retriever not initialized"}), 500

    try:
        # Get query parameters
        start_time = request.args.get('start_time', None)
        end_time = request.args.get('end_time', None)
        level = request.args.get('level', None)
        service = request.args.get('service', None)
        limit = int(request.args.get('limit', 100))

        # Convert time strings to timestamps if provided
        if start_time:
            start_time = datetime.fromisoformat(start_time).timestamp() * 1000
        else:
            # Default to 24 hours ago
            start_time = (datetime.now() - timedelta(days=1)).timestamp() * 1000

        if end_time:
            end_time = datetime.fromisoformat(end_time).timestamp() * 1000
        else:
            end_time = datetime.now().timestamp() * 1000

        # Retrieve logs based on filters
        if level:
            logs = log_retriever.get_logs_by_level(level, limit)
        elif service:
            logs = log_retriever.get_logs_by_service(service, limit)
        else:
            logs = log_retriever.get_logs_by_timerange(start_time, end_time, limit)

        return jsonify(logs)
    except Exception as e:
        logger.error("Error retrieving logs", error=str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/log-levels')
def get_log_levels():
    """Get available log levels"""
    if not log_retriever:
        return jsonify(["info", "error", "warning", "debug"])

    try:
        levels = log_retriever.get_available_log_levels()
        return jsonify(levels)
    except Exception as e:
        logger.error("Error retrieving log levels", error=str(e))
        return jsonify(["info", "error", "warning", "debug"])

@app.route('/api/services')
def get_services():
    """Get available services"""
    if not log_retriever:
        return jsonify(["monarch"])

    try:
        services = log_retriever.get_available_services()
        return jsonify(services)
    except Exception as e:
        logger.error("Error retrieving services", error=str(e))
        return jsonify(["monarch"])

def start_web_interface(host='0.0.0.0', port=8081):
    """
    Start the Flask web interface.

    Args:
        host: Host to bind to
        port: Port to bind to
    """
    if not valkey_client:
        logger.error("Cannot start web interface: Valkey client not initialized")
        return

    logger.info("Starting log web interface", host=host, port=port)
    app.run(host=host, port=port, threaded=True)

if __name__ == '__main__':
    # This is for standalone testing only
    # In production, the app should be initialized and started from the main service
    from service.config.settings import Settings
    import valkey

    settings = Settings()
    client = valkey.Valkey(
        host=settings.VALKEY_HOST,
        port=settings.VALKEY_PORT,
        db=settings.VALKEY_DB,
        decode_responses=False
    )

    init_app(client)
    start_web_interface()