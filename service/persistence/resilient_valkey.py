"""
Resilient Valkey client for the Monarch service.
Provides automatic reconnection, health checks, and connection monitoring.
"""
import time
import socket
import threading
import structlog
import valkey

# Configure logger
logger = structlog.get_logger()

class ResilientValkeyClient:
    """
    A wrapper around the Valkey client that provides automatic reconnection,
    health checks, and connection monitoring.
    """
    def __init__(self, host, port, db, max_retries=10, retry_interval=5):
        """
        Initialize the resilient Valkey client.

        Args:
            host: Valkey host
            port: Valkey port
            db: Valkey database
            max_retries: Maximum number of connection attempts
            retry_interval: Seconds between connection attempts
        """
        self.host = host
        self.port = port
        self.db = db
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.client = self._connect()
        self.health_check_interval = 30  # seconds
        self._start_health_check()

    def _connect(self):
        """
        Connect to Valkey with retry logic.

        Returns:
            Valkey client instance
        """
        for attempt in range(self.max_retries):
            try:
                client = valkey.Valkey(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    socket_timeout=10,
                    socket_connect_timeout=10,
                    socket_keepalive=True,
                    socket_keepalive_options={
                        socket.TCP_KEEPINTVL: 30,
                        socket.TCP_KEEPCNT: 3
                    },
                    health_check_interval=15,
                    retry_on_timeout=True,
                    decode_responses=True
                )
                # Test connection
                client.ping()
                logger.info("Connected to Valkey", attempt=attempt+1)
                return client
            except Exception as e:
                logger.warning("Connection attempt failed",
                              attempt=attempt+1,
                              error=str(e))
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_interval)

        raise ConnectionError(f"Failed to connect to Valkey after {self.max_retries} attempts")

    def _start_health_check(self):
        """
        Start a background thread to periodically check the connection health.
        """
        def health_check_task():
            while True:
                try:
                    if not self._check_connection():
                        logger.warning("Reconnecting to Valkey")
                        self.client = self._connect()
                    time.sleep(self.health_check_interval)
                except Exception as e:
                    logger.error("Health check error", error=str(e))
                    time.sleep(self.retry_interval)

        thread = threading.Thread(target=health_check_task, daemon=True)
        thread.start()

    def _check_connection(self):
        """
        Check if the connection to Valkey is still alive.

        Returns:
            bool: True if connection is alive, False otherwise
        """
        try:
            return self.client.ping()
        except Exception:
            return False

    def __getattr__(self, name):
        """
        Delegate method calls to the underlying Valkey client.

        Args:
            name: Method name

        Returns:
            The method from the underlying client
        """
        return getattr(self.client, name)