"""
Service Watchdog for the Monarch service.
Monitors service health and triggers recovery actions when needed.
"""
import os
import time
import threading
import structlog

# Configure logger
logger = structlog.get_logger()

class ServiceWatchdog:
    """
    Monitors service health and triggers recovery actions when needed.
    """
    def __init__(self, check_interval=60, max_missed_checks=3):
        """
        Initialize the service watchdog.

        Args:
            check_interval: Seconds between health checks
            max_missed_checks: Number of missed checks before triggering recovery
        """
        self.check_interval = check_interval
        self.max_missed_checks = max_missed_checks
        self.last_heartbeat = time.time()
        self.missed_checks = 0
        self.recovery_callbacks = []
        self.valkey_client = None
        self._start_watchdog()

    def register_valkey_client(self, valkey_client):
        """
        Register a Valkey client for storing heartbeat information.

        Args:
            valkey_client: Valkey client instance
        """
        self.valkey_client = valkey_client

    def register_recovery_callback(self, callback):
        """
        Register a callback function to be called when recovery is triggered.

        Args:
            callback: Function to call for recovery
        """
        self.recovery_callbacks.append(callback)

    def heartbeat(self):
        """
        Update the heartbeat timestamp.
        """
        self.last_heartbeat = time.time()
        self.missed_checks = 0

        # Optionally write to Valkey for external monitoring
        if self.valkey_client:
            try:
                self.valkey_client.set("monarch:heartbeat", str(self.last_heartbeat))
            except Exception as e:
                logger.error("Failed to update heartbeat in Valkey", error=str(e))

    def _start_watchdog(self):
        """
        Start a background thread to periodically check service health.
        """
        def watchdog_task():
            while True:
                time.sleep(self.check_interval)
                self._check_health()

        thread = threading.Thread(target=watchdog_task, daemon=True)
        thread.start()

    def _check_health(self):
        """
        Check if the service is healthy based on heartbeat.
        """
        time_since_heartbeat = time.time() - self.last_heartbeat

        if time_since_heartbeat > self.check_interval:
            self.missed_checks += 1
            logger.warning("Missed heartbeat",
                          missed=self.missed_checks,
                          seconds_since=time_since_heartbeat)

            if self.missed_checks >= self.max_missed_checks:
                logger.critical("Watchdog triggered recovery")
                self._trigger_recovery()
        else:
            self.missed_checks = 0

    def _trigger_recovery(self):
        """
        Trigger recovery actions.
        """
        try:
            # Call all registered recovery callbacks
            for callback in self.recovery_callbacks:
                try:
                    callback()
                except Exception as e:
                    logger.error("Recovery callback failed", error=str(e))

            logger.info("Service components recovery attempted by watchdog")
            self.heartbeat()  # Reset heartbeat after recovery attempt

        except Exception as e:
            logger.critical("Failed to recover service components", error=str(e))
            # In extreme cases, exit and let the container restart policy handle it
            os._exit(1)