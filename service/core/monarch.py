"""Core service module for the Monarch service"""
import sys
import time
import signal
import threading
import concurrent.futures
import structlog
import valkey
from prometheus_client import start_http_server

import service.custom_logging as custom_logging
from service.persistence.resilient_valkey import ResilientValkeyClient
from service.messaging.resilient_pubsub import ResilientPubSub
from service.monitoring.service_watchdog import ServiceWatchdog
from service.resilience.circuit_breaker import CircuitBreaker
from service.migration.state_manager import MigrationStateManager
from service.migration.sliding_window_controller import SlidingWindowController
from service.models.models import MigrationStatus
from service.config.settings import Settings
from service.monitoring.metrics import Metrics

# Import from new modules
from service.integrations.github import GitHubIntegration
from service.migration.processor import MigrationProcessor
from service.notifications.notifier import Notifier
from service.monitoring.monitor import Monitor
from service.messaging.handler import MessageHandler

logger = structlog.get_logger()

class TicketMigrator:
    """Core service to migrate issues from one repository to multiple repositories"""
    def __init__(self):
        self.current_source = None
        self.settings = Settings()
        self.service_start_time = time.time()

        # Initialize components
        self._initialize_valkey()
        self._initialize_logging()
        self._initialize_metrics()
        self._initialize_watchdog()
        self._initialize_circuit_breakers()
        self._initialize_thread_pool()
        self._initialize_migration_components()

        # Initialize modules
        self.github = GitHubIntegration(self.settings, self.metrics, self.github_circuit)
        self.migration_processor = MigrationProcessor(self, self.settings, self.window_controller, self.migration_state_manager)
        self.notifier = Notifier(self.settings)
        self.monitor = Monitor(self, self.settings, self.valkey_client, self.migration_state_manager)
        self.message_handler = MessageHandler(self, self.executor)

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._graceful_shutdown)
        signal.signal(signal.SIGINT, self._graceful_shutdown)

    def _initialize_valkey(self):
        """Initialize Valkey client"""
        # Initialize resilient Valkey client
        self.valkey_client = ResilientValkeyClient(
            host=self.settings.VALKEY_HOST,
            port=self.settings.VALKEY_PORT,
            db=self.settings.VALKEY_DB
        )

    def _initialize_logging(self):
        """Initialize logging"""
        # Configure structured logging with Valkey handler
        # Use regular Valkey client for logging since it needs decode_responses=False
        valkey_log_client = valkey.Valkey(
            host=self.settings.VALKEY_HOST,
            port=self.settings.VALKEY_PORT,
            db=self.settings.VALKEY_DB,
            decode_responses=False,  # Keep as bytes for log handler
        )
        # Get ValkeyLogHandler using the lazy-loading function
        ValkeyLogHandler = custom_logging.get_valkey_log_handler()
        valkey_handler = ValkeyLogHandler(valkey_log_client)

        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                valkey_handler,  # Add Valkey handler (using __call__ method)
                structlog.processors.JSONRenderer()
            ]
        )

        # Initialize web interface using lazy loading
        web_interface = custom_logging.get_web_interface()
        web_interface.init_app(valkey_log_client)

    def _initialize_metrics(self):
        """Initialize metrics"""
        # Initialize metrics
        self.metrics = Metrics()

    def _initialize_watchdog(self):
        """Initialize watchdog"""
        # Initialize watchdog
        self.watchdog = ServiceWatchdog(check_interval=30, max_missed_checks=3)
        self.watchdog.register_valkey_client(self.valkey_client)

    def _initialize_circuit_breakers(self):
        """Initialize circuit breakers"""
        # Initialize circuit breakers
        self.github_circuit = CircuitBreaker("github_api")

    def _initialize_thread_pool(self):
        """Initialize thread pool"""
        # Initialize pubsub and thread pool
        self.pubsub = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    def _initialize_migration_components(self):
        """Initialize migration components"""
        # Initialize migration state manager and sliding window controller
        self.migration_state_manager = MigrationStateManager(self.valkey_client)

        # Initialize sliding window controller with adaptive polling
        self.window_controller = SlidingWindowController(
            state_manager=self.migration_state_manager,
            process_func=self._process_single_migration_proxy,
            initial_window_size=5,
            retry_interval=self.settings.GITHUB_RATE_LIMIT_PAUSE * 2,
            min_poll_interval=3.0,  # Start with 3 second polling interval
            max_poll_interval=10.0   # Maximum 10 second polling interval when system is idle
        )

        self.window_controller.start()

    def _process_single_migration_proxy(self, migration_state):
        """Proxy method to call the migration processor's _process_single_migration method"""
        return self.migration_processor._process_single_migration(migration_state)

    def _graceful_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Received shutdown signal, stopping service gracefully")

        # Stop the window controller
        if hasattr(self, 'window_controller'):
            self.window_controller.stop()

        # Stop accepting new messages
        if self.pubsub:
            try:
                self.pubsub.stop()
            except Exception as e:
                logger.error("Error stopping pubsub", error=str(e))

        # Wait for in-progress tasks to complete
        logger.info("Shutting down thread pool")
        self.executor.shutdown(wait=True)

        # Close connections
        try:
            self.valkey_client.client.close()
        except Exception as e:
            logger.error("Error closing Valkey connection", error=str(e))

        logger.info("Service shutdown complete")
        sys.exit(0)

    async def run(self):
        """Run the Monarch service"""
        # Start Prometheus metrics server
        start_http_server(self.settings.PROMETHEUS_PORT)

        # Recover in-progress migrations
        recovery_thread = threading.Thread(
            target=self._recover_migrations,
            daemon=True
        )
        recovery_thread.start()

        # Start the web interface in a background thread
        web_interface = custom_logging.get_web_interface()
        web_thread = threading.Thread(
            target=web_interface.start_web_interface,
            kwargs={'port': 8081},
            daemon=True
        )
        web_thread.start()
        logger.info('Started log web interface', port=8081)

        # Initialize the resilient PubSub
        self.pubsub = ResilientPubSub(
            valkey_client=self.valkey_client,
            channel='channel_migrate_issue_tickets',
            message_handler=self.message_handler.handle_message
        )
        pubsub_thread = self.pubsub.start()
        logger.info('Started resilient PubSub handler')

        # Register watchdog recovery callback
        self.watchdog.register_recovery_callback(self._recovery_callback)

        # Start sending heartbeats
        heartbeat_thread = self.monitor.start_heartbeat(self.watchdog)

        # Update connection status metric
        self.metrics.valkey_connection_status.set(1)

        logger.info('Monarch service started with enhanced resilience')

        try:
            # Keep the main thread alive
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            self._graceful_shutdown(None, None)
        except Exception as e:
            logger.error("Fatal error", error=str(e))
            raise

    def _recover_migrations(self):
        """Recover in-progress migrations"""
        try:
            # Get in-flight migrations and reset to pending
            in_flight = self.migration_state_manager.get_migrations_by_status(
                MigrationStatus.IN_FLIGHT
            )

            if in_flight:
                logger.info(f"Recovering {len(in_flight)} in-flight migrations")

                for migration in in_flight:
                    migration.status = MigrationStatus.PENDING
                    self.migration_state_manager.save_migration_state(migration)

        except Exception as e:
            logger.error("Failed to recover migrations", error=str(e))

    def _recovery_callback(self):
        """Callback for watchdog recovery"""
        try:
            if self.pubsub:
                self.pubsub.stop()

            # Reinitialize pubsub
            self.pubsub = ResilientPubSub(
                valkey_client=self.valkey_client,
                channel='channel_migrate_issue_tickets',
                message_handler=self.message_handler.handle_message
            )
            self.pubsub.start()
            logger.info("PubSub reinitialized by watchdog")
        except Exception as e:
            logger.error("Recovery callback failed", error=str(e))