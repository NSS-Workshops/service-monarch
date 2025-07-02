from prometheus_client import Counter, Gauge, Histogram

class Metrics:
    """Prometheus metrics for the Monarch service."""
    def __init__(self):
        # Existing metrics
        self.issues_migrated = Counter(
            'issues_migrated_total',
            'Number of issues migrated',
            ['source_repo', 'target_repo']
        )
        self.migration_errors = Counter(
            'migration_errors_total',
            'Number of migration errors',
            ['source_repo', 'target_repo']
        )
        self.github_rate_limit = Gauge(
            'github_rate_limit_remaining',
            'Remaining GitHub API rate limit'
        )

        # New metrics for connection monitoring
        self.valkey_connection_status = Gauge(
            'monarch_valkey_connection_status',
            'Valkey connection status (1=connected, 0=disconnected)'
        )

        # Message processing metrics
        self.message_processing_time = Histogram(
            'monarch_message_processing_seconds',
            'Time spent processing messages'
        )
        self.message_processing_errors = Counter(
            'monarch_message_processing_errors',
            'Number of errors during message processing'
        )
        self.last_message_received_timestamp = Gauge(
            'monarch_last_message_timestamp',
            'Timestamp of last received message'
        )

        # Reconnection metrics
        self.pubsub_reconnections = Counter(
            'monarch_pubsub_reconnections',
            'Number of pubsub reconnection attempts'
        )

        # Circuit breaker metrics
        self.circuit_breaker_state = Gauge(
            'monarch_circuit_breaker_state',
            'Circuit breaker state (0=closed, 1=open, 2=half-open)',
            ['circuit_name']
        )
        self.circuit_breaker_failures = Counter(
            'monarch_circuit_breaker_failures',
            'Number of failures tracked by circuit breakers',
            ['circuit_name']
        )

        # Watchdog metrics
        self.watchdog_missed_heartbeats = Counter(
            'monarch_watchdog_missed_heartbeats',
            'Number of missed heartbeats detected by watchdog'
        )
        self.watchdog_recovery_attempts = Counter(
            'monarch_watchdog_recovery_attempts',
            'Number of recovery attempts triggered by watchdog'
        )
