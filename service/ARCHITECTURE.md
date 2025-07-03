# Monarch Service Architecture

This document outlines the architectural patterns and strategies used in the Monarch service, which is responsible for migrating GitHub issues from source repositories to target repositories.

## Table of Contents

- [Monarch Service Architecture](#monarch-service-architecture)
  - [Table of Contents](#table-of-contents)
  - [Modular Directory Structure](#modular-directory-structure)
  - [Settings Management](#settings-management)
  - [Circuit Breaker Pattern](#circuit-breaker-pattern)
  - [Heartbeat and Watchdog Mechanism](#heartbeat-and-watchdog-mechanism)
  - [Metrics Publishing](#metrics-publishing)
  - [Logging Strategy](#logging-strategy)
  - [Valkey as Message Broker and Log Storage](#valkey-as-message-broker-and-log-storage)
  - [Resilient Connections](#resilient-connections)
  - [Graceful Shutdown](#graceful-shutdown)
  - [Asynchronous Processing](#asynchronous-processing)
  - [Error Handling and Retries](#error-handling-and-retries)
  - [Sliding Window Pattern](#sliding-window-pattern)
    - [Polling Optimization](#polling-optimization)
  - [Conclusion](#conclusion)

## Modular Directory Structure

The Monarch service uses a modular directory structure to organize code by functionality, improving maintainability and separation of concerns:

```
service/
├── core/                 # Core service functionality
│   ├── __init__.py
│   └── service.py        # Main service class
├── config/               # Configuration settings
│   ├── __init__.py
│   └── settings.py       # Settings class
├── models/               # Data models
│   ├── __init__.py
│   └── models.py         # Pydantic models
├── integrations/         # External service integrations
│   ├── __init__.py
│   ├── github.py         # GitHub API integration
│   ├── github_request.py # GitHub API request handling
│   └── slack.py          # Slack API integration
├── messaging/            # Message handling and pub/sub
│   ├── __init__.py
│   ├── handler.py        # Message processing
│   └── resilient_pubsub.py # Resilient PubSub implementation
├── migration/            # Migration processing
│   ├── __init__.py
│   ├── processor.py      # Migration processing logic
│   ├── state_manager.py  # Migration state persistence
│   └── sliding_window_controller.py # Sliding window pattern
├── monitoring/           # Monitoring and metrics
│   ├── __init__.py
│   ├── monitor.py        # Monitoring functionality
│   ├── metrics.py        # Prometheus metrics
│   └── service_watchdog.py # Service health monitoring
├── notifications/        # Notification handling
│   ├── __init__.py
│   └── notifier.py       # Notification sending logic
├── persistence/          # Data persistence
│   ├── __init__.py
│   └── resilient_valkey.py # Resilient Valkey client
├── resilience/           # Resilience patterns
│   ├── __init__.py
│   └── circuit_breaker.py # Circuit breaker pattern
├── logging/              # Logging functionality
│   ├── __init__.py
│   ├── valkey_log_handler.py # Valkey log handler
│   ├── log_retriever.py  # Log retrieval functionality
│   └── web_interface.py  # Web interface for logs
└── templates/            # HTML and markdown templates
    ├── index.html        # HTML template for log web interface
    └── issue.md          # Issue template
```

This modular structure provides several benefits:

- **Clear Separation of Concerns**: Each directory focuses on a specific aspect of the system
- **Improved Code Organization**: Related functionality is grouped together
- **Better Maintainability**: Changes to one component are less likely to affect others
- **Easier Navigation**: Developers can quickly find relevant code
- **More Intuitive Dependencies**: Dependencies between components are more explicit

The main entry point (`main.py`) remains in the root directory and imports the core service from the appropriate module.

## Settings Management

The Monarch service uses Pydantic's `BaseModel` for configuration management, providing:

- **Type validation** for configuration values
- **Environment variable integration** with fallback defaults
- **Centralized configuration** in a single `Settings` class
- **Environment file support** via `.env` files

```python
class Settings(BaseModel):
    VALKEY_HOST: str = os.getenv("VALKEY_HOST", "localhost")
    VALKEY_PORT: int = 6379
    VALKEY_DB: int = 0
    GITHUB_API_URL: HttpUrl = "https://api.github.com"
    GITHUB_TOKEN: str = os.getenv("GH_PAT")
    # ... other settings

    class Config:
        env_file = ".env"
```

This approach ensures type safety, simplifies configuration management, and provides a clear interface for service settings.

## Circuit Breaker Pattern

The Monarch service implements the Circuit Breaker pattern to prevent cascading failures when external services (like GitHub API) become unavailable:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   CLOSED    │────▶│   HALF-OPEN │────▶│    OPEN     │
│  (Normal)   │     │  (Testing)  │     │ (Fast Fail) │
└─────────────┘     └─────────────┘     └─────────────┘
       ▲                   │                   │
       └───────────────────┘                   │
                ▲                              │
                └──────────────────────────────┘
```

Key features:
- **State Management**: Tracks circuit state (CLOSED, OPEN, HALF-OPEN)
- **Failure Counting**: Monitors consecutive failures
- **Automatic Recovery**: Transitions to HALF-OPEN after a timeout period
- **Thread Safety**: Uses locks to ensure thread-safe state transitions
- **Metrics Integration**: Reports circuit state to Prometheus metrics

The circuit breaker is used to protect GitHub API calls:

```python
self.github_circuit = CircuitBreaker("github_api")
# ...
try:
    return self.github_circuit.execute(func, *args, **kwargs)
except CircuitBreakerOpenError as e:
    logger.error("GitHub API circuit breaker open", error=str(e))
    self.metrics.circuit_breaker_state.labels(circuit_name="github_api").set(1)
    raise Exception(f"GitHub API unavailable: {str(e)}")
```

This pattern improves system resilience by failing fast when external dependencies are unavailable, preventing resource exhaustion and allowing for graceful degradation.

## Heartbeat and Watchdog Mechanism

The Monarch service implements a watchdog pattern to monitor service health and automatically recover from failures:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Heartbeat  │────▶│  Watchdog   │────▶│  Recovery   │
│  Generator  │     │   Monitor   │     │   Actions   │
└─────────────┘     └─────────────┘     └─────────────┘
```

Key components:
- **Heartbeat Generation**: Regular signals indicating service health
- **Health Monitoring**: Watchdog thread that checks for missed heartbeats
- **Recovery Actions**: Callbacks triggered when health issues are detected
- **External Visibility**: Heartbeats stored in Valkey for external monitoring

Implementation:
```python
# Initialize watchdog
self.watchdog = ServiceWatchdog(check_interval=30, max_missed_checks=3)
self.watchdog.register_valkey_client(self.valkey_client)

# Register recovery callback
def recovery_callback():
    # Reinitialize pubsub connection
    # ...
self.watchdog.register_recovery_callback(recovery_callback)

# Start sending heartbeats
def heartbeat_task():
    while True:
        try:
            self.watchdog.heartbeat()
            time.sleep(30)  # Send heartbeat every 30 seconds
        except Exception as e:
            logger.error("Error in heartbeat task", error=str(e))
            time.sleep(5)
```

This pattern ensures the service can detect and recover from various failure modes, improving overall reliability.

## Metrics Publishing

The Monarch service uses Prometheus for metrics collection and exposure:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Service   │────▶│  Prometheus │────▶│   Metrics   │
│    Code     │     │   Client    │     │   Server    │
└─────────────┘     └─────────────┘     └─────────────┘
```

Key metrics categories:
- **Operational Metrics**: Issues migrated, errors encountered
- **External Dependencies**: GitHub API rate limits
- **Connection Status**: Valkey connection health
- **Performance Metrics**: Message processing time
- **Circuit Breaker State**: Circuit state and failure counts
- **Watchdog Metrics**: Missed heartbeats and recovery attempts

Implementation:
```python
# Initialize metrics
self.metrics = Metrics()

# Start Prometheus metrics server
start_http_server(self.settings.PROMETHEUS_PORT)

# Use metrics in code
self.metrics.issues_migrated.labels(
    source_repo=self.current_source,
    target_repo=target_repo
).inc()
```

The metrics are exposed on port 8080 and can be scraped by Prometheus for monitoring and alerting.

## Logging Strategy

The Monarch service implements a sophisticated logging strategy using structured logging and persistent storage:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Structlog  │────▶│ Valkey Log  │────▶│   Valkey    │
│  Generator  │     │   Handler   │     │   Storage   │
└─────────────┘     └─────────────┘     └─────────────┘
                                              │
                                              ▼
                                         ┌─────────────┐
                                         │    Web      │
                                         │  Interface  │
                                         └─────────────┘
```

Key features:
- **Structured Logging**: JSON-formatted logs with consistent fields
- **Persistent Storage**: Logs stored in Valkey Redis Streams
- **Automatic Rotation**: Daily log streams with 30-day retention
- **Indexing**: Efficient retrieval by timestamp, level, and service
- **Web Interface**: Browser-based log viewer and search

Implementation:
```python
# Configure structured logging with Valkey handler
valkey_log_client = valkey.Valkey(
    host=self.settings.VALKEY_HOST,
    port=self.settings.VALKEY_PORT,
    db=self.settings.VALKEY_DB,
    decode_responses=False,  # Keep as bytes for log handler
)
valkey_handler = ValkeyLogHandler(valkey_log_client)

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        valkey_handler,  # Add Valkey handler
        structlog.processors.JSONRenderer()
    ]
)
```

The web interface provides a user-friendly way to browse, filter, and search logs, accessible at http://localhost:8081/.

## Valkey as Message Broker and Log Storage

The Monarch service uses Valkey (Redis-compatible) for multiple purposes:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Message   │────▶│    Valkey   │────▶│  Subscriber │
│  Publisher  │     │    Broker   │     │   Service   │
└─────────────┘     └─────────────┘     └─────────────┘
                          │
                          ▼
                     ┌─────────────┐
                     │     Log     │
                     │   Storage   │
                     └─────────────┘
```

Key uses:
- **Message Broker**: PubSub for receiving migration requests
- **Log Storage**: Redis Streams for persistent logging
- **Health Monitoring**: Storing heartbeat and status information
- **Service Discovery**: Potential use for service registration

Implementation:
```python
# Initialize resilient Valkey client
self.valkey_client = ResilientValkeyClient(
    host=self.settings.VALKEY_HOST,
    port=self.settings.VALKEY_PORT,
    db=self.settings.VALKEY_DB
)

# Create and start the resilient PubSub
self.pubsub = ResilientPubSub(
    valkey_client=self.valkey_client,
    channel='channel_migrate_issue_tickets',
    message_handler=message_handler
)
pubsub_thread = self.pubsub.start()
```

This multi-purpose use of Valkey simplifies the infrastructure requirements while providing robust messaging and storage capabilities.

## Resilient Connections

The Monarch service implements resilient connection patterns for external dependencies:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Connection │────▶│ Auto-Retry  │────▶│Health Check │
│    Wrapper  │     │    Logic    │     │   Thread    │
└─────────────┘     └─────────────┘     └─────────────┘
```

Key features:
- **Automatic Reconnection**: Retry logic for connection failures
- **Health Checking**: Background threads monitoring connection health
- **Exponential Backoff**: Increasing delays between retry attempts
- **Connection Pooling**: Efficient connection management
- **Socket Options**: TCP keepalive for detecting stale connections

Implementation for Valkey:
```python
class ResilientValkeyClient:
    def __init__(self, host, port, db, max_retries=10, retry_interval=5):
        # ...
        self.client = self._connect()
        self._start_health_check()

    def _connect(self):
        for attempt in range(self.max_retries):
            try:
                client = valkey.Valkey(
                    # ... connection parameters
                    socket_keepalive=True,
                    socket_keepalive_options={
                        socket.TCP_KEEPINTVL: 30,
                        socket.TCP_KEEPCNT: 3
                    },
                    health_check_interval=15,
                    retry_on_timeout=True
                )
                # Test connection
                client.ping()
                return client
            except Exception as e:
                # ... retry logic
```

Similar patterns are used for PubSub connections, ensuring robust communication even in the face of network issues or service restarts.

## Graceful Shutdown

The Monarch service implements graceful shutdown handling:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Signal    │────▶│  Shutdown   │────▶│ Connection  │
│  Handlers   │     │   Handler   │     │   Cleanup   │
└─────────────┘     └─────────────┘     └─────────────┘
```

Key features:
- **Signal Handling**: Captures SIGTERM and SIGINT signals
- **Task Completion**: Waits for in-progress tasks to complete
- **Resource Cleanup**: Properly closes connections and releases resources
- **Logging**: Records shutdown process for diagnostics

Implementation:
```python
# Register signal handlers for graceful shutdown
signal.signal(signal.SIGTERM, self._graceful_shutdown)
signal.signal(signal.SIGINT, self._graceful_shutdown)

def _graceful_shutdown(self, signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info("Received shutdown signal, stopping service gracefully")

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
```

This pattern ensures that the service can be safely stopped without losing in-progress work or corrupting data.

## Asynchronous Processing

The Monarch service uses asynchronous processing patterns for handling messages:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Message   │────▶│Thread Pool  │────▶│  Async      │
│  Receiver   │     │  Executor   │     │  Functions  │
└─────────────┘     └─────────────┘     └─────────────┘
```

Key features:
- **Thread Pool**: Concurrent processing of messages
- **Non-blocking I/O**: Asynchronous HTTP requests
- **Message Queuing**: Buffering of incoming messages
- **Backpressure Handling**: Managing load during high traffic

Implementation:
```python
# Initialize thread pool
self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

# Process message with metrics
with self.metrics.message_processing_time.time():
    try:
        data = MigrationData.model_validate_json(message['data'])
        # Use executor to process message asynchronously
        self.executor.submit(self._run_async_process, data)
    except Exception as e:
        self.metrics.message_processing_errors.inc()
        logger.error("Error processing message", error=str(e))
```

This pattern improves throughput and responsiveness, allowing the service to handle multiple migration requests concurrently.

## Error Handling and Retries

The Monarch service implements comprehensive error handling and retry strategies:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Error     │────▶│   Retry     │────▶│  Fallback   │
│  Detection  │     │  Strategy   │     │   Actions   │
└─────────────┘     └─────────────┘     └─────────────┘
```

Key features:
- **Retry Decorators**: Using the tenacity library for automatic retries
- **Exponential Backoff**: Increasing delays between retry attempts
- **Circuit Breaking**: Preventing retries when services are down
- **Error Logging**: Comprehensive error reporting
- **Fallback Mechanisms**: Graceful degradation when operations fail

Implementation:
```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
async def migrate_single_issue(
    self,
    target_repo: str,
    issue: Issue
) -> None:
    # ... implementation with error handling
```

This pattern improves reliability by automatically recovering from transient failures and providing clear diagnostics for persistent issues.

## Sliding Window Pattern

The Monarch service implements a sliding window pattern for reliable and efficient GitHub issue migration:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Pending   │────▶│  In-Flight  │────▶│  Completed  │
│ Migrations  │     │ Migrations  │     │ Migrations  │
└─────────────┘     └─────────────┘     └─────────────┘
       │                  │                    │
       │                  │                    │
       ▼                  ▼                    ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Window    │────▶│ Concurrency │────▶│ Completion  │
│  Controller │     │   Control   │     │  Detection  │
└─────────────┘     └─────────────┘     └─────────────┘
```

Key features:

- **State Tracking**: Each migration has a state (pending, in-flight, completed, failed) persisted in Valkey
- **Controlled Concurrency**: Limits the number of concurrent migrations to prevent overwhelming the GitHub API
- **Dynamic Window Sizing**: TCP-like congestion control that adjusts window size based on success/failure rates
- **Completion Detection**: Monitors migration status and sends notifications when all migrations for a target repository are complete
- **Recovery Mechanism**: Handles in-progress migrations during service restarts
- **Adaptive Polling**: Dynamically adjusts polling frequency based on migration activity
- **Idle Mode**: Completely stops polling when no migrations are pending or in-flight

Implementation:

```python
# Initialize migration state manager and sliding window controller
self.migration_state_manager = MigrationStateManager(self.valkey_client)

self.window_controller = SlidingWindowController(
    state_manager=self.migration_state_manager,
    process_func=self._process_single_migration,
    initial_window_size=5,
    retry_interval=self.settings.GITHUB_RATE_LIMIT_PAUSE * 2,
    min_poll_interval=3.0,  # Start with 3 second polling interval
    max_poll_interval=10.0  # Maximum 10 second polling interval when system is idle
)

# Start the controller
self.window_controller.start()

# Add migrations to the window
for issue in issues_to_migrate:
    migration_state = MigrationState(
        sequence_id=self.migration_state_manager.get_next_sequence_id(),
        source_repo=data.source_repo,
        target_repo=target,
        issue_id=issue_id,
        issue_data=issue.model_dump(),
        status=MigrationStatus.PENDING
    )
    self.window_controller.add_migration(migration_state)
```

### Polling Optimization

The sliding window controller implements several optimizations to reduce the frequency of Valkey queries:

1. **Adaptive Polling**: Dynamically adjusts polling intervals (3-10 seconds) based on migration activity
2. **Conditional Checking**: Skips queries entirely when the processing window is full
3. **Idle Mode**: Completely stops polling when no migrations are pending or in-flight

The idle mode feature uses a condition variable to pause both the window processor and retry processor threads when the system is inactive, eliminating all unnecessary database queries. The controller wakes up immediately when a new migration is added, ensuring responsiveness while minimizing database load.

This pattern is particularly useful for operations that interact with rate-limited external APIs (like GitHub) or services with potential availability issues, providing resilience, flow control, and efficient resource utilization.

## Conclusion

The Monarch service architecture demonstrates several modern software design patterns that contribute to its reliability, scalability, and maintainability:

- **Resilience Patterns**: Circuit breakers, watchdogs, and retry mechanisms
- **Observability**: Comprehensive logging, metrics, and monitoring
- **Resource Management**: Connection pooling and graceful shutdown
- **Concurrency**: Asynchronous processing and thread safety
- **Configuration**: Type-safe settings with environment integration

These patterns work together to create a robust service capable of reliably migrating GitHub issues even in the face of network issues, service outages, or other operational challenges.