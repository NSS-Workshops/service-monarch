""" Main service to migrate issues from one repository to multiple repositories """
import os
import sys
import time
import signal
import threading
import asyncio
import concurrent.futures
from typing import List, Dict
import valkey
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential
from prometheus_client import start_http_server

from valkey_log_handler import ValkeyLogHandler
import log_web_interface
from resilient_valkey import ResilientValkeyClient
from resilient_pubsub import ResilientPubSub
from service_watchdog import ServiceWatchdog
from circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from migration_state_manager import MigrationStateManager
from sliding_window_controller import SlidingWindowController
from models import MigrationState, MigrationStatus

from config import Settings
from models import IssueTemplate, Issue, MigrationData
from github_request import GithubRequest
from slack import SlackAPI
from metrics import Metrics

# Logger instance will be configured after Valkey client is initialized
logger = structlog.get_logger()

class TicketMigrator:
    """ Service to migrate issues from one repository to multiple repositories """
    def __init__(self):
        self.current_source = None
        self.settings = Settings()
        self.github = GithubRequest()
        self.service_start_time = time.time()

        # Initialize resilient Valkey client
        self.valkey_client = ResilientValkeyClient(
            host=self.settings.VALKEY_HOST,
            port=self.settings.VALKEY_PORT,
            db=self.settings.VALKEY_DB
        )

        # Initialize metrics
        self.metrics = Metrics()

        # Configure structured logging with Valkey handler
        # Use regular Valkey client for logging since it needs decode_responses=False
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
                valkey_handler,  # Add Valkey handler (using __call__ method)
                structlog.processors.JSONRenderer()
            ]
        )

        # Initialize web interface
        log_web_interface.init_app(valkey_log_client)

        # Initialize watchdog
        self.watchdog = ServiceWatchdog(check_interval=30, max_missed_checks=3)
        self.watchdog.register_valkey_client(self.valkey_client)

        # Initialize circuit breakers
        self.github_circuit = CircuitBreaker("github_api")

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._graceful_shutdown)
        signal.signal(signal.SIGINT, self._graceful_shutdown)

        # Initialize pubsub and thread pool
        self.pubsub = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

        # Initialize migration state manager and sliding window controller
        self.migration_state_manager = MigrationStateManager(self.valkey_client)

        # Initialize sliding window controller with adaptive polling
        # This reduces the frequency of Valkey SMEMBERS calls by:
        # 1. Using a longer polling interval (3-10 seconds instead of 0.1 seconds)
        # 2. Dynamically adjusting the interval based on migration activity
        # 3. Skipping database queries entirely when the window is full
        # 4. Entering idle mode with zero polling when no migrations are pending or in-flight
        self.window_controller = SlidingWindowController(
            state_manager=self.migration_state_manager,
            process_func=self._process_single_migration,
            initial_window_size=5,
            retry_interval=self.settings.GITHUB_RATE_LIMIT_PAUSE * 2,
            min_poll_interval=3.0,  # Start with 3 second polling interval (30x less frequent than before)
            max_poll_interval=10.0   # Maximum 10 second polling interval when system is idle (100x less frequent)
            # Note: When completely idle (no pending or in-flight migrations), polling stops entirely
        )

        self.window_controller.start()
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

    def format_issue(self, template_data: IssueTemplate) -> str:
        """ Format the issue using the provided template data. """
        template_file = os.path.join(self.settings.TEMPLATE_DIR, 'issue.md')
        with open(template_file, 'r', encoding='utf-8') as f:
            template = f.read()

        return template.format(**template_data.model_dump())

    def _github_api_call(self, func, *args, **kwargs):
        """
        Wrap GitHub API calls with circuit breaker protection.

        Args:
            func: GitHub API function to call
            *args: Arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function

        Returns:
            The result of the function

        Raises:
            Exception: Any exception raised by the function
        """
        try:
            return self.github_circuit.execute(func, *args, **kwargs)
        except CircuitBreakerOpenError as e:
            logger.error("GitHub API circuit breaker open", error=str(e))
            # Update circuit breaker state metric
            self.metrics.circuit_breaker_state.labels(circuit_name="github_api").set(1)  # 1 = open
            raise Exception(f"GitHub API unavailable: {str(e)}")
        except Exception as e:
            # Update circuit breaker failures metric
            self.metrics.circuit_breaker_failures.labels(circuit_name="github_api").inc()
            raise e

    def _process_single_migration(self, migration_state: MigrationState) -> bool:
        """
        Process a single migration using the sliding window pattern.

        Args:
            migration_state: The migration state to process

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create Issue object from the stored data
            issue = Issue(**migration_state.issue_data)

            # Use the existing migrate_single_issue method
            # but catch exceptions to handle in this method
            try:
                # We need to run this synchronously in this context
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(
                    self.migrate_single_issue(
                        migration_state.target_repo,
                        issue
                    )
                )
                loop.close()

                # If we get here, migration was successful
                return True

            except CircuitBreakerOpenError:
                # Circuit is open, we'll retry later
                logger.warning(
                    "Circuit breaker open, will retry migration later",
                    sequence_id=migration_state.sequence_id,
                    issue_id=migration_state.issue_id
                )
                return False

            except Exception as e:
                logger.error(
                    "Error in migration",
                    error=str(e),
                    sequence_id=migration_state.sequence_id,
                    issue_id=migration_state.issue_id
                )
                return False

        except Exception as e:
            logger.error(
                "Error processing migration state",
                error=str(e),
                state=migration_state.model_dump()
            )
            return False


    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def migrate_single_issue(
        self,
        target_repo: str,
        issue: Issue
    ) -> None:
        """ Migrate a single issue to the target repository. """
        try:
            url = f'{self.settings.GITHUB_API_URL}/repos/{target_repo}/issues'

            # Use circuit breaker for GitHub API call
            response = self._github_api_call(
                self.github.post,
                url,
                issue.model_dump()
            )

            if response.status_code != 201:
                raise Exception(f'Failed to create issue. {response.text}')

            self.metrics.issues_migrated.labels(
                source_repo=self.current_source,
                target_repo=target_repo
            ).inc()

            # Update rate limit metric
            remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            self.metrics.github_rate_limit.set(remaining)

            logger.info(
                "issue.migrated",
                target_repo=target_repo,
                issue_title=issue.title
            )

        except Exception as e:
            self.metrics.migration_errors.labels(
                source_repo=self.current_source,
                target_repo=target_repo
            ).inc()

            logger.error(
                "issue.migration_failed",
                target_repo=target_repo,
                issue_title=issue.title,
                error=str(e)
            )
            raise

    async def get_source_issues(self, source_repo: str) -> List[Dict]:
        """ Get all open issues from the source repository. """
        issues = []
        page = 1

        while True:
            url = (f'{self.settings.GITHUB_API_URL}/repos/{source_repo}/issues'
                  f'?state=open&direction=asc&page={page}')

            response = self._github_api_call(self.github.get, url)
            new_issues = response.json()
            logger.info(
                "github.response",
                issues=new_issues,
                status=response.status_code
            )

            if not new_issues or response.status_code != 200:
                break

            issues.extend(new_issues)
            page += 1

            # Update rate limit metric
            remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            self.metrics.github_rate_limit.set(remaining)

        return issues

    async def migrate_tickets(self, data: MigrationData) -> None:
        """ Migrate tickets from the source repository to the target repositories. """
        slack = SlackAPI()
        self.current_source = data.source_repo

        try:
            # Get all issues from source repo
            source_issues = await self.get_source_issues(data.source_repo)

            if not source_issues:
                logger.info(
                    "migration.skipped",
                    source_repo=data.source_repo,
                    reason="No issues found in source repository"
                )
                return

            # Format the issues for migration
            issues_to_migrate = []
            for issue in source_issues:
                # Use the IssueTemplate to format the issue data
                template_data = IssueTemplate(
                    user_name=issue['user']['login'],
                    user_url=issue['user']['html_url'],
                    user_avatar=issue['user']['avatar_url'],
                    date=issue['created_at'],
                    url=issue['html_url'],
                    body=issue['body']
                )

                # Create a new Issue object
                new_issue = Issue(
                    title=issue['title'],
                    body=self.format_issue(template_data)
                )

                # Append the new issue to the list of issues to migrate
                issues_to_migrate.append(new_issue)

            # Migrate issues to each target repo using sliding window
            for target in data.all_target_repositories:
                issue_count = 0
                for issue in issues_to_migrate:
                    # Create a unique ID for this issue
                    issue_id = f"{issue.title}_{hash(issue.body)}"

                    # Create migration state so we can track its progress
                    migration_state = MigrationState(
                        sequence_id=self.migration_state_manager.get_next_sequence_id(),
                        source_repo=data.source_repo,
                        target_repo=target,
                        issue_id=issue_id,
                        issue_data=issue.model_dump(),
                        status=MigrationStatus.PENDING
                    )

                    # Add to sliding window controller
                    self.window_controller.add_migration(migration_state)
                    issue_count += 1

                    # No need to sleep between issues as the window controller
                    # will manage the rate of migrations

                # We still pause between repositories
                time.sleep(self.settings.REPO_MIGRATION_PAUSE)

                # Send initial status message to Slack
                await slack.send_message(
                    data.notification_channel,
                    f"Migration to {target} has been queued. You'll receive updates as issues are processed."
                )

                # Start the completion checker for this target repository
                self.start_completion_checker(
                    data.source_repo,
                    target,
                    data.notification_channel,
                    issue_count
                )

        except Exception as e:
            logger.error(
                "migration.failed",
                source_repo=data.source_repo,
                error=str(e)
            )
            await slack.send_message(
                data.notification_channel,
                f'Migration failed: {str(e)}'
            )
            raise

    async def check_migration_completion(self, source_repo: str, target_repo: str, notification_channel: str, total_issues: int):
        """
        Check if all migrations for a specific source/target pair are complete
        and send a notification if they are.
        """
        slack = SlackAPI()

        try:
            # Get counts by status for this specific source/target pair
            pending_key = f"monarch:migration:status:{MigrationStatus.PENDING.value}"
            in_flight_key = f"monarch:migration:status:{MigrationStatus.IN_FLIGHT.value}"
            completed_key = f"monarch:migration:status:{MigrationStatus.COMPLETED.value}"
            failed_key = f"monarch:migration:status:{MigrationStatus.FAILED.value}"

            # Filter keys by source/target and this specific migration batch
            # We need to be more specific to avoid counting migrations from previous runs
            prefix = f"monarch:migration:{source_repo}:{target_repo}:"

            # Get all keys for this migration batch
            all_keys = set()

            # Collect all keys from all status sets
            for status_key in [pending_key, in_flight_key, completed_key, failed_key]:
                keys = self.valkey_client.smembers(status_key)
                for key in keys:
                    if key.startswith(prefix):
                        all_keys.add(key)

            # Now count by status, but only for keys that belong to this batch
            # This ensures we don't double-count or include migrations from previous runs
            pending_count = 0
            in_flight_count = 0
            completed_count = 0
            failed_count = 0

            # For each key, check its current status
            for key in all_keys:
                data = self.valkey_client.get(key)
                if data:
                    migration = MigrationState.model_validate_json(data)
                    if migration.status == MigrationStatus.PENDING:
                        pending_count += 1
                    elif migration.status == MigrationStatus.IN_FLIGHT:
                        in_flight_count += 1
                    elif migration.status == MigrationStatus.COMPLETED:
                        completed_count += 1
                    elif migration.status == MigrationStatus.FAILED:
                        failed_count += 1

            # Log status periodically
            if (pending_count + in_flight_count + completed_count + failed_count) > 0:
                logger.info(
                    f"Migration status for {source_repo} -> {target_repo}: " +
                    f"Pending: {pending_count}, In-flight: {in_flight_count}, " +
                    f"Completed: {completed_count}, Failed: {failed_count}, " +
                    f"Total expected: {total_issues}"
                )

            # Check if all migrations are complete (either completed or failed)
            completion_condition = pending_count == 0 and in_flight_count == 0 and (completed_count + failed_count) >= total_issues

            if completion_condition:
                logger.info(
                    f"Migration completion detected for {source_repo} -> {target_repo}. " +
                    f"Completed: {completed_count}, Failed: {failed_count}, Total: {total_issues}"
                )

                # All migrations are complete, send final notification
                if failed_count == 0:
                    message = f"All {total_issues} issues have been successfully migrated to {target_repo}."
                    logger.info(f"Sending completion notification: {message}")

                    try:
                        await slack.send_message(notification_channel, message)
                        logger.info(f"Successfully sent completion notification to {notification_channel}")
                    except Exception as e:
                        logger.error(f"Failed to send completion notification: {str(e)}")
                else:
                    # Get failed migrations
                    failed_migrations = []
                    for key in all_keys:
                        data = self.valkey_client.get(key)
                        if data:
                            migration = MigrationState.model_validate_json(data)
                            if migration.status == MigrationStatus.FAILED:
                                failed_migrations.append(migration)

                    # Create error message
                    error_messages = []
                    for migration in failed_migrations:
                        error_messages.append(
                            f"Error creating issue {migration.issue_id}: {migration.error_message or 'Unknown error'}."
                        )

                    message = f"Migration to {target_repo} completed with {failed_count} errors:\n" + '\n'.join(error_messages)
                    logger.info(f"Sending completion notification with errors: {message}")

                    try:
                        await slack.send_message(notification_channel, message)
                        logger.info(f"Successfully sent completion notification to {notification_channel}")
                    except Exception as e:
                        logger.error(f"Failed to send completion notification: {str(e)}")

                # Return True to indicate completion was detected and notification sent
                return True

            # Not all migrations are complete yet
            return False

        except Exception as e:
            logger.error("Error checking migration completion", error=str(e))
            return False

    def start_completion_checker(self, source_repo: str, target_repo: str, notification_channel: str, total_issues: int, check_interval: int = 10):
        """Start a thread to check for migration completion"""

        logger.info(f"Starting completion checker for {source_repo} -> {target_repo} with {total_issues} issues")

        def checker_task():
            completion_detected = False
            check_count = 0
            max_checks = 600  # 10 minutes at 1 second intervals

            while not completion_detected and check_count < max_checks:
                try:
                    check_count += 1
                    if check_count % 10 == 0:  # Log every 10 checks to avoid log spam
                        logger.info(f"Checking completion status for {source_repo} -> {target_repo} (attempt {check_count}/{max_checks})")

                    # Create a new event loop for this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                    # Check for completion
                    try:
                        completion_detected = loop.run_until_complete(
                            self.check_migration_completion(
                                source_repo, target_repo, notification_channel, total_issues
                            )
                        )

                        if completion_detected:
                            logger.info(f"Completion detected for {source_repo} -> {target_repo} on attempt {check_count}")
                    except Exception as e:
                        logger.error(f"Error checking completion: {str(e)}")
                    finally:
                        loop.close()

                    if completion_detected:
                        logger.info(
                            f"Migration completion detected and notification sent for {source_repo} -> {target_repo}"
                        )
                        break

                except Exception as e:
                    logger.error(f"Error in completion checker task: {str(e)}")

                # Sleep for the check interval
                time.sleep(check_interval)

            # If we reached max checks without completion, send a timeout notification
            if not completion_detected and check_count >= max_checks:
                logger.warning(f"Completion checker timed out for {source_repo} -> {target_repo} after {check_count} attempts")

                try:
                    # Create a new event loop for this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                    # Send timeout notification
                    slack_api = SlackAPI()
                    loop.run_until_complete(
                        slack_api.send_message(
                            notification_channel,
                            f"Migration to {target_repo} may be incomplete. Completion checker timed out after {check_count} attempts."
                        )
                    )

                    loop.close()
                except Exception as e:
                    logger.error(f"Failed to send timeout notification: {str(e)}")

        # Start the checker thread
        thread = threading.Thread(target=checker_task, daemon=True)
        thread.start()

        return thread

    async def report_migration_status(self, channel: str, source_repo: str, target_repo: str):
        """Report on the status of migrations for a specific source/target pair"""
        slack = SlackAPI()

        try:
            # Get counts by status
            statuses = {}
            for status in MigrationStatus:
                status_key = f"monarch:migration:status:{status.value}"
                count = len(self.valkey_client.smembers(status_key))
                statuses[status.value] = count

            # Get recent failures
            failed = self.migration_state_manager.get_migrations_by_status(
                MigrationStatus.FAILED,
                limit=5
            )

            # Create status message
            message = f"*Migration Status Report*\n"
            message += f"Source: {source_repo}\n"
            message += f"Target: {target_repo}\n\n"

            message += f"*Status Counts:*\n"
            for status, count in statuses.items():
                message += f"• {status.capitalize()}: {count}\n"

            if failed:
                message += f"\n*Recent Failures:*\n"
                for f in failed:
                    message += f"• {f.issue_id}: {f.error_message or 'Unknown error'} (Attempts: {f.attempts})\n"

            # Send status message
            await slack.send_message(channel, message)

        except Exception as e:
            logger.error("Failed to report migration status", error=str(e))
            await slack.send_message(
                channel,
                f"Failed to generate migration status report: {str(e)}"
            )

    def start_status_reporting(self, channel: str, source_repo: str, target_repo: str, interval: int = 300):
        """Start periodic status reporting"""

        def report_task():
            while True:
                try:
                    # Create a new event loop for this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                    # Run the report method
                    loop.run_until_complete(
                        self.report_migration_status(channel, source_repo, target_repo)
                    )

                    loop.close()

                except Exception as e:
                    logger.error("Error in status reporting task", error=str(e))

                # Sleep for the interval
                time.sleep(interval)

        # Start the reporting thread
        thread = threading.Thread(target=report_task, daemon=True)
        thread.start()

        return thread


    async def run(self):
        """ Run the Monarch service with improved message handling. """

        # Start Prometheus metrics server
        start_http_server(self.settings.PROMETHEUS_PORT)

        # Recover in-progress migrations
        def recover_migrations():
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

        # Run recovery in a separate thread
        recovery_thread = threading.Thread(target=recover_migrations, daemon=True)
        recovery_thread.start()

        # Start the web interface in a background thread
        web_thread = threading.Thread(
            target=log_web_interface.start_web_interface,
            kwargs={'port': 8081},
            daemon=True
        )
        web_thread.start()
        logger.info('Started log web interface', port=8081)

        def recover_migrations():
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

        # Run recovery in a separate thread
        recovery_thread = threading.Thread(target=recover_migrations, daemon=True)
        recovery_thread.start()

        # Initialize the resilient PubSub
        def message_handler(message):
            if message['type'] == 'message':
                try:
                    # Update metrics
                    self.metrics.last_message_received_timestamp.set(time.time())
                    self.valkey_client.set("monarch:last_message_time", str(time.time()))

                    # Process message with metrics
                    with self.metrics.message_processing_time.time():
                        try:
                            data = MigrationData.model_validate_json(message['data'])
                            # Use executor to process message asynchronously
                            self.executor.submit(self._run_async_process, data)
                        except Exception as e:
                            self.metrics.message_processing_errors.inc()
                            logger.error("Error processing message", error=str(e))
                except Exception as e:
                    logger.error("Error in message handler", error=str(e))

        # Create and start the resilient PubSub
        self.pubsub = ResilientPubSub(
            valkey_client=self.valkey_client,
            channel='channel_migrate_issue_tickets',
            message_handler=message_handler
        )
        pubsub_thread = self.pubsub.start()
        logger.info('Started resilient PubSub handler')

        # Register watchdog recovery callback
        def recovery_callback():
            try:
                if self.pubsub:
                    self.pubsub.stop()

                # Reinitialize pubsub
                self.pubsub = ResilientPubSub(
                    valkey_client=self.valkey_client,
                    channel='channel_migrate_issue_tickets',
                    message_handler=message_handler
                )
                self.pubsub.start()
                logger.info("PubSub reinitialized by watchdog")
            except Exception as e:
                logger.error("Recovery callback failed", error=str(e))

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

        heartbeat_thread = threading.Thread(target=heartbeat_task, daemon=True)
        heartbeat_thread.start()

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

    def _run_async_process(self, data):
        """Run the async process_message function in a thread."""
        try:
            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            # Run the async function in this thread's event loop
            loop.run_until_complete(self._process_message(data))
            loop.close()
        except Exception as e:
            logger.error("Error in async thread execution", error=str(e))

    async def _process_message(self, data):
        """Process a message in a separate thread."""
        try:
            await self.migrate_tickets(data)
        except Exception as e:
            logger.error("Error processing message", error=str(e))

if __name__ == "__main__":
    import asyncio

    migrator = TicketMigrator()
    asyncio.run(migrator.run())