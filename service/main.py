""" Main service to migrate issues from one repository to multiple repositories """
import os
import sys
import time
import signal
import threading
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

    def format_issue(self, template_data: IssueTemplate) -> str:
        """ Format the issue using the provided template data. """
        template_file = os.path.join(self.settings.TEMPLATE_DIR, 'issue.md')
        with open(template_file, 'r', encoding='utf-8') as f:
            template = f.read()

        return template.format(**template_data.dict())

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
                template_data = IssueTemplate(
                    user_name=issue['user']['login'],
                    user_url=issue['user']['html_url'],
                    user_avatar=issue['user']['avatar_url'],
                    date=issue['created_at'],
                    url=issue['html_url'],
                    body=issue['body']
                )

                new_issue = Issue(
                    title=issue['title'],
                    body=self.format_issue(template_data)
                )
                issues_to_migrate.append(new_issue)

            # Migrate issues to each target repo
            for target in data.all_target_repositories:
                messages = []

                for issue in issues_to_migrate:
                    try:
                        await self.migrate_single_issue(target, issue)
                    except Exception as e:
                        messages.append(
                            f'Error creating issue {issue.title}. {str(e)}.'
                        )

                    # Pause between issues to prevent rate limiting
                    time.sleep(self.settings.GITHUB_RATE_LIMIT_PAUSE)

                # Send status message to Slack
                if messages:
                    await slack.send_message(
                        data.notification_channel,
                        '\n'.join(messages)
                    )
                else:
                    await slack.send_message(
                        data.notification_channel,
                        f'All issues migrated successfully to {target}.'
                    )

                # Pause between repositories
                time.sleep(self.settings.REPO_MIGRATION_PAUSE)

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

    async def run(self):
        """ Run the Monarch service with improved message handling. """

        # Start Prometheus metrics server
        start_http_server(self.settings.PROMETHEUS_PORT)

        # Start the web interface in a background thread
        web_thread = threading.Thread(
            target=log_web_interface.start_web_interface,
            kwargs={'port': 8081},
            daemon=True
        )
        web_thread.start()
        logger.info('Started log web interface', port=8081)

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