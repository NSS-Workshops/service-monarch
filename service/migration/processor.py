"""Migration processing for the Monarch service"""
import os
import time
import asyncio
import structlog
from service.resilience.circuit_breaker import CircuitBreakerOpenError
from service.models.models import IssueTemplate, Issue, MigrationState, MigrationStatus, MigrationData

logger = structlog.get_logger()

class MigrationProcessor:
    """Handles issue migration processing"""
    def __init__(self, service, settings, window_controller, migration_state_manager):
        self.service = service
        self.settings = settings
        self.window_controller = window_controller
        self.migration_state_manager = migration_state_manager

    def format_issue(self, template_data: IssueTemplate) -> str:
        """ Format the issue using the provided template data. """
        template_file = os.path.join(self.settings.TEMPLATE_DIR, 'issue.md')
        with open(template_file, 'r', encoding='utf-8') as f:
            template = f.read()

        return template.format(**template_data.model_dump())

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
                    self.service.github.migrate_single_issue(
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

    async def migrate_tickets(self, data: MigrationData) -> None:
        """ Migrate tickets from the source repository to the target repositories. """
        self.service.github.current_source = data.source_repo

        try:
            # Get all issues from source repo
            source_issues = await self.service.github.get_source_issues(data.source_repo)

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
                await self.service.notifier.send_migration_queued_notification(
                    data.notification_channel,
                    target
                )

                # Start the completion checker for this target repository
                self.service.monitor.start_completion_checker(
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
            await self.service.notifier.send_error_notification(
                data.notification_channel,
                f'Migration failed: {str(e)}'
            )
            raise
