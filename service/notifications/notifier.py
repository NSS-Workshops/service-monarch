"""Notification handling for the Monarch service"""
import structlog
from models import MigrationStatus
from service.notifications.slack import SlackAPI

logger = structlog.get_logger()

class Notifier:
    """Handles notifications and status reporting"""
    def __init__(self, settings):
        self.settings = settings
        self.slack = SlackAPI()

    async def send_migration_queued_notification(self, channel, target_repo):
        """Send notification that migration has been queued"""
        message = f"Migration to {target_repo} has been queued. You'll receive updates as issues are processed."
        await self.slack.send_message(channel, message)
        logger.info(f"Sent migration queued notification for {target_repo}")

    async def send_completion_notification(self, channel, target_repo, total_issues, failed_count=0, failed_migrations=None):
        """Send notification that migration is complete"""
        if failed_count == 0:
            message = f"All {total_issues} issues have been successfully migrated to {target_repo}."
            logger.info(f"Sending completion notification: {message}")

            try:
                await self.slack.send_message(channel, message)
                logger.info(f"Successfully sent completion notification to {channel}")
            except Exception as e:
                logger.error(f"Failed to send completion notification: {str(e)}")
        else:
            # Create error message
            error_messages = []
            for migration in failed_migrations:
                error_messages.append(
                    f"Error creating issue {migration.issue_id}: {migration.error_message or 'Unknown error'}."
                )

            message = f"Migration to {target_repo} completed with {failed_count} errors:\n" + '\n'.join(error_messages)
            logger.info(f"Sending completion notification with errors: {message}")

            try:
                await self.slack.send_message(channel, message)
                logger.info(f"Successfully sent completion notification with errors to {channel}")
            except Exception as e:
                logger.error(f"Failed to send completion notification: {str(e)}")

    async def send_timeout_notification(self, channel, target_repo, check_count):
        """Send notification that migration completion check timed out"""
        message = f"Migration to {target_repo} may be incomplete. Completion checker timed out after {check_count} attempts."
        try:
            await self.slack.send_message(channel, message)
            logger.info(f"Sent timeout notification for {target_repo}")
        except Exception as e:
            logger.error(f"Failed to send timeout notification: {str(e)}")

    async def send_error_notification(self, channel, message):
        """Send error notification"""
        try:
            await self.slack.send_message(channel, message)
            logger.info(f"Sent error notification: {message}")
        except Exception as e:
            logger.error(f"Failed to send error notification: {str(e)}")

    async def report_migration_status(self, channel, source_repo, target_repo, valkey_client, migration_state_manager):
        """Report on the status of migrations for a specific source/target pair"""
        try:
            # Get counts by status
            statuses = {}
            for status in MigrationStatus:
                status_key = f"monarch:migration:status:{status.value}"
                count = len(valkey_client.smembers(status_key))
                statuses[status.value] = count

            # Get recent failures
            failed = migration_state_manager.get_migrations_by_status(
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
            await self.slack.send_message(channel, message)
            logger.info(f"Sent migration status report for {source_repo} -> {target_repo}")

        except Exception as e:
            logger.error("Failed to report migration status", error=str(e))
            await self.slack.send_message(
                channel,
                f"Failed to generate migration status report: {str(e)}"
            )
