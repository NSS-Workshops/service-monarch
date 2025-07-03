"""Monitoring functionality for the Monarch service"""
import time
import threading
import asyncio
import structlog
from models import MigrationStatus, MigrationState

logger = structlog.get_logger()

class Monitor:
    """Handles monitoring and completion checking"""
    def __init__(self, service, settings, valkey_client, migration_state_manager):
        self.service = service
        self.settings = settings
        self.valkey_client = valkey_client
        self.migration_state_manager = migration_state_manager

    async def check_migration_completion(self, source_repo, target_repo, notification_channel, total_issues):
        """
        Check if all migrations for a specific source/target pair are complete
        and send a notification if they are.
        """
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
                    await self.service.notifier.send_completion_notification(
                        notification_channel,
                        target_repo,
                        total_issues
                    )
                else:
                    # Get failed migrations
                    failed_migrations = []
                    for key in all_keys:
                        data = self.valkey_client.get(key)
                        if data:
                            migration = MigrationState.model_validate_json(data)
                            if migration.status == MigrationStatus.FAILED:
                                failed_migrations.append(migration)

                    await self.service.notifier.send_completion_notification(
                        notification_channel,
                        target_repo,
                        total_issues,
                        failed_count,
                        failed_migrations
                    )

                # Return True to indicate completion was detected and notification sent
                return True

            # Not all migrations are complete yet
            return False

        except Exception as e:
            logger.error("Error checking migration completion", error=str(e))
            return False

    def start_completion_checker(self, source_repo, target_repo, notification_channel, total_issues, check_interval=10):
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
                    loop.run_until_complete(
                        self.service.notifier.send_timeout_notification(
                            notification_channel,
                            target_repo,
                            check_count
                        )
                    )

                    loop.close()
                except Exception as e:
                    logger.error(f"Failed to send timeout notification: {str(e)}")

        # Start the checker thread
        thread = threading.Thread(target=checker_task, daemon=True)
        thread.start()

        return thread

    def start_status_reporting(self, channel, source_repo, target_repo, interval=300):
        """Start periodic status reporting"""
        def report_task():
            while True:
                try:
                    # Create a new event loop for this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                    # Run the report method
                    loop.run_until_complete(
                        self.service.notifier.report_migration_status(
                            channel,
                            source_repo,
                            target_repo,
                            self.valkey_client,
                            self.migration_state_manager
                        )
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

    def start_heartbeat(self, watchdog, interval=30):
        """Start sending heartbeats"""
        def heartbeat_task():
            while True:
                try:
                    watchdog.heartbeat()
                    time.sleep(interval)  # Send heartbeat every 30 seconds
                except Exception as e:
                    logger.error("Error in heartbeat task", error=str(e))
                    time.sleep(5)

        # Start the heartbeat thread
        thread = threading.Thread(target=heartbeat_task, daemon=True)
        thread.start()

        return thread
