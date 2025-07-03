"""Sliding Window Controller for issue migration"""

import time
import threading
from datetime import datetime, timedelta
from typing import Callable
import structlog

from service.models.models import MigrationState, MigrationStatus
from service.migration.state_manager import MigrationStateManager

logger = structlog.get_logger()

class SlidingWindowController:
    """
    Implements a sliding window pattern for reliable issue migration.

    Features:
    - Adaptive polling: Dynamically adjusts polling frequency based on migration activity
    - Conditional checking: Skips unnecessary database queries when the window is full
    - TCP-like congestion control: Adjusts window size based on success/failure rates
    """

    def __init__(self, state_manager: MigrationStateManager,
                 process_func: Callable[[MigrationState], bool],
                 initial_window_size: int = 5,
                 min_window_size: int = 1,
                 max_window_size: int = 20,
                 retry_interval: int = 60,
                 min_poll_interval: float = 3.0,
                 max_poll_interval: float = 10.0):
        """
        Initialize the sliding window controller

        Args:
            state_manager: The migration state manager
            process_func: Function to process a migration (returns True if successful)
            initial_window_size: Initial size of the sliding window
            min_window_size: Minimum window size
            max_window_size: Maximum window size
            retry_interval: Seconds to wait before retrying failed migrations
            min_poll_interval: Minimum seconds to wait between checks for pending migrations (default: 3.0)
            max_poll_interval: Maximum seconds to wait between checks when system is idle (default: 10.0)
        """
        self.state_manager = state_manager
        self.process_func = process_func
        self.window_size = initial_window_size
        self.min_window_size = min_window_size
        self.max_window_size = max_window_size
        self.retry_interval = retry_interval

        # Adaptive polling parameters
        self.min_poll_interval = min_poll_interval
        self.max_poll_interval = max_poll_interval
        self.current_poll_interval = min_poll_interval
        self.empty_checks_count = 0
        self.max_empty_checks = 5  # After this many empty checks, increase poll interval

        self.in_flight = {}  # sequence_id -> MigrationState
        self.lock = threading.RLock()
        self.running = False
        self.window_thread = None
        self.retry_thread = None

        # Idle mode parameters
        self.idle_mode = False
        self.idle_check_interval = 30  # Seconds to wait between checks when in idle mode
        self.idle_condition = threading.Condition(self.lock)  # For signaling when to wake up from idle

    def start(self):
        """Start the sliding window controller"""
        with self.lock:
            if self.running:
                return

            self.running = True

            # Start the window processing thread
            self.window_thread = threading.Thread(
                target=self._window_processor,
                daemon=True
            )
            self.window_thread.start()

            # Start the retry thread
            self.retry_thread = threading.Thread(
                target=self._retry_processor,
                daemon=True
            )
            self.retry_thread.start()

            logger.info("Sliding window controller started",
                       window_size=self.window_size,
                       min_poll_interval=self.min_poll_interval,
                       max_poll_interval=self.max_poll_interval)

    def stop(self):
        """Stop the sliding window controller"""
        with self.lock:
            if not self.running:
                return

            self.running = False
            logger.info("Sliding window controller stopping")

    def add_migration(self, state: MigrationState) -> bool:
        """Add a migration to be processed"""
        with self.lock:
            # Save the initial state
            result = self.state_manager.save_migration_state(state)

            # If we're in idle mode, wake up the window processor
            if self.idle_mode:
                logger.info("New migration received, waking up from idle mode")
                self.idle_mode = False
                self.current_poll_interval = self.min_poll_interval
                self.empty_checks_count = 0
                self.idle_condition.notify_all()  # Wake up the window processor

            return result

    def _window_processor(self):
        """Main processing loop for the sliding window"""
        while self.running:
            try:
                found_pending = False
                with self.lock:
                    # If we have room in the window, get pending migrations
                    available_slots = self.window_size - len(self.in_flight)

                    # Only query for pending migrations if we have available slots
                    if available_slots > 0:
                        # Get pending migrations
                        pending = self.state_manager.get_migrations_by_status(
                            MigrationStatus.PENDING,
                            limit=available_slots
                        )

                        # Process each pending migration
                        if pending:
                            found_pending = True
                            self.empty_checks_count = 0  # Reset counter when we find migrations
                            self.idle_mode = False  # Ensure we're not in idle mode

                            # If we found pending migrations, decrease poll interval for responsiveness
                            self.current_poll_interval = self.min_poll_interval

                            for migration in pending:
                                # Update status to IN_FLIGHT
                                migration.status = MigrationStatus.IN_FLIGHT
                                migration.attempts += 1
                                migration.last_attempt = datetime.now()
                                self.state_manager.save_migration_state(migration)

                                # Add to in-flight tracking
                                self.in_flight[migration.sequence_id] = migration

                                # Process in a separate thread to not block the window
                                threading.Thread(
                                    target=self._process_migration,
                                    args=(migration,),
                                    daemon=True
                                ).start()
                        else:
                            # No pending migrations found
                            self.empty_checks_count += 1

                            # If we've had several empty checks, gradually increase the poll interval
                            if self.empty_checks_count >= self.max_empty_checks:
                                # Increase poll interval up to max_poll_interval
                                self.current_poll_interval = min(
                                    self.current_poll_interval * 1.5,  # Increase by 50%
                                    self.max_poll_interval
                                )
                                # Reset counter but don't let it go to zero to avoid oscillation
                                self.empty_checks_count = self.max_empty_checks // 2

                                # Check if we should enter idle mode (no pending migrations and no in-flight)
                                if len(self.in_flight) == 0:
                                    # Double-check that there are no pending migrations
                                    pending_count = len(self.state_manager.get_migrations_by_status(
                                        MigrationStatus.PENDING,
                                        limit=1  # We only need to know if there are any
                                    ))

                                    if pending_count == 0:
                                        logger.info("No pending or in-flight migrations, entering idle mode")
                                        self.idle_mode = True
                    else:
                        # Window is full, no need to check for pending migrations
                        # Log this at debug level to avoid flooding logs
                        logger.debug("Window full, skipping pending migration check",
                                    window_size=self.window_size,
                                    in_flight=len(self.in_flight))

                    # If we're in idle mode, wait for a signal instead of polling
                    if self.idle_mode:
                        logger.debug("In idle mode, waiting for new migrations")
                        # Wait with a timeout (in case we need to check for new migrations periodically)
                        self.idle_condition.wait(self.idle_check_interval)

                        # After waking up, check if we're still in idle mode
                        if self.idle_mode:
                            # Periodically check for new pending migrations even in idle mode
                            # This is a safety measure in case a migration was added without waking us up
                            pending_count = len(self.state_manager.get_migrations_by_status(
                                MigrationStatus.PENDING,
                                limit=1
                            ))

                            if pending_count > 0:
                                logger.info("Found pending migrations while in idle mode, resuming normal operation")
                                self.idle_mode = False
                                self.current_poll_interval = self.min_poll_interval
                                self.empty_checks_count = 0

                # Only sleep if we're not in idle mode (otherwise we're already waiting on the condition)
                if not self.idle_mode:
                    # Adaptive sleep based on current conditions
                    time.sleep(self.current_poll_interval)

                # Log poll interval changes at debug level
                if self.current_poll_interval != self.min_poll_interval:
                    logger.debug("Adaptive polling interval",
                                current_interval=self.current_poll_interval,
                                empty_checks=self.empty_checks_count)

            except Exception as e:
                logger.error("Error in window processor", error=str(e))
                time.sleep(1)

    def _retry_processor(self):
        """Process failed migrations that are due for retry"""
        while self.running:
            try:
                with self.lock:
                    # If we're in idle mode, wait for a signal instead of polling
                    if self.idle_mode:
                        logger.debug("Retry processor in idle mode, waiting for new migrations")
                        # Wait with a timeout (in case we need to check for new migrations periodically)
                        self.idle_condition.wait(self.idle_check_interval)
                        continue  # Skip this iteration and check idle_mode again in the next loop

                # Only proceed with polling if not in idle mode
                # Get failed migrations
                failed = self.state_manager.get_migrations_by_status(
                    MigrationStatus.FAILED
                )

                retry_time = datetime.now() - timedelta(seconds=self.retry_interval)

                for migration in failed:
                    # Check if it's time to retry
                    if migration.last_attempt and migration.last_attempt < retry_time:
                        with self.lock:
                            # If we've entered idle mode while processing, stop and wait
                            if self.idle_mode:
                                break

                            # Reset to PENDING for the window processor to pick up
                            migration.status = MigrationStatus.PENDING
                            self.state_manager.save_migration_state(migration)

                            # Since we're adding a pending migration, make sure we're not in idle mode
                            if self.idle_mode:
                                logger.info("New pending migration from retry, waking up from idle mode")
                                self.idle_mode = False
                                self.current_poll_interval = self.min_poll_interval
                                self.empty_checks_count = 0
                                self.idle_condition.notify_all()  # Wake up the window processor

                # Check every 5 seconds if not in idle mode
                time.sleep(5)

            except Exception as e:
                logger.error("Error in retry processor", error=str(e))
                time.sleep(1)

    def _process_migration(self, migration: MigrationState):
        """Process a single migration"""
        success = False
        error_message = None

        try:
            # Call the process function
            success = self.process_func(migration)
        except Exception as e:
            error_message = str(e)
            logger.error("Error processing migration",
                        error=error_message,
                        migration=migration.model_dump())

        with self.lock:
            # Remove from in-flight
            self.in_flight.pop(migration.sequence_id, None)

            if success:
                # Update window size on success (TCP-like congestion control)
                if self.window_size < self.max_window_size:
                    self.window_size += 1

                # Update migration state
                migration.status = MigrationStatus.COMPLETED
                migration.completed_at = datetime.now()
                self.state_manager.save_migration_state(migration)

                logger.info("Migration completed successfully",
                           sequence_id=migration.sequence_id,
                           issue_id=migration.issue_id)
            else:
                # Reduce window size on failure
                self.window_size = max(self.min_window_size, self.window_size - 1)

                # Update migration state
                migration.status = MigrationStatus.FAILED
                migration.error_message = error_message
                self.state_manager.save_migration_state(migration)

                logger.warning("Migration failed, will retry later",
                             sequence_id=migration.sequence_id,
                             issue_id=migration.issue_id,
                             attempts=migration.attempts)