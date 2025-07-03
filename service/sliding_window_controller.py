"""Sliding Window Controller for issue migration"""

import time
import threading
from datetime import datetime, timedelta
from typing import Callable
import structlog

from models import MigrationState, MigrationStatus
from migration_state_manager import MigrationStateManager

logger = structlog.get_logger()

class SlidingWindowController:
    """Implements a sliding window pattern for reliable issue migration"""

    def __init__(self, state_manager: MigrationStateManager,
                 process_func: Callable[[MigrationState], bool],
                 initial_window_size: int = 5,
                 min_window_size: int = 1,
                 max_window_size: int = 20,
                 retry_interval: int = 60):
        """
        Initialize the sliding window controller

        Args:
            state_manager: The migration state manager
            process_func: Function to process a migration (returns True if successful)
            initial_window_size: Initial size of the sliding window
            min_window_size: Minimum window size
            max_window_size: Maximum window size
            retry_interval: Seconds to wait before retrying failed migrations
        """
        self.state_manager = state_manager
        self.process_func = process_func
        self.window_size = initial_window_size
        self.min_window_size = min_window_size
        self.max_window_size = max_window_size
        self.retry_interval = retry_interval

        self.in_flight = {}  # sequence_id -> MigrationState
        self.lock = threading.RLock()
        self.running = False
        self.window_thread = None
        self.retry_thread = None

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
                       window_size=self.window_size)

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
            return self.state_manager.save_migration_state(state)

    def _window_processor(self):
        """Main processing loop for the sliding window"""
        while self.running:
            try:
                with self.lock:
                    # If we have room in the window, get pending migrations
                    available_slots = self.window_size - len(self.in_flight)

                    if available_slots > 0:
                        # Get pending migrations
                        pending = self.state_manager.get_migrations_by_status(
                            MigrationStatus.PENDING,
                            limit=available_slots
                        )

                        # Process each pending migration
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

                # Sleep briefly to prevent CPU spinning
                time.sleep(0.1)

            except Exception as e:
                logger.error("Error in window processor", error=str(e))
                time.sleep(1)

    def _retry_processor(self):
        """Process failed migrations that are due for retry"""
        while self.running:
            try:
                # Get failed migrations
                failed = self.state_manager.get_migrations_by_status(
                    MigrationStatus.FAILED
                )

                retry_time = datetime.now() - timedelta(seconds=self.retry_interval)

                for migration in failed:
                    # Check if it's time to retry
                    if migration.last_attempt and migration.last_attempt < retry_time:
                        with self.lock:
                            # Reset to PENDING for the window processor to pick up
                            migration.status = MigrationStatus.PENDING
                            self.state_manager.save_migration_state(migration)

                # Check every 5 seconds
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
