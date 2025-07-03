"""Migration State Manager"""
from typing import List, Optional
import structlog
from models import MigrationState, MigrationStatus

logger = structlog.get_logger()


class MigrationStateManager:
    """Manages persistence of migration state"""

    def __init__(self, valkey_client):
        self.valkey_client = valkey_client
        self.migration_key_prefix = "monarch:migration:"
        self.sequence_key = "monarch:migration:sequence"

    def get_next_sequence_id(self) -> int:
        """Get the next sequence ID for a migration"""
        return self.valkey_client.incr(self.sequence_key)

    def save_migration_state(self, state: MigrationState) -> bool:
        """Save a migration state to Valkey"""
        key = f"{self.migration_key_prefix}{state.source_repo}:{state.target_repo}:{state.issue_id}"
        try:
            self.valkey_client.set(key, state.model_dump_json())
            # Add to appropriate index sets based on status
            status_key = f"{self.migration_key_prefix}status:{state.status.value}"
            self.valkey_client.sadd(status_key, key)

            # If status changed, remove from old status set
            for status in MigrationStatus:
                if status != state.status:
                    self.valkey_client.srem(f"{self.migration_key_prefix}status:{status.value}", key)

            return True
        except Exception as e:
            logger.error("Failed to save migration state", error=str(e), state=state.model_dump())
            return False

    def get_migration_state(self, source_repo: str, target_repo: str, issue_id: str) -> Optional[MigrationState]:
        """Get a migration state from Valkey"""
        key = f"{self.migration_key_prefix}{source_repo}:{target_repo}:{issue_id}"
        try:
            data = self.valkey_client.get(key)
            if data:
                return MigrationState.model_validate_json(data)
            return None
        except Exception as e:
            logger.error("Failed to get migration state", error=str(e), key=key)
            return None

    def get_migrations_by_status(self, status: MigrationStatus, limit: int = 100) -> List[MigrationState]:
        """Get migrations with a specific status"""
        status_key = f"{self.migration_key_prefix}status:{status.value}"
        try:
            keys = self.valkey_client.smembers(status_key)
            states = []

            for key in list(keys)[:limit]:
                data = self.valkey_client.get(key)
                if data:
                    states.append(MigrationState.model_validate_json(data))

            return states
        except Exception as e:
            logger.error("Failed to get migrations by status", error=str(e), status=status.value)
            return []
