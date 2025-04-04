"""
Valkey Log Handler for the Monarch service.
Stores logs in Valkey Redis Streams with appropriate indexing.
"""
import json
import time
from datetime import datetime, timedelta
import structlog

class ValkeyLogHandler:
    """
    Custom log handler that stores logs in Valkey Redis Streams.
    Provides automatic indexing and retention policy enforcement.
    """
    def __init__(self, valkey_client, retention_days=30):
        """
        Initialize the Valkey log handler.

        Args:
            valkey_client: Valkey client instance
            retention_days: Number of days to retain logs (default: 30)
        """
        self.valkey = valkey_client
        self.retention_days = retention_days
        self.logger = structlog.get_logger()

    def __call__(self, logger, method_name, event_dict):
        """
        Process and store a log event in Valkey.
        This method signature matches the structlog processor interface.

        Args:
            logger: The logger that created the event
            method_name: The name of the logger method called
            event_dict: The event dictionary

        Returns:
            The processed event dictionary (for chaining with other processors)
        """
        try:
            # Add level if not present (derived from method_name)
            if 'level' not in event_dict:
                event_dict['level'] = method_name

            # Format the log entry as JSON
            log_json = json.dumps(event_dict)

            # Get the current date for stream naming
            current_date = datetime.now().strftime("%Y-%m-%d")
            stream_key = f"logs:{current_date}"

            # Add to the stream
            entry_id = self.valkey.xadd(stream_key, {"data": log_json})

            # Index by timestamp, level, and service name
            timestamp = event_dict.get("timestamp")
            level = event_dict.get("level", "info")
            service = "monarch"  # Can be parameterized if multiple services

            # Add to sorted sets for indexing
            if timestamp:
                # Convert ISO timestamp to float if it's a string
                if isinstance(timestamp, str):
                    try:
                        # Remove the 'Z' suffix if present and convert to datetime
                        if timestamp.endswith('Z'):
                            timestamp = timestamp[:-1]
                        dt = datetime.fromisoformat(timestamp)
                        # Convert to timestamp (seconds since epoch)
                        timestamp_float = dt.timestamp()
                    except Exception as e:
                        print(f"Error converting timestamp {timestamp}: {str(e)}")
                        timestamp_float = time.time()  # Fallback to current time
                else:
                    timestamp_float = float(timestamp)  # Ensure it's a float

                self.valkey.zadd("logs:index:timestamp", {entry_id: timestamp_float})
            self.valkey.zadd(f"logs:index:level:{level}", {entry_id: time.time()})
            self.valkey.zadd(f"logs:index:service:{service}", {entry_id: time.time()})

            # Update metadata
            self.valkey.hset("logs:metadata", stream_key, int(time.time()))

            # Apply retention policy
            self._apply_retention_policy()
        except Exception as e:
            # Log the error but don't break the logging chain
            print(f"Error in ValkeyLogHandler: {str(e)}")

        # Return the event for further processing
        return event_dict

    def _apply_retention_policy(self):
        """
        Apply the retention policy by removing logs older than retention_days.
        """
        try:
            # Get all stream keys from metadata
            all_streams = self.valkey.hgetall("logs:metadata")

            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=self.retention_days)

            # Delete streams older than retention period
            for stream_key, timestamp in all_streams.items():
                try:
                    stream_date = datetime.strptime(stream_key.split(":")[-1], "%Y-%m-%d")
                    if stream_date < cutoff_date:
                        # Delete the stream
                        self.valkey.delete(stream_key)
                        # Delete from metadata
                        self.valkey.hdel("logs:metadata", stream_key)

                        # Clean up indexes
                        # This is a simplified approach - in production, you might want
                        # to be more selective about which entries to remove
                        self._clean_up_indexes(stream_key)
                except Exception as e:
                    print(f"Error cleaning up stream {stream_key}: {str(e)}")
        except Exception as e:
            print(f"Error applying retention policy: {str(e)}")

    def _clean_up_indexes(self, stream_key):
        """
        Clean up indexes for a deleted stream.

        Args:
            stream_key: The key of the stream that was deleted
        """
        try:
            # Get all entries for the stream
            # This is a simplified approach - in a real implementation,
            # you would need to store the mapping between stream keys and entry IDs

            # For timestamp index
            self.valkey.zremrangebyscore("logs:index:timestamp", 0, time.time())

            # For level indexes - clean up old entries across all levels
            for level in ["info", "warning", "error", "debug"]:
                self.valkey.zremrangebyscore(f"logs:index:level:{level}", 0, time.time() - (self.retention_days * 86400))

            # For service index
            self.valkey.zremrangebyscore("logs:index:service:monarch", 0, time.time() - (self.retention_days * 86400))
        except Exception as e:
            print(f"Error cleaning up indexes: {str(e)}")