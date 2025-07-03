"""
Log Retriever for the Monarch service.
Provides methods to retrieve and filter logs stored in Valkey.
"""
import json
from datetime import datetime

class LogRetriever:
    """
    Retrieves logs from Valkey based on various filters.
    """
    def __init__(self, valkey_client):
        """
        Initialize the log retriever.

        Args:
            valkey_client: Valkey client instance
        """
        self.valkey = valkey_client

    def get_logs_by_timerange(self, start_time, end_time, limit=100):
        """
        Get logs within a specific time range.

        Args:
            start_time: Start timestamp (milliseconds since epoch)
            end_time: End timestamp (milliseconds since epoch)
            limit: Maximum number of logs to return

        Returns:
            List of log entries
        """
        # Use the numeric timestamp values directly for zrangebyscore
        # Valkey expects numeric values (floats) for min and max parameters
        entry_ids = self.valkey.zrangebyscore(
            "logs:index:timestamp",
            float(start_time/1000),  # Convert to seconds and ensure it's a float
            float(end_time/1000),    # Convert to seconds and ensure it's a float
            start=0,
            num=limit
        )

        # Retrieve the actual log entries
        return self._get_log_entries(entry_ids)

    def get_logs_by_level(self, level, limit=100):
        """
        Get logs with a specific log level.

        Args:
            level: Log level (info, error, warning, debug)
            limit: Maximum number of logs to return

        Returns:
            List of log entries
        """
        # Query the level index
        entry_ids = self.valkey.zrevrange(
            f"logs:index:level:{level}",
            0,
            limit-1
        )

        # Retrieve the actual log entries
        return self._get_log_entries(entry_ids)

    def get_logs_by_service(self, service, limit=100):
        """
        Get logs for a specific service.

        Args:
            service: Service name
            limit: Maximum number of logs to return

        Returns:
            List of log entries
        """
        # Query the service index
        entry_ids = self.valkey.zrevrange(
            f"logs:index:service:{service}",
            0,
            limit-1
        )

        # Retrieve the actual log entries
        return self._get_log_entries(entry_ids)

    def _get_log_entries(self, entry_ids):
        """
        Retrieve log entries by their IDs.

        Args:
            entry_ids: List of entry IDs

        Returns:
            List of log entries
        """
        logs = []

        for entry_id in entry_ids:
            try:
                # Handle both string and bytes entry_ids
                entry_id_str = entry_id.decode('utf-8') if isinstance(entry_id, bytes) else entry_id

                # Extract date from entry_id (format: timestamp-sequence)
                timestamp_part = entry_id_str.split('-')[0]
                date_str = datetime.fromtimestamp(int(timestamp_part)/1000).strftime('%Y-%m-%d')
                stream_key = f"logs:{date_str}"

                # Get the entry from the stream
                entries = self.valkey.xrange(stream_key, entry_id, entry_id)
                if entries:
                    _, entry_data = entries[0]
                    # Handle both string and bytes data
                    data = entry_data.get(b'data', b'{}') if isinstance(entry_data.get('data', b'{}'), bytes) else entry_data.get('data', '{}')
                    log_json = data.decode('utf-8') if isinstance(data, bytes) else data
                    logs.append(json.loads(log_json))
            except Exception as e:
                print(f"Error retrieving log entry {entry_id}: {str(e)}")

        return logs

    def get_available_log_levels(self):
        """
        Get all available log levels.

        Returns:
            List of log levels
        """
        # Get all keys matching the pattern logs:index:level:*
        level_keys = self.valkey.keys("logs:index:level:*")

        # Extract the level from each key
        levels = [key.decode('utf-8').split(':')[-1] for key in level_keys]

        # If no levels found, return default levels
        if not levels:
            return ["info", "error", "warning", "debug"]

        return levels

    def get_available_services(self):
        """
        Get all available services.

        Returns:
            List of services
        """
        # Get all keys matching the pattern logs:index:service:*
        service_keys = self.valkey.keys("logs:index:service:*")

        # Extract the service from each key
        services = [key.decode('utf-8').split(':')[-1] for key in service_keys]

        # If no services found, return default service
        if not services:
            return ["monarch"]

        return services