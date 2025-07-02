"""
Resilient PubSub handler for the Monarch service.
Provides non-blocking message processing and automatic reconnection.
"""
import time
import threading
import structlog

# Configure logger
logger = structlog.get_logger()

class ResilientPubSub:
    """
    A wrapper around the Valkey PubSub that provides non-blocking message processing
    and automatic reconnection.
    """
    def __init__(self, valkey_client, channel, message_handler, error_handler=None):
        """
        Initialize the resilient PubSub handler.

        Args:
            valkey_client: Valkey client instance
            channel: Channel to subscribe to
            message_handler: Function to handle messages
            error_handler: Function to handle errors (optional)
        """
        self.valkey_client = valkey_client
        self.channel = channel
        self.message_handler = message_handler
        self.error_handler = error_handler or (lambda e: logger.error("PubSub error", error=str(e)))
        self.running = False
        self.pubsub = None
        self._initialize_pubsub()

    def _initialize_pubsub(self):
        """
        Initialize the PubSub connection and subscribe to the channel.
        """
        try:
            self.pubsub = self.valkey_client.pubsub()
            self.pubsub.subscribe(self.channel)
            logger.info(f"Subscribed to channel {self.channel}")
        except Exception as e:
            logger.error(f"Failed to subscribe to channel {self.channel}", error=str(e))
            raise

    def start(self):
        """
        Start listening for messages in a separate thread.

        Returns:
            Thread: The background thread
        """
        self.running = True
        thread = threading.Thread(target=self._listen_loop, daemon=True)
        thread.start()
        return thread

    def _listen_loop(self):
        """
        Main loop for listening to messages.
        Uses non-blocking get_message with timeout instead of blocking listen().
        """
        reconnect_delay = 1
        max_reconnect_delay = 30

        while self.running:
            try:
                # Use get_message with timeout instead of blocking listen()
                message = self.pubsub.get_message(timeout=1.0)
                if message and message['type'] == 'message':
                    # Process message in a separate thread to avoid blocking
                    threading.Thread(
                        target=self._safe_process_message,
                        args=(message,),
                        daemon=True
                    ).start()

                # Reset reconnect delay on successful operation
                reconnect_delay = 1

            except Exception as e:
                self.error_handler(e)

                # Exponential backoff for reconnection attempts
                logger.warning(f"PubSub error, reconnecting in {reconnect_delay}s", error=str(e))
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

                try:
                    # Attempt to re-initialize the pubsub connection
                    self._initialize_pubsub()
                except Exception as e:
                    logger.error("Failed to reconnect pubsub", error=str(e))

    def _safe_process_message(self, message):
        """
        Process a message safely, catching any exceptions.

        Args:
            message: The message to process
        """
        try:
            self.message_handler(message)
        except Exception as e:
            logger.error("Error processing message", error=str(e))

    def stop(self):
        """
        Stop listening for messages.
        """
        self.running = False
        if self.pubsub:
            try:
                self.pubsub.unsubscribe()
                self.pubsub.close()
            except Exception as e:
                logger.error("Error closing pubsub", error=str(e))