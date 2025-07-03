"""Message handling for the Monarch service"""
import asyncio
import time
import structlog
from models import MigrationData

logger = structlog.get_logger()

class MessageHandler:
    """Handles message processing"""
    def __init__(self, service, executor):
        self.service = service
        self.executor = executor

    def handle_message(self, message):
        """Handle a received message"""
        if message['type'] == 'message':
            try:
                # Update metrics
                self.service.metrics.last_message_received_timestamp.set(time.time())
                self.service.valkey_client.set("monarch:last_message_time", str(time.time()))

                # Process message with metrics
                with self.service.metrics.message_processing_time.time():
                    try:
                        data = MigrationData.model_validate_json(message['data'])
                        # Use executor to process message asynchronously
                        self.executor.submit(self._run_async_process, data)
                    except Exception as e:
                        self.service.metrics.message_processing_errors.inc()
                        logger.error("Error processing message", error=str(e))
            except Exception as e:
                logger.error("Error in message handler", error=str(e))

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
            await self.service.migration_processor.migrate_tickets(data)
        except Exception as e:
            logger.error("Error processing message", error=str(e))
