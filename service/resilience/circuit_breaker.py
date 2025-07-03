"""
Circuit Breaker for the Monarch service.
Prevents cascading failures by failing fast when a service is unavailable.
"""
import time
import threading
import structlog

# Configure logger
logger = structlog.get_logger()

class CircuitBreakerOpenError(Exception):
    """Exception raised when a circuit breaker is open."""
    pass

class CircuitBreaker:
    """
    Implements the Circuit Breaker pattern to prevent cascading failures.
    """
    def __init__(self, name, failure_threshold=5, reset_timeout=60):
        """
        Initialize the circuit breaker.

        Args:
            name: Name of the circuit breaker (for logging)
            failure_threshold: Number of failures before opening the circuit
            reset_timeout: Seconds to wait before attempting to close the circuit
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.lock = threading.RLock()

    def execute(self, func, *args, **kwargs):
        """
        Execute a function with circuit breaker protection.

        Args:
            func: Function to execute
            *args: Arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function

        Returns:
            The result of the function

        Raises:
            CircuitBreakerOpenError: If the circuit is open
            Exception: Any exception raised by the function
        """
        with self.lock:
            if self.state == "OPEN":
                # Check if reset timeout has elapsed
                if time.time() - self.last_failure_time > self.reset_timeout:
                    logger.info(f"Circuit {self.name} entering HALF-OPEN state")
                    self.state = "HALF-OPEN"
                else:
                    raise CircuitBreakerOpenError(f"Circuit {self.name} is OPEN")

        try:
            result = func(*args, **kwargs)

            # Success - reset if in HALF-OPEN state
            with self.lock:
                if self.state == "HALF-OPEN":
                    logger.info(f"Circuit {self.name} closing after successful execution")
                    self.state = "CLOSED"
                    self.failures = 0

            return result

        except Exception as e:
            with self.lock:
                self.failures += 1
                self.last_failure_time = time.time()

                if self.state == "CLOSED" and self.failures >= self.failure_threshold:
                    logger.warning(f"Circuit {self.name} opening after {self.failures} failures")
                    self.state = "OPEN"
                elif self.state == "HALF-OPEN":
                    logger.warning(f"Circuit {self.name} reopening after failure in HALF-OPEN state")
                    self.state = "OPEN"

            raise e