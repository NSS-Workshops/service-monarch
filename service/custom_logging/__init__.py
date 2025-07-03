"""Custom logging module for the Monarch service."""

# Use lazy loading for all components to avoid circular imports

def get_valkey_log_handler():
    """Get the ValkeyLogHandler class lazily to avoid circular imports."""
    from service.custom_logging.valkey_log_handler import ValkeyLogHandler
    return ValkeyLogHandler

def get_log_retriever():
    """Get the LogRetriever class lazily to avoid circular imports."""
    from service.custom_logging.log_retriever import LogRetriever
    return LogRetriever

def get_web_interface():
    """Get the web_interface module lazily to avoid circular imports."""
    import service.custom_logging.web_interface as web_interface
    return web_interface

__all__ = ['get_valkey_log_handler', 'get_log_retriever', 'get_web_interface']