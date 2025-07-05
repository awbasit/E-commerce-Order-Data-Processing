"""
Logging configuration
"""
import logging
import structlog
import os
from typing import Any


def setup_logging(service_name: str) -> Any:
    """
    Set up structured logging
    
    Args:
        service_name: Name of the service
        
    Returns:
        Configured logger
    """
    # Configure standard logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Create logger
    logger = structlog.get_logger(service_name)
    
    # Add context
    logger = logger.bind(
        service=service_name,
        environment=os.getenv('ENVIRONMENT', 'dev'),
        version=os.getenv('SERVICE_VERSION', '1.0.0')
    )
    
    return logger