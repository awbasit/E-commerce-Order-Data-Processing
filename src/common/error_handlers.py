"""
Error handling utilities
"""
import traceback
from typing import Dict, Any, Optional


class ValidationError(Exception):
    """Custom validation error"""
    pass


def handle_exception(exception: Exception, logger: Optional[Any] = None) -> Dict[str, Any]:
    """
    Handle exceptions and return structured error info
    
    Args:
        exception: The exception to handle
        logger: Optional logger for logging
        
    Returns:
        Dict with error details
    """
    error_details = {
        'type': type(exception).__name__,
        'message': str(exception),
        'traceback': traceback.format_exc()
    }
    
    if logger:
        logger.error("Exception occurred", 
                    error_type=error_details['type'],
                    error_message=error_details['message'])
    
    return error_details


def create_error_response(error_type: str, message: str, **kwargs) -> Dict[str, Any]:
    """
    Create standardized error response
    
    Args:
        error_type: Type of error
        message: Error message
        **kwargs: Additional error details
        
    Returns:
        Standardized error dict
    """
    error_response = {
        'type': error_type,
        'message': message,
        'timestamp': pd.Timestamp.now().isoformat()
    }
    
    error_response.update(kwargs)
    return error_response