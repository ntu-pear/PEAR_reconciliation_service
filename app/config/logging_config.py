import logging
import logging.config
import sys
from pythonjsonlogger import jsonlogger
from app.config.settings import get_settings

settings = get_settings()


def setup_logging():
    """Setup structured logging configuration"""
    
    if settings.LOG_FORMAT.lower() == "json":
        # JSON structured logging
        formatter = jsonlogger.JsonFormatter(
            fmt='%(asctime)s %(name)s %(levelname)s %(message)s %(pathname)s %(funcName)s %(lineno)d',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    else:
        # Standard text logging
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))
    root_logger.addHandler(console_handler)
    
    # Application logger
    app_logger = logging.getLogger("app")
    app_logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))
    
    # Third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    
    # Add service context to all logs
    class ServiceContextFilter(logging.Filter):
        def filter(self, record):
            record.service_name = settings.SERVICE_NAME
            record.service_version = settings.SERVICE_VERSION
            return True
    
    context_filter = ServiceContextFilter()
    for handler in root_logger.handlers:
        handler.addFilter(context_filter)
