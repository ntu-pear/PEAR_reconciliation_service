from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Service Configuration
    SERVICE_NAME: str = Field(default="reconciliation-service", description="Service name")
    SERVICE_VERSION: str = Field(default="1.0.0", description="Service version")
    PORT: int = Field(default=8000, description="Port to run the service on")
    DEBUG: bool = Field(default=False, description="Enable debug mode")
    RELOAD: bool = Field(default=False, description="Enable auto-reload for development")
    
    # External Service URLs
    ACTIVITY_SERVICE_URL: str = Field(default="http://localhost:8001", description="Activity service base URL")
    PATIENT_SERVICE_URL: str = Field(default="http://localhost:8002", description="Patient service base URL")
    SCHEDULER_SERVICE_URL: str = Field(default="http://localhost:8003", description="Scheduler service base URL")
    
    # RabbitMQ Configuration
    RABBITMQ_URL: str = Field(default="amqp://admin:password@localhost:5672/vhost", description="RabbitMQ connection URL")
    
    # Optional RabbitMQ fields (for compatibility with existing env vars)
    RABBITMQ_HOST: Optional[str] = Field(default=None, description="RabbitMQ host (optional)")
    RABBITMQ_PORT: Optional[str] = Field(default=None, description="RabbitMQ port (optional)")
    RABBITMQ_USER: Optional[str] = Field(default=None, description="RabbitMQ user (optional)")
    RABBITMQ_PASS: Optional[str] = Field(default=None, description="RabbitMQ password (optional)")
    RABBITMQ_VIRTUAL_HOST: Optional[str] = Field(default=None, description="RabbitMQ virtual host (optional)")
    
    # Reconciliation Configuration
    RECONCILIATION_SCHEDULE: str = Field(default="55 * * * *", description="Cron expression for reconciliation schedule")
    RECONCILIATION_WINDOW_HOURS: int = Field(default=1, description="Default time window for reconciliation")
    RECONCILIATION_TIMEOUT_SECONDS: int = Field(default=30, description="Timeout for service calls")
    MAX_RECORDS_PER_REQUEST: int = Field(default=1000, description="Maximum records per integrity request")
    
    # Logging Configuration
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")
    LOG_FORMAT: str = Field(default="json", description="Log format (json or text)")
    
    # Monitoring Configuration
    ENABLE_METRICS: bool = Field(default=True, description="Enable Prometheus metrics")
    METRICS_PORT: int = Field(default=9090, description="Port for metrics endpoint")
    
    # Performance Configuration
    MAX_CONCURRENT_CHECKS: int = Field(default=5, description="Maximum concurrent integrity checks")
    RETRY_MAX_ATTEMPTS: int = Field(default=3, description="Maximum retry attempts for failed operations")
    RETRY_DELAY_SECONDS: int = Field(default=1, description="Delay between retry attempts")
    
    # Drift Resolution Configuration
    AUTO_RESOLVE_DRIFTS: bool = Field(default=True, description="Automatically resolve detected drifts")
    MAX_DRIFT_AGE_HOURS: int = Field(default=24, description="Maximum age of drifts to auto-resolve")
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Ignore extra environment variables


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()
