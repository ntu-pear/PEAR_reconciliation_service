import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

from .rabbitmq_client import RabbitMQClient
from app.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class DriftPublisher:
    """
    Publisher for drift detection notifications.
    Publishes to reconciliation.events exchange with drift.detected.* routing keys.
    """
    
    def __init__(self):
        self.client = RabbitMQClient("reconciliation-drift-publisher")
        self._connected = False
    
    def connect(self):
        """Establish connection to RabbitMQ and declare exchanges"""
        try:
            if not self.client.connect():
                raise ConnectionError("Failed to connect to RabbitMQ")
            
            # Declare the reconciliation.events exchange
            self.client.declare_exchange(
                exchange='reconciliation.events',
                exchange_type='topic',
                durable=True
            )
            
            self._connected = True
            logger.info("Drift publisher connected and exchanges declared")
            
        except Exception as e:
            logger.error(f"Failed to connect drift publisher: {str(e)}")
            self._connected = False
            raise
    
    def ensure_connected(self):
        """Ensure publisher is connected"""
        if not self._connected:
            self.connect()
    
    def publish_drift_detected(
        self,
        record_type: str,
        record_id: int,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ) -> bool:
        """
        Publish a drift detection event.
        
        Args:
            record_type: Type of record (e.g., 'activity', 'centre_activity', 'patient')
            record_id: ID of the record with drift
            details: Optional additional details about the drift
            correlation_id: Optional correlation ID for tracking
            
        Returns:
            True if published successfully, False otherwise
        """
        try:
            self.ensure_connected()
            
            correlation_id = correlation_id or f"{uuid.uuid4()}"
            
            # Routing key format: drift.detected.<record_type>.<record_id>
            routing_key = f"drift.detected.{record_type}.{record_id}"
            
            # Message payload
            message = {
                "event_type": "DRIFT_DETECTED",
                "record_type": record_type,
                "record_id": record_id,
                "correlation_id": correlation_id,
                "detected_at": datetime.now().isoformat(),
                "source_service": "reconciliation_service",
                "details": details or {}
            }
            
            # Publish to reconciliation.events exchange
            success = self.client.publish(
                exchange='reconciliation.events',
                routing_key=routing_key,
                message=message,
                correlation_id=correlation_id
            )
            
            if success:
                logger.info(
                    f"Published drift notification for {record_type} {record_id}"
                )
            else:
                logger.error(
                    f"Failed to publish drift notification for {record_type} {record_id}"
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Error publishing drift detection: {str(e)}")
            return False
    
    def publish_batch_drifts(
        self,
        drifts: list[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Publish multiple drift notifications in batch.
        
        Args:
            drifts: List of drift dictionaries with keys:
                   - record_type
                   - record_id
                   - details (optional)
        
        Returns:
            Dictionary with success and failure counts
        """
        results = {
            "total": len(drifts),
            "successful": 0,
            "failed": 0
        }
        
        for drift in drifts:
            success = self.publish_drift_detected(
                record_type=drift.get("record_type"),
                record_id=drift.get("record_id"),
                details=drift.get("details")
            )
            
            if success:
                results["successful"] += 1
            else:
                results["failed"] += 1
        
        logger.info(
            f"Batch drift publishing completed: {results['successful']}/{results['total']} successful"
        )
        
        return results
    
    def close(self):
        """Close RabbitMQ connection"""
        try:
            if self.client:
                self.client.close()
                self._connected = False
            logger.info("Drift publisher disconnected")
        except Exception as e:
            logger.error(f"Error closing drift publisher: {str(e)}")
    
    def __enter__(self):
        """Context manager support"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.close()


# Global publisher instance (singleton pattern)
_drift_publisher_instance = None


def get_drift_publisher() -> DriftPublisher:
    """
    Get or create global drift publisher instance.
    This ensures we reuse the same connection.
    """
    global _drift_publisher_instance
    
    if _drift_publisher_instance is None:
        _drift_publisher_instance = DriftPublisher()
        _drift_publisher_instance.connect()
    
    return _drift_publisher_instance


def cleanup_drift_publisher():
    """Cleanup global drift publisher instance"""
    global _drift_publisher_instance
    
    if _drift_publisher_instance:
        _drift_publisher_instance.close()
        _drift_publisher_instance = None


# Legacy MessagePublisher for backward compatibility
class MessagePublisher:
    """
    Legacy message publisher - kept for backward compatibility.
    New code should use DriftPublisher instead.
    """
    
    def __init__(self):
        self.drift_publisher = get_drift_publisher()
    
    async def connect(self):
        """Connect to RabbitMQ"""
        # Connection is handled automatically by get_drift_publisher()
        pass
    
    async def disconnect(self):
        """Disconnect from RabbitMQ"""
        # Don't close the global instance
        pass
    
    async def publish_drift_detected(
        self,
        record_type: str,
        record_id: int,
        correlation_id: Optional[str] = None
    ) -> bool:
        """Publish drift detection (async wrapper)"""
        return self.drift_publisher.publish_drift_detected(
            record_type=record_type,
            record_id=record_id,
            correlation_id=correlation_id
        )


# For backward compatibility
async def get_publisher() -> MessagePublisher:
    """Get or create message publisher instance (async)"""
    return MessagePublisher()


async def cleanup_publisher():
    """Cleanup publisher instance (async)"""
    cleanup_drift_publisher()
