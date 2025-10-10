import pika
import json
import logging
import os
import time
from typing import Dict, Any
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class RabbitMQClient:
    """
    Base RabbitMQ client for PEAR Reconciliation Service
    Handles connection management and publishing operations
    """
    
    def __init__(self, service_name: str = "reconciliation-service"):
        self.service_name = service_name
        
        # Try to get individual parameters first, fall back to URL
        self.host = os.getenv('RABBITMQ_HOST')
        self.port = os.getenv('RABBITMQ_PORT')
        self.username = os.getenv('RABBITMQ_USER')
        self.password = os.getenv('RABBITMQ_PASS')
        self.virtual_host = os.getenv('RABBITMQ_VIRTUAL_HOST', 'vhost')
        
        # Fall back to URL if individual params not provided
        self.rabbitmq_url = os.getenv('RABBITMQ_URL')
        
        self.connection = None
        self.channel = None
        self.is_connected = False
    
    def connect(self, max_retries: int = 5) -> bool:
        """Connect to RabbitMQ with retry logic"""
        for attempt in range(max_retries):
            try:
                # Try URL-based connection first if available
                if self.rabbitmq_url:
                    parameters = pika.URLParameters(self.rabbitmq_url)
                else:
                    # Fall back to individual parameters
                    credentials = pika.PlainCredentials(self.username, self.password)
                    parameters = pika.ConnectionParameters(
                        host=self.host,
                        port=int(self.port) if self.port else 5672,
                        virtual_host=self.virtual_host,
                        credentials=credentials,
                        heartbeat=30,
                        blocked_connection_timeout=300
                    )
                
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Enable publisher confirms for reliability
                self.channel.confirm_delivery()
                
                self.is_connected = True
                
                logger.info(f"{self.service_name} connected to RabbitMQ")
                return True
                
            except Exception as e:
                logger.error(f"Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
        self.is_connected = False
        return False
    
    def ensure_connection(self):
        """Ensure connection is active"""
        if not self.is_connected or not self.connection or self.connection.is_closed:
            if not self.connect():
                raise ConnectionError(f"{self.service_name}: Failed to connect to RabbitMQ")
    
    def declare_exchange(self, exchange: str, exchange_type: str = 'topic', durable: bool = True):
        """Declare an exchange (idempotent)"""
        try:
            self.ensure_connection()
            self.channel.exchange_declare(
                exchange=exchange,
                exchange_type=exchange_type,
                durable=durable
            )
            logger.info(f"{self.service_name} declared exchange: {exchange}")
        except Exception as e:
            logger.error(f"Failed to declare exchange {exchange}: {str(e)}")
            raise
    
    def publish(
        self, 
        exchange: str, 
        routing_key: str, 
        message: Dict[str, Any],
        correlation_id: str = None,
        max_retries: int = 3
    ) -> bool:
        """
        Publish message with fault tolerance
        """
        message_body = {
            'timestamp': datetime.now().isoformat(),
            'source_service': self.service_name,
            'data': message
        }
        
        for attempt in range(max_retries):
            try:
                self.ensure_connection()
                
                # Log the message before publishing
                msg_correlation_id = correlation_id or message.get('correlation_id', 'unknown')
                logger.info(f"Publishing message {msg_correlation_id} to {exchange}/{routing_key} (attempt {attempt+1})")
                
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=json.dumps(message_body, default=str),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Persistent message
                        timestamp=int(time.time()),
                        content_type='application/json',
                        correlation_id=msg_correlation_id,
                        message_id=f"{self.service_name}_{int(time.time() * 1000)}"
                    )
                )
                
                logger.info(f"Successfully published {msg_correlation_id} to {exchange}/{routing_key}")
                return True
                
            except Exception as e:
                logger.error(f"Publish attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    # Try to reconnect
                    self.is_connected = False
                    
        logger.error(f"{self.service_name} failed to publish after {max_retries} attempts")
        return False
    
    def close(self):
        """Close connection"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
                logger.debug(f"{self.service_name} channel closed")
                
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.debug(f"{self.service_name} connection closed")
                
            self.is_connected = False
            logger.info(f"{self.service_name} RabbitMQ connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {str(e)}")
    
    def __enter__(self):
        """Context manager support"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.close()
