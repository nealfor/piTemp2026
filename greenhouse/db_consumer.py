
# db_consumer.py - Runs on Pi #3 (greenKafka)
# Consumes messages from all Kafka topics and writes to TimescaleDB

import json
import logging
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
import os
from datetime import datetime
from dotenv import load_dotenv
import time

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConsumer:
    """
    Consumes messages from Kafka topics and writes to TimescaleDB
    """
    
    def __init__(self):
        # Database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'greenhouse'),
            'user': os.getenv('DB_USER', 'greenhouse'),
            'password': os.getenv('DB_PASSWORD', 'greenhouse123')
        }
        
        # Kafka configuration
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
        
        # Topics to consume
        self.topics = [
            'greenhouse-readings',
            'greenhouse-commands',
            'greenhouse-status',
            'greenhouse-alerts'
        ]
        
        # Connect to database
        self.conn = None
        self.connect_db()
        
        # Connect to Kafka
        self.consumer = None
        self.connect_kafka()
        
        # Stats
        self.messages_processed = 0
        self.last_stats_time = datetime.now()
    
    def connect_db(self):
        """Connect to TimescaleDB with retry logic"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(**self.db_config)
                self.conn.autocommit = True
                logger.info(f"Connected to database at {self.db_config['host']}:{self.db_config['port']}")
                return
            except psycopg2.Error as e:
                logger.warning(f"Database connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    logger.error("Failed to connect to database after all retries")
                    raise
    
    def connect_kafka(self):
        """Connect to Kafka with retry logic"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=self.kafka_brokers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id='database-writer',
                    auto_offset_reset='earliest',  # Start from beginning if new consumer
                    enable_auto_commit=True
                )
                logger.info(f"Connected to Kafka at {self.kafka_brokers}")
                logger.info(f"Subscribed to topics: {', '.join(self.topics)}")
                return
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    logger.error("Failed to connect to Kafka after all retries")
                    raise
    
    def save_sensor_reading(self, data):
        """Save sensor reading to database"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sensor_readings 
                    (time, sensor_id, greenhouse_id, sensor_temp_f, sensor_humidity, 
                     soil_moisture, light_level, web_temp_f, weather_description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data.get('timestamp'),
                    data.get('sensor_id'),
                    data.get('greenhouse_id'),
                    data.get('sensor_temp_f'),
                    data.get('sensor_humidity'),
                    data.get('soil_moisture'),
                    data.get('light_level'),
                    data.get('web_temp_f'),
                    data.get('weather_description')
                ))
            logger.debug(f"Saved sensor reading: {data.get('greenhouse_id')} - {data.get('sensor_temp_f')}°F")
        except psycopg2.Error as e:
            logger.error(f"Failed to save sensor reading: {e}")
            self.connect_db()  # Reconnect on error
    
    def save_command(self, data):
        """Save actuator command to database"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO actuator_commands 
                    (time, command_id, greenhouse_id, actuator_id, device, action, 
                     value, reason, source, priority)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data.get('timestamp'),
                    data.get('command_id'),
                    data.get('greenhouse_id'),
                    data.get('actuator_id'),
                    data.get('device'),
                    data.get('action'),
                    data.get('value'),
                    data.get('reason'),
                    data.get('source'),
                    data.get('priority')
                ))
            logger.debug(f"Saved command: {data.get('command_id')} - {data.get('device')}")
        except psycopg2.Error as e:
            logger.error(f"Failed to save command: {e}")
            self.connect_db()
    
    def save_status(self, data):
        """Save actuator status to database"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO actuator_status 
                    (time, command_id, actuator_id, greenhouse_id, device, status, 
                     actual_value, execution_time_sec, error)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data.get('timestamp'),
                    data.get('command_id'),
                    data.get('actuator_id'),
                    data.get('greenhouse_id'),
                    data.get('device'),
                    data.get('status'),
                    data.get('actual_value'),
                    data.get('execution_time_sec'),
                    data.get('error')
                ))
            logger.debug(f"Saved status: {data.get('actuator_id')} - {data.get('status')}")
        except psycopg2.Error as e:
            logger.error(f"Failed to save status: {e}")
            self.connect_db()
    
    def save_alert(self, data):
        """Save alert to database"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO alerts 
                    (time, alert_id, greenhouse_id, source, alert_type, severity, 
                     message, current_value, threshold, requires_action, auto_resolved)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data.get('timestamp'),
                    data.get('alert_id', f"alert_{int(datetime.now().timestamp())}"),
                    data.get('greenhouse_id'),
                    data.get('source'),
                    data.get('alert_type'),
                    data.get('severity'),
                    data.get('message'),
                    data.get('current_value'),
                    data.get('threshold'),
                    data.get('requires_action', False),
                    data.get('auto_resolved', False)
                ))
            logger.info(f"Saved alert: {data.get('alert_type')} - {data.get('severity')}")
        except psycopg2.Error as e:
            logger.error(f"Failed to save alert: {e}")
            self.connect_db()
    
    def process_message(self, topic, data):
        """Route message to appropriate handler based on topic"""
        if topic == 'greenhouse-readings':
            self.save_sensor_reading(data)
        elif topic == 'greenhouse-commands':
            self.save_command(data)
        elif topic == 'greenhouse-status':
            self.save_status(data)
        elif topic == 'greenhouse-alerts':
            self.save_alert(data)
        else:
            logger.warning(f"Unknown topic: {topic}")
    
    def print_stats(self):
        """Print processing statistics"""
        elapsed = (datetime.now() - self.last_stats_time).total_seconds()
        if elapsed >= 60:  # Every minute
            rate = self.messages_processed / elapsed if elapsed > 0 else 0
            logger.info(f"Stats: {self.messages_processed} messages processed ({rate:.1f}/sec)")
            self.messages_processed = 0
            self.last_stats_time = datetime.now()
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting database consumer...")
        logger.info(f"Consuming from topics: {', '.join(self.topics)}")
        
        try:
            for message in self.consumer:
                try:
                    # Process the message
                    self.process_message(message.topic, message.value)
                    self.messages_processed += 1
                    
                    # Print stats periodically
                    self.print_stats()
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        except Exception as e:
            logger.error(f"Fatal error in consumer loop: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up...")
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
        
        logger.info("Shutdown complete")

if __name__ == "__main__":
    consumer = DatabaseConsumer()
    consumer.run()