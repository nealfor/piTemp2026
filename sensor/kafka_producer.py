# kafka_producer.py - Runs on Pi #1 (piProducer.local)
import Adafruit_DHT
import requests
import json
import logging
import time
import os
from datetime import datetime
from dataclasses import dataclass, asdict
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SensorReading:
    timestamp: str
    sensor_temp_f: float
    sensor_humidity: float
    web_temp_f: float
    weather_description: str
    sensor_id: str = "greenhouse_01_sensor"
    greenhouse_id: str = "greenhouse_01"


    
    def to_dict(self):
        return asdict(self)
    
    def to_json(self):
        """Convert to JSON string for Kafka"""
        return json.dumps(self.to_dict())

class GreenhouseKafkaProducer:
    def __init__(self):
        # Kafka broker configuration (Pi #2)
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', 'raspberrypi2.local:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'greenhouse-readings')
        
        # Sensor configuration
        self.dht_sensor = Adafruit_DHT.DHT22
        self.dht_pin = 18
        
        # Weather API
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.latitude = os.getenv('LATITUDE')
        self.longitude = os.getenv('LONGITUDE')
        
        # Reading interval
        self.interval = int(os.getenv('READING_INTERVAL', 300))  # 5 minutes default
        
        # Initialize Kafka producer
        self.producer = None
        self.connect_kafka()
    
    def connect_kafka(self):
        """Connect to Kafka broker with retry logic"""
        max_retries = 5
        retry_delay = 10
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_brokers.split(','),
                    value_serializer=lambda v: v.encode('utf-8'),
                    # Reliability settings
                    acks='all',  # Wait for all replicas to acknowledge
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                    # Timeout settings
                    request_timeout_ms=30000,
                    # Compression (optional, saves bandwidth)
                    compression_type='gzip',
                    # Batching for efficiency (optional)
                    linger_ms=100,
                    batch_size=16384
                )
                logger.info(f"Connected to Kafka broker at {self.kafka_brokers}"
)
                return
                
            except NoBrokersAvailable as e:
                logger.warning(f"Kafka connection attempt {attempt + 1}/{max_retries} failed: No brokers available")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to Kafka after all retries")
                    raise
                    
            except Exception as e:
                logger.error(f"Unexpected error connecting to Kafka: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise
    
    def read_sensor(self):
        """Read DHT22 sensor data"""
        try:
            humidity, temperature_c = Adafruit_DHT.read_retry(self.dht_sensor, self.dht_pin)
            
            if humidity is None or temperature_c is None:
                logger.error("Failed to read from DHT sensor")
                return None
            
            # Convert to Fahrenheit
            temperature_f = (temperature_c * 1.8) + 32
            
            logger.info(f"Sensor read: {temperature_f:.1f}°F, {humidity:.1f}%")
            return temperature_f, humidity
            
        except Exception as e:
            logger.error(f"Sensor reading error: {e}")
            return None
    
    def get_weather_data(self):
        """Fetch weather data from OpenWeather API"""
        api_url = (
            f"https://api.openweathermap.org/data/2.5/weather?"
            f"lat={self.latitude}&lon={self.longitude}"
            f"&appid={self.api_key}&units=imperial"
        )
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(api_url, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                temp = data["main"]["temp"]
                description = data["weather"][0]["description"]
                
                logger.info(f"Weather API: {temp:.1f}°F, {description}")
                return temp, description
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Weather API attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    logger.error("Weather API failed after all retries")
                    return None, "unavailable"
    
    def publish_reading(self, reading: SensorReading):
        """Publish reading to Kafka topic"""
        try:
            # Send to Kafka
            future = self.producer.send(
                self.topic,
                value=reading.to_json(),
                key=reading.sensor_id.encode('utf-8')  # Partition by sensor_id
            )
            
            # Wait for acknowledgment (with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Published reading to Kafka - "
                f"Topic: {record_metadata.topic}, "
                f"Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}, "
                f"Temp: {reading.sensor_temp_f:.1f}°F"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to publish to Kafka: {e}")
            # Try to reconnect
            try:
                self.connect_kafka()
            except Exception as reconnect_error:
                logger.error(f"Reconnection failed: {reconnect_error}")
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error publishing to Kafka: {e}")
            return False
    
    def run(self):
        """Main producer loop"""
        logger.info("Starting greenhouse Kafka producer...")
        logger.info(f"Kafka broker: {self.kafka_brokers}")
        logger.info(f"Topic: {self.topic}")
        logger.info(f"Reading interval: {self.interval} seconds")
        
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        while True:
            try:
                # Read sensor
                sensor_data = self.read_sensor()
                if sensor_data is None:
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(f"Sensor failed {consecutive_failures} times in a row")
                    time.sleep(60)  # Wait a minute before retry
                    continue
                
                temp_f, humidity = sensor_data
                consecutive_failures = 0  # Reset on successful read
                
                # Get weather data
                web_temp, weather_desc = self.get_weather_data()
                if web_temp is None:
                    web_temp = 0.0  # Use default if API fails
                
                # Create reading object
                reading = SensorReading(
                    timestamp=datetime.now().isoformat(),
                    sensor_temp_f=round(temp_f, 1),
                    sensor_humidity=round(humidity, 1),
                    web_temp_f=round(web_temp, 1),
                    weather_description=weather_desc,
                    sensor_id="greenhouse_01_sensor",
                    greenhouse_id="greenhouse_01"
                )
                
                # Publish to Kafka
                success = self.publish_reading(reading)
                
                if not success:
                    consecutive_failures += 1
                    logger.warning(f"Failed to publish ({consecutive_failures} consecutive failures)")
                
                # Wait for next reading
                logger.info(f"Sleeping for {self.interval} seconds...")
                time.sleep(self.interval)
                
            except KeyboardInterrupt:
                logger.info("Shutting down producer...")
                break
                
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                consecutive_failures += 1
                time.sleep(60)
        
        # Cleanup
        if self.producer:
            logger.info("Flushing remaining messages...")
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    producer = GreenhouseKafkaProducer()
    producer.run()
