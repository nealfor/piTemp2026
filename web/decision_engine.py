# decision_engine.py - Runs on Pi #4 (greenWeb)
# Analyzes sensor data and sends automation commands

import json
import logging
import os
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone
import time
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DecisionEngine:
    """
    Greenhouse automation decision engine
    Analyzes sensor readings and sends control commands
    """
    
    def __init__(self):
        # Configuration
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', '[kafka_IP]:9092').split(',')
        self.greenhouse_id = os.getenv('GREENHOUSE_ID', '[greenhouse_id]')
        
        # Target temperatures
        self.target_temp = float(os.getenv('TARGET_TEMP', '75'))  # Ideal temperature
        self.temp_tolerance = float(os.getenv('TEMP_TOLERANCE', '3'))  # +/- tolerance
        
        # Thresholds
        self.fan_on_temp = float(os.getenv('FAN_ON_TEMP', '80'))  # Turn on fan
        self.fan_off_temp = float(os.getenv('FAN_OFF_TEMP', '76'))  # Turn off fan (hysteresis)
        self.high_humidity = float(os.getenv('HIGH_HUMIDITY', '80'))  # High humidity threshold
        
        # Emergency thresholds
        self.emergency_high_temp = float(os.getenv('EMERGENCY_HIGH_TEMP', '95'))
        self.emergency_low_temp = float(os.getenv('EMERGENCY_LOW_TEMP', '40'))
        
        # State tracking
        self.last_reading = None
        self.last_command_time = {}
        self.current_fan_state = False
        self.current_window_position = 0
        
        # Connect to Kafka
        self.consumer = None
        self.producer = None
        self.connect_kafka()
    
    def connect_kafka(self):
        """Connect to Kafka broker"""
        try:
            self.consumer = KafkaConsumer(
                'greenhouse-readings',
                bootstrap_servers=self.kafka_brokers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='decision-engine',
                auto_offset_reset='latest'
            )
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            logger.info(f"Connected to Kafka at {self.kafka_brokers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def is_night_mode(self):
        """Check if currently in night mode (10 PM - 6 AM)"""
        current_hour = datetime.now().hour
        return current_hour >= 22 or current_hour < 6
    
    def calculate_window_position(self, temp, humidity):
        """
        Calculate optimal window position (0-100%)
        Based on temperature, humidity, and time of day
        """
        # Emergency overrides
        if temp >= self.emergency_high_temp:
            return 100  # Full open
        
        if temp <= self.emergency_low_temp:
            return 0  # Full close
        
        # High humidity forces ventilation
        if humidity >= self.high_humidity:
            return max(75, self.current_window_position)  # At least 75% open
        
        # Calculate based on temperature difference from target
        temp_diff = temp - self.target_temp
        
        if temp_diff <= -self.temp_tolerance:
            # Too cold - close windows
            position = 0
        elif temp_diff >= self.temp_tolerance:
            # Too hot - open windows proportionally
            # Scale: +3°F = 50%, +6°F = 75%, +10°F = 100%
            position = min(100, int((temp_diff / 10) * 100))
        else:
            # Within tolerance - maintain slight ventilation
            position = 25
        
        # Night mode - reduce ventilation by 50%
        if self.is_night_mode():
            position = int(position * 0.5)
            logger.info("Night mode active - reducing ventilation")
        
        return position
    
    def should_fan_be_on(self, temp, humidity, window_position):
        """
        Determine if exhaust fan should be on
        Uses hysteresis to prevent rapid cycling
        """
        # Emergency high temp
        if temp >= self.emergency_high_temp:
            return True
        
        # High humidity
#        if humidity >= self.high_humidity:
#            return True
        
        # Temperature-based control with hysteresis
        if temp >= self.fan_on_temp:
            return True
        
        if temp <= self.fan_off_temp:
            return False
        
        # Between thresholds - maintain current state (hysteresis)
        return self.current_fan_state
    
    def send_command(self, device, action, value, reason):
        """Send command to Kafka"""
        try:
            command_id = f"cmd_{int(datetime.now().timestamp() * 1000)}"
            
            command = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'command_id': command_id,
                'greenhouse_id': self.greenhouse_id,
                'device': device,
                'action': action,
                'value': value,
                'reason': reason,
                'source': 'automation',
                'priority': 'normal'
            }
            
            self.producer.send('greenhouse-commands', command)
            self.producer.flush()
            
            logger.info(f"Sent command: {device} {action} (value={value}) - {reason}")
            
            # Track last command time
            self.last_command_time[device] = datetime.now()
            
        except Exception as e:
            logger.error(f"Failed to send command: {e}")
    
    def process_reading(self, reading):
        """Process sensor reading and make decisions"""
        try:
            # Extract data
            temp = reading.get('sensor_temp_f')
            humidity = reading.get('sensor_humidity')
            
            if temp is None or humidity is None:
                logger.warning("Missing temperature or humidity data")
                return
            
            logger.info(f"Processing: Temp={temp}°F, Humidity={humidity}%")
            
            # Store last reading
            self.last_reading = reading
            
            # Calculate optimal window position
            target_window_position = self.calculate_window_position(temp, humidity)
            
            # Check if windows need adjustment (only if change > 10%)
            position_diff = abs(target_window_position - self.current_window_position)
#            if position_diff >= 10:
#                self.send_command(
#                    device='louver_windows',
#                    action='set_position',
#                    value=target_window_position,
#                    reason=f"temp={temp}°F, humidity={humidity}%"
#                )
#                self.current_window_position = target_window_position
            
            # Determine fan state
            should_fan_run = self.should_fan_be_on(temp, humidity, target_window_position)
            
            # Send fan command if state needs to change
            if should_fan_run != self.current_fan_state:
                action = 'turn_on' if should_fan_run else 'turn_off'
                reason = self._get_fan_reason(temp, humidity, should_fan_run)
                
                self.send_command(
                    device='exhaust_fan',
                    action=action,
                    value=1 if should_fan_run else 0,
                    reason=reason
                )
                
                self.current_fan_state = should_fan_run
            
            # Log current state
            logger.info(f"State: Windows={self.current_window_position}%, Fan={'ON' if self.current_fan_state else 'OFF'}")
            
        except Exception as e:
            logger.error(f"Error processing reading: {e}")
    
    def _get_fan_reason(self, temp, humidity, turning_on):
        """Get human-readable reason for fan decision"""
        if turning_on:
            if temp >= self.emergency_high_temp:
                return f"emergency_high_temp_{temp}°F"
            elif humidity >= self.high_humidity:
                return f"high_humidity_{humidity}%"
            elif temp >= self.fan_on_temp:
                return f"temp_high_{temp}°F"
            else:
                return f"ventilation_needed"
        else:
            return f"temp_normal_{temp}°F"
    
    def run(self):
        """Main loop - process sensor readings"""
        logger.info("Decision engine started")
        logger.info(f"Target temperature: {self.target_temp}°F (+/- {self.temp_tolerance}°F)")
        logger.info(f"Fan thresholds: ON at {self.fan_on_temp}°F, OFF at {self.fan_off_temp}°F")
        logger.info(f"Listening to: {self.kafka_brokers}")
        
        try:
            for message in self.consumer:
                try:
                    reading = message.value
                    
                    # Only process readings for our greenhouse
                    if reading.get('greenhouse_id') != self.greenhouse_id:
                        continue
                    
                    self.process_reading(reading)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down decision engine...")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up...")
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")
        
        logger.info("Shutdown complete")

if __name__ == "__main__":
    engine = DecisionEngine()
    engine.run()
