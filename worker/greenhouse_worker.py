# actuator_controller_v2.py - Runs on Pi #2 (piActuator)
# Controls louver windows (servos) and exhaust fan (TP-Link switch)

import json
import logging
import os
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone
import asyncio
from kasa import Discover, Credentials
from dotenv import load_dotenv

# Servo control (uncomment when you have servos connected)
try:
    import RPi.GPIO as GPIO
    SERVOS_AVAILABLE = True
except ImportError:
    logger.warning("RPi.GPIO not available - servo control disabled")
    SERVOS_AVAILABLE = False

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ActuatorController:
    """
    Controls greenhouse actuators:
    - Louver windows via servo motors
    - Exhaust fan via TP-Link Kasa smart switch
    """
    
    def __init__(self):
        # Configuration
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', 'default_gh:9092').split(',')
        self.greenhouse_id = os.getenv('GREENHOUSE_ID', 'greenhouse_id')
        self.device_id = f"{self.greenhouse_id}_actuator"
        
        # TP-Link Switch configuration
        self.switch_ip = os.getenv('SWITCH_IP', '0.0.0.0')  #change to your switch IP
        self.switch = None
        
        # Servo configuration (GPIO pins)
        self.servo_pins = [17, 18, 27, 22]
        self.servos_initialized = False
        
        # State tracking
        self.current_window_position = 0
        self.current_fan_state = False
        
        # Event loop
        self.loop = None
        
        # Connect to Kafka
        self.consumer = None
        self.producer = None
        self.connect_kafka()
        
        # Initialize hardware
        if SERVOS_AVAILABLE:
            self.init_servos()
      
    
    def connect_kafka(self):
        """Connect to Kafka broker"""
        try:
            self.consumer = KafkaConsumer(
                'greenhouse-commands',
                bootstrap_servers=self.kafka_brokers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='actuator-controller',
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
     
    async def init_switch(self):
       """Initialize TP-Link switch connection"""
       try:
           from kasa import Discover, Credentials
        
           # Get credentials from environment
           username = os.getenv('KASA_USERNAME')
           password = os.getenv('KASA_PASSWORD')
        
           if username and password:
               credentials = Credentials(username, password)
               self.switch = await Discover.discover_single(
                   self.switch_ip,
                   credentials=credentials
               )
           else:
               # Try without credentials (older devices)
               self.switch = await Discover.discover_single(self.switch_ip)
        
           await self.switch.update()
           self.current_fan_state = self.switch.is_on
        
           logger.info(f"Connected to TP-Link switch at {self.switch_ip}")
           logger.info(f"Fan current state: {'ON' if self.current_fan_state else 'OFF'}")
       except Exception as e:
           logger.error(f"Failed to connect to TP-Link switch: {e}")
           self.switch = None    

    def init_servos(self):
        """Initialize servo motors"""
        try:
            GPIO.setmode(GPIO.BCM)
            GPIO.setwarnings(False)
            
            for pin in self.servo_pins:
                GPIO.setup(pin, GPIO.OUT)
            
            self.servos_initialized = True
            logger.info(f"Initialized {len(self.servo_pins)} servo motors")
        except Exception as e:
            logger.error(f"Failed to initialize servos: {e}")
            self.servos_initialized = False
    
    def set_servo_position(self, pin, position):
        """
        Set servo position (0-100%)
        Position maps to PWM duty cycle
        """
        if not self.servos_initialized:
            return False
        
        try:
            # Convert position (0-100) to duty cycle (2-12)
            # 0% = 2% duty (0 degrees), 100% = 12% duty (180 degrees)
            duty = 2 + (position / 100) * 10
            
            pwm = GPIO.PWM(pin, 50)  # 50Hz frequency
            pwm.start(duty)
            asyncio.sleep(0.5)  # Give servo time to move
            pwm.stop()
            
            return True
        except Exception as e:
            logger.error(f"Error setting servo position on pin {pin}: {e}")
            return False
    
    def control_windows(self, position):
        """
        Control louver windows (0-100%)
        Moves all servo motors to the same position
        """
        if not self.servos_initialized:
            logger.warning("Servos not available - simulating window control")
            self.current_window_position = position
            return True
        
        try:
            logger.info(f"Moving windows to {position}%...")
            
            success = True
            for pin in self.servo_pins:
                if not self.set_servo_position(pin, position):
                    success = False
            
            if success:
                self.current_window_position = position
                logger.info(f"Windows moved to {position}%")
            
            return success
            
        except Exception as e:
            logger.error(f"Error controlling windows: {e}")
            return False
    
    async def control_fan(self, turn_on):
        """Control exhaust fan via TP-Link switch"""
        if not self.switch:
            logger.error("TP-Link switch not available")
            return False
        
        try:
            await self.switch.update()
            
            if turn_on:
                await self.switch.turn_on()
                logger.info("Fan turned ON")
            else:
                await self.switch.turn_off()
                logger.info("Fan turned OFF")
            
            # Update state
            await self.switch.update()
            self.current_fan_state = self.switch.is_on
            
            return True
            
        except Exception as e:
            logger.error(f"Error controlling fan: {e}")
            return False
    
    def publish_status(self, command_id, device, status, actual_value, error=None):
        """Publish status update to Kafka"""
        try:
            status_message = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'command_id': command_id,
                'device_id': self.device_id,
                'greenhouse_id': self.greenhouse_id,
                'device': device,
                'status': status,
                'actual_value': actual_value,
                'error': error
            }
            
            self.producer.send('greenhouse-status', status_message)
            self.producer.flush()
            
            logger.debug(f"Published status: {device} - {status}")
            
        except Exception as e:
            logger.error(f"Failed to publish status: {e}")
    
    def process_command(self, command):
        """Process a command from Kafka"""
        try:
            # Check if command is for this greenhouse
            if command.get('greenhouse_id') != self.greenhouse_id:
                return
            
            command_id = command.get('command_id', 'unknown')
            device = command.get('device')
            action = command.get('action')
            value = command.get('value', 0)
            
            logger.info(f"Processing command: {device} {action} (value={value})")
            
            # Route to appropriate device
            if device == 'louver_windows':
                success = self.control_windows(int(value))
                status = 'completed' if success else 'failed'
                error = None if success else 'Failed to control servos'
                actual_value = self.current_window_position
                
            elif device == 'exhaust_fan':
                turn_on = (action == 'turn_on' or value > 0)
                success = asyncio.run(self.control_fan(turn_on))
                status = 'completed' if success else 'failed'
                error = None if success else 'Failed to control switch'
                actual_value = 1 if self.current_fan_state else 0
                
            else:
                logger.warning(f"Unknown device: {device}")
                status = 'failed'
                error = f"Unknown device: {device}"
                actual_value = 0
            
            # Publish status
            self.publish_status(command_id, device, status, actual_value, error)
            
        except Exception as e:
            logger.error(f"Error processing command: {e}")
            self.publish_status(
                command.get('command_id', 'unknown'),
                command.get('device', 'unknown'),
                'failed',
                0,
                str(e)
            )
    
    def run(self):
        """Main loop - listen for commands"""
        logger.info("Actuator controller started")
        logger.info(f"Device ID: {self.device_id}")
        logger.info(f"Controlling: louver_windows (servos) + exhaust_fan (TP-Link)")
        logger.info(f"Listening for commands on greenhouse-commands topic")
        
        # Create and run event loop
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Initialize switch in the event loop
        self.loop.run_until_complete(self.init_switch())
        
        # Publish initial status
        self.publish_status('startup', 'system', 'online', 0, None)
        
        try:
            for message in self.consumer:
                try:
                    command = message.value
                    # Process command in the event loop
                    self.loop.run_until_complete(self.process_command_async(command))
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down actuator controller...")
        finally:
            self.cleanup()
    
    async def process_command_async(self, command):
        """Async version of process_command"""
        try:
            # Check if command is for this greenhouse
            if command.get('greenhouse_id') != self.greenhouse_id:
                return
            
            command_id = command.get('command_id', 'unknown')
            device = command.get('device')
            action = command.get('action')
            value = command.get('value', 0)
            
            logger.info(f"Processing command: {device} {action} (value={value})")
            
            # Route to appropriate device
            if device == 'louver_windows':
                success = self.control_windows(int(value))
                status = 'completed' if success else 'failed'
                error = None if success else 'Failed to control servos'
                actual_value = self.current_window_position
                
            elif device == 'exhaust_fan':
                turn_on = (action == 'turn_on' or value > 0)
                success = await self.control_fan(turn_on)
                status = 'completed' if success else 'failed'
                error = None if success else 'Failed to control switch'
                actual_value = 1 if self.current_fan_state else 0
                
            else:
                logger.warning(f"Unknown device: {device}")
                status = 'failed'
                error = f"Unknown device: {device}"
                actual_value = 0
            
            # Publish status
            self.publish_status(command_id, device, status, actual_value, error)
            
        except Exception as e:
            logger.error(f"Error processing command: {e}")
            self.publish_status(
                command.get('command_id', 'unknown'),
                command.get('device', 'unknown'),
                'failed',
                0,
                str(e)
            )
    
    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up...")
        
        # Close windows and turn off fan
        if self.servos_initialized:
            logger.info("Closing windows...")
            self.control_windows(0)
        
        if self.switch and self.loop:
            logger.info("Turning off fan...")
            self.loop.run_until_complete(self.control_fan(False))
        
        # Close event loop
        if self.loop:
            self.loop.close()
        
        # Cleanup GPIO
        if SERVOS_AVAILABLE and self.servos_initialized:
            GPIO.cleanup()
            logger.info("GPIO cleaned up")
        
        # Close Kafka connections
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")
        
        logger.info("Shutdown complete")


if __name__ == "__main__":
    controller = ActuatorController()
    controller.run()
