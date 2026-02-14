#!/usr/bin/env python3
"""
Test script for Kafka producer - verifies all components work
Run this before deploying the full producer
"""

import os
import sys
from dotenv import load_dotenv

# Color codes for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

def print_status(test_name, passed, message=""):
    status = f"{GREEN}✓ PASS{RESET}" if passed else f"{RED}✗ FAIL{RESET}"
    print(f"{status} - {test_name}")
    if message:
        print(f"      {message}")

def test_environment():
    """Test if .env file exists and has required variables"""
    print("\n=== Testing Environment Configuration ===")
    
    load_dotenv()
    
    required_vars = [
        'KAFKA_BROKERS',
        'KAFKA_TOPIC',
        'OPENWEATHER_API_KEY',
        'LATITUDE',
        'LONGITUDE'
    ]
    
    all_present = True
    for var in required_vars:
        value = os.getenv(var)
        if value and value != f'your_{var.lower()}_here':
            print_status(f"Environment variable {var}", True, f"Value: {value[:2
0]}...")
        else:
            print_status(f"Environment variable {var}", False, "Missing or not c
onfigured")
            all_present = False
    
    return all_present

def test_kafka_connection():
    """Test connection to Kafka broker"""
    print("\n=== Testing Kafka Connection ===")
    
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
        
        load_dotenv()
        brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092')
        
        print(f"Attempting to connect to: {brokers}")
        
        producer = KafkaProducer(
            bootstrap_servers=brokers.split(','),
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=5000
        )
        
        # Try to get metadata to verify connection
        #metadata = producer.bootstrap_connected()
        if producer.bootstrap_connected():
           print_status("Kafka broker connection", True, f"Connected to {brokers
}")
        #print_status("Available topics", True, f"Found {len(metadata.topics)} t
opics")
        
        producer.close()
        return True
        
    except NoBrokersAvailable:
        print_status("Kafka broker connection", False, "No brokers available - c
heck if Kafka is running")
        return False
    except ImportError:
        print_status("Kafka library", False, "kafka-python not installed. Run: p
ip3 install kafka-python")
        return False
    except Exception as e:
        print_status("Kafka broker connection", False, str(e))
        return False

def test_sensor_library():
    """Test if DHT sensor library is available"""
    print("\n=== Testing Sensor Library ===")
    
    try:
        import Adafruit_DHT
        print_status("Adafruit_DHT library", True, f"Version available")
        return True
    except ImportError:
        print_status("Adafruit_DHT library", False, "Not installed. Run: pip3 in
stall Adafruit-DHT")
        return False

def test_weather_api():
    """Test OpenWeather API connection"""
    print("\n=== Testing Weather API ===")
    
    try:
        import requests
        load_dotenv()
        
        api_key = os.getenv('OPENWEATHER_API_KEY')
        lat = os.getenv('LATITUDE')
        lon = os.getenv('LONGITUDE')
        
        if not api_key or api_key == 'your_api_key_here':
            print_status("Weather API key", False, "API key not configured")
            return False
        
        if not lat or lat == 'your_latitude':
            print_status("Location coordinates", False, "Latitude/longitude not 
configured")
            return False
        
        api_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lo
n={lon}&appid={api_key}&units=imperial"
        
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        temp = data["main"]["temp"]
        desc = data["weather"][0]["description"]
        
        print_status("Weather API connection", True, f"Current: {temp}°F, {desc}
")
        return True
        
    except requests.exceptions.RequestException as e:
        print_status("Weather API connection", False, str(e))
        return False
    except ImportError:
        print_status("Requests library", False, "Not installed. Run: pip3 instal
l requests")
        return False

def test_send_message():
    """Test sending a message to Kafka"""
    print("\n=== Testing Message Publishing ===")
    
    try:
        from kafka import KafkaProducer
        import json
        from datetime import datetime
        
        load_dotenv()
        brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092')
        topic = os.getenv('KAFKA_TOPIC', 'greenhouse-readings')
        
        producer = KafkaProducer(
            bootstrap_servers=brokers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000
        )
        
        # Create test message
        test_message = {
            "timestamp": datetime.now().isoformat(),
            "sensor_temp_f": 72.5,
            "sensor_humidity": 45.0,
            "web_temp_f": 70.0,
            "weather_description": "test message",
            "sensor_id": "test_sensor"
        }
        
        # Send message
        future = producer.send(topic, test_message)
        record_metadata = future.get(timeout=10)
        
        print_status("Send test message", True, 
                    f"Topic: {topic}, Partition: {record_metadata.partition}, Of
fset: {record_metadata.offset}")
        
        producer.flush()
        producer.close()
        return True
        
    except Exception as e:
        print_status("Send test message", False, str(e))
        return False

def main():
    print(f"\n{YELLOW}{'='*60}{RESET}")
    print(f"{YELLOW}Greenhouse Kafka Producer - System Test{RESET}")
    print(f"{YELLOW}{'='*60}{RESET}")
    
    tests = [
        ("Environment Configuration", test_environment),
        ("Sensor Library", test_sensor_library),
        ("Weather API", test_weather_api),
        ("Kafka Connection", test_kafka_connection),
        ("Message Publishing", test_send_message),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print_status(test_name, False, f"Unexpected error: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n{YELLOW}{'='*60}{RESET}")
    print(f"{YELLOW}Test Summary{RESET}")
    print(f"{YELLOW}{'='*60}{RESET}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = f"{GREEN}PASS{RESET}" if result else f"{RED}FAIL{RESET}"
        print(f"{status} - {test_name}")
    
    print(f"\n{passed}/{total} tests passed")
    
    if passed == total:
        print(f"\n{GREEN}All tests passed! Your system is ready.{RESET}")
        print(f"\nYou can now run the producer with:")
        print(f"  python3 kafka_producer.py")
        return 0
    else:
        print(f"\n{RED}Some tests failed. Please fix the issues above.{RESET}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
