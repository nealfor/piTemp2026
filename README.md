
# Greenhouse Automation System

Distributed IoT system for automated greenhouse climate control using Raspberry Pis and Kafka.

## System Overview

## Components

### 🌡️ Sensor Module (`/sensor`)
- Reads temperature, humidity, soil moisture
- Publishes to Kafka every 5 minutes
- Deploy to: Raspberry Pi in center of greenhouse

### 🪟 Actuator Module (`/actuator`)  
- Controls louver windows and exhaust fans
- Receives commands from decision engine
- Local safety override for emergencies
- Deploy to: Raspberry Pi near windows

### 📊 Broker Module (`/broker`)
- Kafka message broker
- TimescaleDB for data storage
- Data persistence layer
- Deploy to: Server/Pi offsite

### 🖥️ Web Module (`/web`)
- Decision engine (automation logic)
- Web dashboard for monitoring
- Manual control interface
- Deploy to: Server/Pi offsite (can be same as broker)

