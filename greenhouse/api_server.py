
# api_server.py - Runs on Pi #3 (greenKafka)
# REST API for querying greenhouse data from TimescaleDB

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Greenhouse API",
    description="REST API for greenhouse monitoring data",
    version="1.0.0"
)

# Enable CORS for web dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your web dashboard URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'greenhouse'),
    'user': os.getenv('DB_USER', 'greenhouse'),
    'password': os.getenv('DB_PASSWORD', 'greenhouse123')
}

# Pydantic models for API responses
class SensorReading(BaseModel):
    time: datetime
    sensor_id: str
    greenhouse_id: str
    sensor_temp_f: Optional[float]
    sensor_humidity: Optional[float]
    soil_moisture: Optional[float]
    light_level: Optional[int]
    web_temp_f: Optional[float]
    weather_description: Optional[str]

class ActuatorCommand(BaseModel):
    time: datetime
    command_id: str
    greenhouse_id: str
    actuator_id: Optional[str]
    device: str
    action: str
    value: Optional[float]
    reason: Optional[str]
    source: Optional[str]
    priority: Optional[str]

class ActuatorStatus(BaseModel):
    time: datetime
    command_id: Optional[str]
    actuator_id: str
    greenhouse_id: str
    device: str
    status: str
    actual_value: Optional[float]
    execution_time_sec: Optional[float]
    error: Optional[str]

class Alert(BaseModel):
    time: datetime
    alert_id: str
    greenhouse_id: str
    source: str
    alert_type: str
    severity: str
    message: Optional[str]
    current_value: Optional[float]
    threshold: Optional[float]
    requires_action: Optional[bool]
    auto_resolved: Optional[bool]

class Stats(BaseModel):
    avg_temp: Optional[float]
    min_temp: Optional[float]
    max_temp: Optional[float]
    avg_humidity: Optional[float]
    reading_count: int

def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/")
def root():
    """API root endpoint"""
    return {
        "name": "Greenhouse API",
        "version": "1.0.0",
        "endpoints": {
            "readings": "/api/readings",
            "latest": "/api/readings/latest",
            "commands": "/api/commands",
            "status": "/api/status",
            "alerts": "/api/alerts",
            "stats": "/api/stats"
        }
    }

@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

# ==================== SENSOR READINGS ====================

@app.get("/api/readings/latest", response_model=List[SensorReading])
def get_latest_readings(
    greenhouse_id: Optional[str] = None,
    limit: int = Query(10, ge=1, le=100)
):
    """
    Get the latest sensor readings
    
    - **greenhouse_id**: Filter by greenhouse (optional)
    - **limit**: Number of readings to return (1-100)
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if greenhouse_id:
                cur.execute("""
                    SELECT * FROM sensor_readings 
                    WHERE greenhouse_id = %s
                    ORDER BY time DESC 
                    LIMIT %s
                """, (greenhouse_id, limit))
            else:
                cur.execute("""
                    SELECT * FROM sensor_readings 
                    ORDER BY time DESC 
                    LIMIT %s
                """, (limit,))
            
            results = cur.fetchall()
            return results
    finally:
        conn.close()

@app.get("/api/readings", response_model=List[SensorReading])
def get_readings(
    greenhouse_id: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    hours: Optional[int] = Query(None, ge=1, le=720),  # Max 30 days
    limit: int = Query(1000, ge=1, le=10000)
):
    """
    Get sensor readings with flexible time filtering
    
    - **greenhouse_id**: Filter by greenhouse (optional)
    - **start_time**: Start of time range (ISO 8601 format)
    - **end_time**: End of time range (ISO 8601 format)
    - **hours**: Get last N hours (alternative to start/end)
    - **limit**: Maximum number of readings (1-10000)
    
    Examples:
    - Last 24 hours: `/api/readings?hours=24`
    - Specific range: `/api/readings?start_time=2024-02-01T00:00:00Z&end_time=2024-02-02T00:00:00Z`
    - By greenhouse: `/api/readings?greenhouse_id=greenhouse_01&hours=12`
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Build query based on parameters
            conditions = []
            params = []
            
            if greenhouse_id:
                conditions.append("greenhouse_id = %s")
                params.append(greenhouse_id)
            
            # Time filtering
            if hours:
                conditions.append("time > NOW() - INTERVAL '%s hours'")
                params.append(hours)
            elif start_time and end_time:
                conditions.append("time BETWEEN %s AND %s")
                params.extend([start_time, end_time])
            elif start_time:
                conditions.append("time >= %s")
                params.append(start_time)
            elif end_time:
                conditions.append("time <= %s")
                params.append(end_time)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            query = f"""
                SELECT * FROM sensor_readings 
                WHERE {where_clause}
                ORDER BY time DESC 
                LIMIT %s
            """
            params.append(limit)
            
            cur.execute(query, params)
            results = cur.fetchall()
            return results
    finally:
        conn.close()

@app.get("/api/readings/current/{greenhouse_id}", response_model=SensorReading)
def get_current_reading(greenhouse_id: str):
    """
    Get the most recent reading for a specific greenhouse
    
    - **greenhouse_id**: Greenhouse identifier
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM sensor_readings 
                WHERE greenhouse_id = %s
                ORDER BY time DESC 
                LIMIT 1
            """, (greenhouse_id,))
            
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="No readings found for this greenhouse")
            return result
    finally:
        conn.close()

# ==================== STATISTICS ====================

@app.get("/api/stats", response_model=Stats)
def get_stats(
    greenhouse_id: Optional[str] = None,
    hours: int = Query(24, ge=1, le=720)
):
    """
    Get temperature and humidity statistics
    
    - **greenhouse_id**: Filter by greenhouse (optional)
    - **hours**: Time window for statistics (1-720 hours)
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if greenhouse_id:
                cur.execute("""
                    SELECT 
                        AVG(sensor_temp_f) as avg_temp,
                        MIN(sensor_temp_f) as min_temp,
                        MAX(sensor_temp_f) as max_temp,
                        AVG(sensor_humidity) as avg_humidity,
                        COUNT(*) as reading_count
                    FROM sensor_readings 
                    WHERE greenhouse_id = %s 
                      AND time > NOW() - INTERVAL '%s hours'
                """, (greenhouse_id, hours))
            else:
                cur.execute("""
                    SELECT 
                        AVG(sensor_temp_f) as avg_temp,
                        MIN(sensor_temp_f) as min_temp,
                        MAX(sensor_temp_f) as max_temp,
                        AVG(sensor_humidity) as avg_humidity,
                        COUNT(*) as reading_count
                    FROM sensor_readings 
                    WHERE time > NOW() - INTERVAL '%s hours'
                """, (hours,))
            
            result = cur.fetchone()
            return result if result else Stats(reading_count=0)
    finally:
        conn.close()

@app.get("/api/stats/hourly")
def get_hourly_stats(
    greenhouse_id: Optional[str] = None,
    hours: int = Query(24, ge=1, le=720)
):
    """
    Get hourly aggregated statistics (uses TimescaleDB continuous aggregate)
    
    - **greenhouse_id**: Filter by greenhouse (optional)
    - **hours**: Number of hours to retrieve (1-720)
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if greenhouse_id:
                cur.execute("""
                    SELECT * FROM sensor_readings_hourly
                    WHERE greenhouse_id = %s
                      AND hour > NOW() - INTERVAL '%s hours'
                    ORDER BY hour DESC
                """, (greenhouse_id, hours))
            else:
                cur.execute("""
                    SELECT * FROM sensor_readings_hourly
                    WHERE hour > NOW() - INTERVAL '%s hours'
                    ORDER BY hour DESC
                """, (hours,))
            
            results = cur.fetchall()
            return results
    finally:
        conn.close()

# ==================== COMMANDS ====================

@app.get("/api/commands", response_model=List[ActuatorCommand])
def get_commands(
    greenhouse_id: Optional[str] = None,
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(100, ge=1, le=1000)
):
    """
    Get actuator commands history
    
    - **greenhouse_id**: Filter by greenhouse (optional)
    - **hours**: Time window (1-168 hours)
    - **limit**: Maximum results (1-1000)
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if greenhouse_id:
                cur.execute("""
                    SELECT * FROM actuator_commands 
                    WHERE greenhouse_id = %s 
                      AND time > NOW() - INTERVAL '%s hours'
                    ORDER BY time DESC 
                    LIMIT %s
                """, (greenhouse_id, hours, limit))
            else:
                cur.execute("""
                    SELECT * FROM actuator_commands 
                    WHERE time > NOW() - INTERVAL '%s hours'
                    ORDER BY time DESC 
                    LIMIT %s
                """, (hours, limit))
            
            results = cur.fetchall()
            return results
    finally:
        conn.close()

# ==================== STATUS ====================

@app.get("/api/status", response_model=List[ActuatorStatus])
def get_status(
    greenhouse_id: Optional[str] = None,
    actuator_id: Optional[str] = None,
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(100, ge=1, le=1000)
):
    """
    Get actuator status history
    
    - **greenhouse_id**: Filter by greenhouse (optional)
    - **actuator_id**: Filter by specific actuator (optional)
    - **hours**: Time window (1-168 hours)
    - **limit**: Maximum results (1-1000)
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            conditions = ["time > NOW() - INTERVAL '%s hours'"]
            params = [hours]
            
            if greenhouse_id:
                conditions.append("greenhouse_id = %s")
                params.append(greenhouse_id)
            
            if actuator_id:
                conditions.append("actuator_id = %s")
                params.append(actuator_id)
            
            where_clause = " AND ".join(conditions)
            params.append(limit)
            
            query = f"""
                SELECT * FROM actuator_status 
                WHERE {where_clause}
                ORDER BY time DESC 
                LIMIT %s
            """
            
            cur.execute(query, params)
            results = cur.fetchall()
            return results
    finally:
        conn.close()

@app.get("/api/status/latest/{actuator_id}", response_model=ActuatorStatus)
def get_latest_status(actuator_id: str):
    """
    Get the most recent status for a specific actuator
    
    - **actuator_id**: Actuator identifier
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM actuator_status 
                WHERE actuator_id = %s
                ORDER BY time DESC 
                LIMIT 1
            """, (actuator_id,))
            
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="No status found for this actuator")
            return result
    finally:
        conn.close()

# ==================== ALERTS ====================

@app.get("/api/alerts", response_model=List[Alert])
def get_alerts(
    greenhouse_id: Optional[str] = None,
    severity: Optional[str] = Query(None, regex="^(low|medium|high|critical)$"),
    hours: int = Query(24, ge=1, le=720),
    limit: int = Query(100, ge=1, le=1000)
):
    """
    Get alerts history
    
    - **greenhouse_id**: Filter by greenhouse (optional)
    - **severity**: Filter by severity (low, medium, high, critical)
    - **hours**: Time window (1-720 hours)
    - **limit**: Maximum results (1-1000)
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            conditions = ["time > NOW() - INTERVAL '%s hours'"]
            params = [hours]
            
            if greenhouse_id:
                conditions.append("greenhouse_id = %s")
                params.append(greenhouse_id)
            
            if severity:
                conditions.append("severity = %s")
                params.append(severity)
            
            where_clause = " AND ".join(conditions)
            params.append(limit)
            
            query = f"""
                SELECT * FROM alerts 
                WHERE {where_clause}
                ORDER BY time DESC 
                LIMIT %s
            """
            
            cur.execute(query, params)
            results = cur.fetchall()
            return results
    finally:
        conn.close()

# ==================== UTILITY ====================

@app.get("/api/greenhouses")
def get_greenhouses():
    """
    Get list of all greenhouses that have sent data
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT greenhouse_id,
                       COUNT(*) as reading_count,
                       MAX(time) as last_reading
                FROM sensor_readings 
                GROUP BY greenhouse_id
                ORDER BY greenhouse_id
            """)
            results = cur.fetchall()
            return results
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', 8000))
    
    logger.info(f"Starting API server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)