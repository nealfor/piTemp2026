# web_server.py - Complete web server for Pi #4 (greenWeb)
# Serves dashboard and provides API endpoints

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Greenhouse Web Dashboard",
    description="Web interface for greenhouse monitoring",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files (CSS, JS, images)
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the main dashboard"""
    try:
        with open("greenhouse_dashboard.html", "r") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Dashboard not found")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "web-dashboard"}

@app.get("/api/config")
async def get_config():
    """Return API configuration for dashboard"""
    return {
        "api_base": os.getenv("API_BASE", "http://localhost:8000"),
        "greenhouse_id": os.getenv("GREENHOUSE_ID", "default_gh"),
        "refresh_interval": int(os.getenv("REFRESH_INTERVAL", "30000"))
    }

if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", "8080"))
    
    logger.info(f"Starting web dashboard on {host}:{port}")
    logger.info("Access dashboard at: http://localhost:8080")
    
    uvicorn.run(app, host=host, port=port)