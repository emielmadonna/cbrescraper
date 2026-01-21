from fastapi import FastAPI, WebSocket, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import subprocess
import os
import sys
import json
import logging
from typing import Optional, List
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load env from .env file
load_dotenv()

# Add parent directory to path to import crawler_app if needed, 
# though we might run it as a subprocess script.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

app = FastAPI()

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url}")
    try:
        response = await call_next(request)
        logger.info(f"Response status: {response.status_code}")
        return response
    except Exception as e:
        logger.error(f"Request failed: {request.method} {request.url} - Error: {e}")
        raise e

# Global state for the scraper process
scraper_process: Optional[subprocess.Popen] = None
active_websockets: List[WebSocket] = []

class ScrapeRequest(BaseModel):
    url: str
    pinecone_api_key: Optional[str] = None
    pinecone_env: Optional[str] = None
    pinecone_index: Optional[str] = None
    openai_api_key: Optional[str] = None
    headless: bool = False
    dry_run: bool = False
    mode: str = "auto"
    limit: Optional[int] = None

@app.get("/")
async def root():
    return {"status": "CBRE Scraper API is running"}

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_websockets.append(websocket)
    await websocket.send_text("📡 System: Log stream connected. Ready for next scrape.")
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except Exception:
        if websocket in active_websockets:
            active_websockets.remove(websocket)

async def broadcast_log(message: str):
    for ws in active_websockets:
        try:
            await ws.send_text(message)
        except:
            pass

async def run_scraper_subprocess(req: ScrapeRequest):
    global scraper_process
    
    # Construct environment variables
    env = os.environ.copy()
    if req.pinecone_api_key:
        env['PINECONE_API_KEY'] = req.pinecone_api_key
    if req.pinecone_env:
        env['PINECONE_ENV'] = req.pinecone_env
    if req.pinecone_index:
        env['PINECONE_INDEX'] = req.pinecone_index
    if req.openai_api_key:
        env['OPENAI_API_KEY'] = req.openai_api_key
    
    # Path to the script we want to run
    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../run_pipeline.py'))
    
    cmd = [sys.executable, "-u", script_path, "--url", req.url]
    if hasattr(req, 'mode') and req.mode:
        cmd.extend(["--mode", req.mode])
        
    if req.headless:
        cmd.append("--hide-browser")
        
    if req.dry_run:
        cmd.append("--dry-run")
        
    if req.limit:
        cmd.extend(["--limit", str(req.limit)])

    print(f"Starting scraper: {' '.join(cmd)}")
    await broadcast_log(f"Starting scraper for URL: {req.url} (Mode: {getattr(req, 'mode', 'auto')}, Test: {req.dry_run})")
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env
        )
        scraper_process = process # Allow stopping it

        # Stream output
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            
            decoded_line = line.decode().strip()
            if decoded_line:
                # Capture and broadcast
                await broadcast_log(decoded_line)
                print(f"Scraper: {decoded_line}")
                
                # Special Formatting for final property data
                if "--- DATA EXTRACTED ---" in decoded_line or "Extracted:" in decoded_line:
                    await broadcast_log("📊 [DATA SUMMARY] --------------------")
        
        rc = await process.wait()
        await broadcast_log(f"Scraper finished with exit code {rc}")
        
    except Exception as e:
        await broadcast_log(f"Error running scraper: {str(e)}")
    finally:
        scraper_process = None

# Extended Request Models
class PersonScrapeRequest(ScrapeRequest):
    mode: str = "person"

class PropertyScrapeRequest(ScrapeRequest):
    mode: str = "property"

@app.post("/api/scrape/person")
async def scrape_person(req: PersonScrapeRequest, background_tasks: BackgroundTasks):
    return await start_scrape_internal(req, background_tasks)

@app.post("/api/scrape/property")
async def scrape_property(req: PropertyScrapeRequest, background_tasks: BackgroundTasks):
    return await start_scrape_internal(req, background_tasks)

# Internal helper to reuse logic
async def start_scrape_internal(req, background_tasks: BackgroundTasks):
    global scraper_process
    if scraper_process and scraper_process.returncode is None:
        return {"status": "error", "message": "Scraper is already running"}

    background_tasks.add_task(run_scraper_subprocess, req)
    return {"status": "started", "message": f"Scraper ({req.mode}) started in background"}

@app.post("/api/start-scrape")
async def start_scrape_generic(req: ScrapeRequest, background_tasks: BackgroundTasks):
    # Backward compatibility
    req.mode = "auto" # Inject mode attribute
    return await start_scrape_internal(req, background_tasks)

@app.post("/api/stop-scrape")
async def stop_scrape():
    global scraper_process
    if scraper_process and scraper_process.returncode is None:
        scraper_process.terminate()
        try:
            await asyncio.wait_for(scraper_process.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            scraper_process.kill()
        scraper_process = None
        await broadcast_log("Scraper stopped by user.")
        return {"status": "stopped"}
    return {"status": "not_running", "message": "No active scraper process found"}

# --- Voice Agent Query Layer ---
try:
    from crawler_app.vector_db import VectorDB
except ImportError:
    # Fallback path logic
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    from crawler_app.vector_db import VectorDB

# Initialize VectorDB instance for querying
vector_db = None
try:
    vector_db = VectorDB()
except Exception as e:
    print(f"Failed to initialize VectorDB: {e}")
    # Do not crash the app, just leave vector_db as None (endpoints will handle it)

class QueryRequest(BaseModel):
    query: Optional[str] = None
    args: Optional[dict] = None
    top_k: int = 3
    # Allow extra fields like 'call', 'name', etc.
    class Config:
        extra = "allow"

@app.post("/api/query-voice")
async def query_voice_generic(req: QueryRequest):
    """Generic query endpoint (searches all types)"""
    logger.info(f"Voice Query wrapper received: {req.dict()}")
    
    # Handle Retell's 'args' wrapper or direct call
    actual_query = req.query
    actual_top_k = req.top_k
    
    if req.args and isinstance(req.args, dict):
        actual_query = req.args.get('query')
        if req.args.get('top_k'):
            actual_top_k = int(req.args.get('top_k'))
            
    if not actual_query:
        logger.error("Voice Query failed: No query string provided.")
        # Retell expects specific error format or just a polite failure string
        return {"text": "I didn't receive a query to search for.", "variables": {}}

    logger.info(f"Processing Query: {actual_query} (top_k={actual_top_k})")
    
    if not vector_db: 
        logger.error("Voice Query failed: Database not initialized.")
        return {"text": "Database not initialized. Please check API keys.", "variables": {}}
    
    try:
        res = vector_db.search(actual_query, top_k=actual_top_k)
        logger.info(f"Voice Query successful. Matches found: {len(res.get('text', '')) > 0}")
        # Merge variables into top level if requested, or keep separate. 
        # Retell often wants flat response for dynamic variables.
        return {**res, **res.get("variables", {})}
    except Exception as e:
        logger.error(f"Voice Query error: {e}")
        return {"text": f"Error during search: {str(e)}", "variables": {}}

@app.post("/api/query/people")
async def query_people(req: QueryRequest):
    """Query specific to People"""
    if not vector_db: return {"text": "Database not initialized.", "variables": {}}
    res = vector_db.search(req.query, top_k=req.top_k, filter_type='person')
    return {**res, **res.get("variables", {})}

@app.post("/api/query/properties")
async def query_properties(req: QueryRequest):
    """Query specific to Properties"""
    if not vector_db: return {"text": "Database not initialized.", "variables": {}}
    res = vector_db.search(req.query, top_k=req.top_k, filter_type='property')
    return {**res, **res.get("variables", {})}
