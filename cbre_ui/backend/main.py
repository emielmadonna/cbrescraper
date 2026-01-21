from fastapi import FastAPI, WebSocket, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import subprocess
import os
import sys
import json
from typing import Optional, List
from dotenv import load_dotenv

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
    limit: Optional[int] = None

@app.get("/")
async def root():
    return {"status": "CBRE Scraper API is running"}

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_websockets.append(websocket)
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
        
    if not req.headless:
        cmd.append("--show-browser")
        
    if req.dry_run:
        cmd.append("--dry-run")
        
    if req.limit:
        cmd.extend(["--limit", str(req.limit)])

    print(f"Starting scraper: {' '.join(cmd)}")
    await broadcast_log(f"Starting scraper for URL: {req.url} (Mode: {getattr(req, 'mode', 'auto')}, Test: {req.dry_run})")
    
    try:
        scraper_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            bufsize=1  # Line buffered
        )

        # Stream output
        while True:
            line = scraper_process.stdout.readline()
            if not line and scraper_process.poll() is not None:
                break
            if line:
                await broadcast_log(line.strip())
                print(f"Scraper: {line.strip()}")
        
        rc = scraper_process.poll()
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
    if scraper_process and scraper_process.poll() is None:
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
    if scraper_process and scraper_process.poll() is None:
        scraper_process.terminate()
        try:
            scraper_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
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
vector_db = VectorDB()

class QueryRequest(BaseModel):
    query: str
    top_k: int = 3
    
@app.post("/api/query-voice")
async def query_voice_generic(req: QueryRequest):
    """Generic query endpoint (searches all types)"""
    if not vector_db: return {"text": "Database not initialized."}
    return {"text": vector_db.search(req.query, top_k=req.top_k)}

@app.post("/api/query/people")
async def query_people(req: QueryRequest):
    """Query specific to People"""
    if not vector_db: return {"text": "Database not initialized."}
    return {"text": vector_db.search(req.query, top_k=req.top_k, filter_type='person')}

@app.post("/api/query/properties")
async def query_properties(req: QueryRequest):
    """Query specific to Properties"""
    if not vector_db: return {"text": "Database not initialized."}
    return {"text": vector_db.search(req.query, top_k=req.top_k, filter_type='property')}
