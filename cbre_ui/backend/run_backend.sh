#!/bin/bash
cd "$(dirname "$0")"
echo "Backend running on local network: http://192.168.6.27:8000"
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
