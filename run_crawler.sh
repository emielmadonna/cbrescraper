#!/bin/bash
cd "$(dirname "$0")"
python3 -m streamlit run crawler_app/app.py
