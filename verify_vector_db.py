import os
import sys
import json
from crawler_app.vector_db import VectorDB

# Mock env vars if not present for testing
if not os.getenv("PINECONE_API_KEY") or not os.getenv("OPENAI_API_KEY"):
    print("Warning: Missing API Keys. Verification will likely fail or show warning.")

def verify_vector_db():
    print("--- Verifying VectorDB Initialization ---")
    try:
        vdb = VectorDB()
        if vdb.index:
            print("[+] Connected to Pinecone Index.")
        else:
            print("[-] Standard init failed (expected if no keys).")
            
        print("\n--- Verifying Search Method ---")
        # Mock search if no keys
        if not vdb.index:
            print("[*] Mocking internal index for search test logic...")
            # We can't really mock the whole pinecone object easily without mocks lib
            # But we can check if the method exists and handles missing index
            res = vdb.search("test query")
            print(f"Result (expecting 'Vector database is not configured'): {res}")
            
    except Exception as e:
        print(f"Error during verification: {e}")

if __name__ == "__main__":
    verify_vector_db()
