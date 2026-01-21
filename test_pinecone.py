import os
from pinecone import Pinecone
from dotenv import load_dotenv

env_path = os.path.join('cbre_ui', 'backend', '.env')
load_dotenv(env_path)

api_key = os.getenv("PINECONE_API_KEY")
host = "https://cbre-5eba2yo.svc.aped-4627-b74a.pinecone.io"

pc = Pinecone(api_key=api_key)
index = pc.Index(host=host)

print(f"Connected to index: {host}")
try:
    stats = index.describe_index_stats()
    print("Index Stats:", stats)
except Exception as e:
    print(f"Error getting stats: {e}")

# Check if it supports integrated inference (Llama?)
try:
    # Try a query with text if the SDK supports it or if we can see the configuration
    print("Checking for integrated inference...")
    # This is speculative based on "lama" comment
    # res = index.query(text="test", top_k=1) 
    # print("Inference Query Result:", res)
except Exception as e:
    print(f"Integrated inference not found or error: {e}")
