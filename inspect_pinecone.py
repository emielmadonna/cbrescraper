import os
import json
from pinecone import Pinecone
from dotenv import load_dotenv

# Load env from backend dir
env_path = os.path.join('cbre_ui', 'backend', '.env')
load_dotenv(env_path)

api_key = os.getenv("PINECONE_API_KEY")
host = "https://cbre-5eba2yo.svc.aped-4627-b74a.pinecone.io"

pc = Pinecone(api_key=api_key)
index = pc.Index(host=host)

def inspect_record(record_id, namespace):
    print(f"\n--- Inspecting ID: {record_id} in Namespace: {namespace} ---")
    try:
        # Use simple fetch first, usually compatible
        res = index.fetch(ids=[record_id], namespace=namespace)
        
        # Safe access for both object and dict
        vectors = {}
        if hasattr(res, 'vectors'):
            vectors = res.vectors
        elif isinstance(res, dict):
            vectors = res.get('vectors', {})
        
        if record_id in vectors:
            vector = vectors[record_id]
            # Access fields depending on object vs dict
            metadata = getattr(vector, 'metadata', None) or vector.get('metadata', {})
            _id = getattr(vector, 'id', record_id)
            
            print(f"ID: {_id}")
            print(f"Metadata Keys: {list(metadata.keys())}")
            print("Full Metadata JSON:")
            print(json.dumps(metadata, indent=2))
        else:
            print(f"Record {record_id} NOT FOUND in namespace {namespace}.")
            print(f"Available vectors in response: {list(vectors.keys())}")
    except Exception as e:
        print(f"Error fetching record: {e}")

# 1. Inspect Joe Riley
inspect_record('broker-joe-riley', 'seattle_directory')

# 2. Inspect Monte Villa Center
inspect_record('prop-monte-villa-center-s', 'seattle_listings')
