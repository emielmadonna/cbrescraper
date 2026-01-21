import os
from pinecone import Pinecone
from dotenv import load_dotenv

env_path = os.path.join('cbre_ui', 'backend', '.env')
load_dotenv(env_path)

api_key = os.getenv("PINECONE_API_KEY")
host = "https://cbre-5eba2yo.svc.aped-4627-b74a.pinecone.io"

pc = Pinecone(api_key=api_key)
index = pc.Index(host=host)

# 1. Delete Person
record_id = 'broker-joe-riley'
namespace = 'seattle_directory'
print(f"Deleting {record_id} from {namespace}...")
try:
    index.delete(ids=[record_id], namespace=namespace)
    print("Deleted successfully.")
except Exception as e:
    print(f"Error deleting: {e}")

# 2. Delete Property
record_id_prop = 'prop-monte-villa-center-s'
namespace_prop = 'seattle_listings'
print(f"Deleting {record_id_prop} from {namespace_prop}...")
try:
    index.delete(ids=[record_id_prop], namespace=namespace_prop)
    print("Deleted successfully.")
except Exception as e:
    print(f"Error deleting: {e}")
