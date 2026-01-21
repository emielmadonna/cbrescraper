import os
from crawler_app.vector_db import VectorDB

# Initialize VectorDB (it will load its own .env)
db = VectorDB()

def test_query(query, filter_type=None):
    print(f"\n--- Testing Query: '{query}' (Filter: {filter_type}) ---")
    res = db.search(query, filter_type=filter_type)
    print("Response Text:", res.get('text'))
    print("Variables:", res.get('variables'))
    return res

# 1. Test Person Search
test_query("Joe Riley", filter_type='person')

# 2. Test Property Search
test_query("Monte Villa Center", filter_type='property')

# 3. Test Generic Search (Should find Joe Riley in directory AND/OR Monte Villa in listings)
print("\n--- Testing Universal Search (No Filter) ---")
print("Searching for 'Joe'...")
test_query("Joe")

print("\nSearching for 'Monte'...")
test_query("Monte")
