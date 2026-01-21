import os
import json
import re
import logging
import time
from pinecone import Pinecone, ServerlessSpec, SearchQuery
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def slugify(text):
    """Generates a clean ID from text (e.g., 'Joe Riley' -> 'joe-riley')."""
    if not text: return "unknown"
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_-]+', '-', text)
    text = re.sub(r'^-+|-+$', '', text)
    return text

class VectorDB:
    def __init__(self):
        # Load env from backend dir (Optional, for local dev)
        env_path = os.path.join(os.path.dirname(__file__), '..', 'cbre_ui', 'backend', '.env')
        if os.path.exists(env_path):
            load_dotenv(env_path)
        else:
            logger.info("Local .env not found, relying on system environment variables.")
        
        self.api_key = os.getenv("PINECONE_API_KEY")
        self.env = os.getenv("PINECONE_ENV") 
        self.index_name = os.getenv("PINECONE_INDEX", "cbre")
        self.index_host = "https://cbre-5eba2yo.svc.aped-4627-b74a.pinecone.io"
        
        self.pc = None
        self.index = None
        
        self.vector_dimension = 1024 # User's index dimension
        
        if self.api_key:
            try:
                logger.info(f"Connecting to Pinecone Host: {self.index_host}")
                self.pc = Pinecone(api_key=self.api_key)
                self.index = self.pc.Index(host=self.index_host)
                logger.info(f"Connected to Pinecone Index successfully.")
            except Exception as e:
                logger.error(f"Error initializing VectorDB: {e}")
                raise e
        else:
            logger.error("CRITICAL: Missing PINECONE_API_KEY. Vector features will be broken.")
            raise ValueError("Missing PINECONE_API_KEY")

    def get_embedding(self, text):
        if not self.openai:
            return None
        try:
            text = text.replace("\n", " ")
            # Use text-embedding-3-small which supports dimension parameter
            response = self.openai.embeddings.create(
                input=[text], 
                model="text-embedding-3-small",
                dimensions=self.vector_dimension
            )
            return response.data[0].embedding
        except Exception as e:
            print(f"Error generating embedding: {e}")
            return None

    def exists(self, url, namespace=None):
        """Checks if a URL already exists in the index/namespace by querying metadata."""
        if not self.index: return False
        try:
            # Query by 'url' metadata field instead of ID
            res = self.index.query(
                vector=[0.0] * self.vector_dimension,
                top_k=1,
                filter={'url': url},
                namespace=namespace
            )
            return len(res.get('matches', [])) > 0
        except Exception as e:
            # print(f"Exists check error: {e}")
            return False

    def upsert_person(self, person_data):
        if not self.index:
            return
        
        try:
            url = person_data.get('URL', '')
            if not url: return
            
            namespace = "seattle_directory"

            # DUPLICATE CHECK (In specific namespace)
            if self.exists(url, namespace=namespace):
                print(f"    - Skipping (Already in Namespace {namespace}): {url}")
                return

            # Structured Search Text (The "Brain")
            name = f"{person_data.get('First Name', '')} {person_data.get('Last Name', '')}".strip()
            title = person_data.get('Title', 'N/A')
            specialties = person_data.get('Specialties', 'N/A')
            # Extract keywords for searchable identity
            # Layout: "Broker Name: [Name]. Specialty: [Specialties]. Role: [Title] at CBRE Seattle."
            text_blob = f"Broker Name: {name}. Specialty: {specialties}. Role: {title} at CBRE Seattle."
            
            # ID Structure: "broker-slugified-name"
            record_id = f"broker-{slugify(name)}"
            if record_id == "broker-unknown": record_id = f"broker-{slugify(url)}" # Fallback

            # Metadata (Exact March Pilot Layout)
            metadata = {
                'type': 'person',
                'full_name': name,
                'phone_number': person_data.get('phone_number') or '',
                'mobile_number': person_data.get('mobile_phoneNumber') or '',
                'email': person_data.get('Email', ''),
                'vcard_url': person_data.get('vCardURL', ''),
                'specialty_tags': person_data.get('specialty_tags', []),
                'bio': person_data.get('bio_summary', ''),
                'url': url
            }
            
            # Upsert (Integrated Inference v6/v7: Pass 'text' as field for llama-text-embed-v2)
            self.index.upsert_records(
                namespace=namespace,
                records=[{
                    "_id": record_id,
                    "text": text_blob,
                    **metadata
                }]
            )
            logger.info(f"Successfully upserted person to {namespace}: {record_id}")
            
        except Exception as e:
            print(f"Error upserting person: {e}")

    def upsert_property(self, prop_data):
        if not self.index:
            return
            
        try:
            url = prop_data.get('URL', '')
            if not url: return
            
            namespace = "seattle_listings"

            # DUPLICATE CHECK
            if self.exists(url, namespace=namespace):
                print(f"    - Skipping (Already in Namespace {namespace}): {url}")
                return

            name = prop_data.get('Property Name', 'Unknown Property')
            address = prop_data.get('Address', '')
            prop_type = prop_data.get('Type', 'Commercial space')
            
            # Record Layout: "Property: X. Address: Y. Type: Z."
            text_blob = f"Property: {name}. Address: {address}. Type: {prop_type}."
            
            # ID Structure: "prop-slugified-name"
            prop_id = prop_data.get('Property ID') or slugify(name)[:20]
            record_id = f"prop-{prop_id}"

            # Primary Broker logic
            brokers = prop_data.get('Brokers', [])
            primary_broker = brokers[0].get('Name', 'Not Listed') if brokers else "Not Listed"
            broker_phone = brokers[0].get('phone_number', '') if brokers else ""

            # Metadata (Exact March Pilot Layout)
            metadata = {
                'type': 'property',
                'address': address,
                'brochure_url': prop_data.get('Brochure URL', 'Not Found'),
                'primary_broker': primary_broker,
                'broker_phone': broker_phone,
                'sq_ft_range': prop_data.get('SqFt', 'N/A'),
                'url': url
            }
            
            # Upsert (Integrated Inference v6/v7)
            self.index.upsert_records(
                namespace=namespace,
                records=[{
                    "_id": record_id,
                    "text": text_blob,
                    **metadata
                }]
            )
            logger.info(f"Successfully upserted property to {namespace}: {record_id}")
            
        except Exception as e:
            print(f"Error upserting property: {e}")

    def search(self, query_text, top_k=3, filter_type=None):
        """
        Searches specific namespaces for Structured RAG.
        :param filter_type: 'person' or 'property' or None
        """
        if not self.index:
            return {"text": "Vector database is not configured.", "variables": {}}
            
        try:
            # Determine Namespaces to Query
            namespaces_to_query = []
            if filter_type == 'person': 
                namespaces_to_query = ["seattle_directory"]
            elif filter_type == 'property': 
                namespaces_to_query = ["seattle_listings"]
            else: 
                # Generic Search: Query both!
                namespaces_to_query = ["seattle_directory", "seattle_listings"]
            
            all_matches = []

            for ns in namespaces_to_query:
                # logger.info(f"DEBUG: Querying namespace: {ns}")
                try:
                    # Construct query object for this namespace
                    # Use 'None' for filter if generic search, or specific type if we had one (though implied by namespace)
                    current_filter = {'type': filter_type} if filter_type else None 
                    
                    # If generic search, we might optionally filter by type validation if we trust data purity,
                    # but typically just querying the namespace is enough.
                    # Let's be safe: if searching seattle_directory, filter by type='person' just in case data is mixed?
                    # Actually, data is siloed. Let's just trust namespace.
                    
                    query_obj = SearchQuery(
                        inputs={"text": query_text},
                        top_k=top_k,
                        filter=current_filter
                    )
                    
                    results = self.index.search_records(
                        namespace=ns,
                        query=query_obj
                    )

                    # Extract hits
                    hits = []
                    if hasattr(results, 'result'): # v7 object style
                        hits = getattr(results.result, 'hits', [])
                    elif isinstance(results, dict):
                        if 'result' in results and 'hits' in results['result']:
                            hits = results['result']['hits']
                        elif 'hits' in results:
                            hits = results['hits']
                    
                    if not hits and hasattr(results, 'hits'):
                         hits = results.hits

                    for hit in hits:
                        _id = getattr(hit, '_id', None) or hit.get('_id')
                        fields = getattr(hit, 'fields', {}) or hit.get('fields', {})
                        score = getattr(hit, '_score', 0.0) or hit.get('_score', 0.0)
                        
                        all_matches.append({
                            'id': _id,
                            'metadata': fields,
                            'score': score
                        })
                except Exception as ns_err:
                     logger.error(f"Error querying namespace {ns}: {ns_err}")
                     continue

            # Sort combined results by score descending
            all_matches.sort(key=lambda x: x['score'], reverse=True)
            
            # Take top_k from combined
            matches = all_matches[:top_k]

            if not matches:
                return {"text": "I couldn't find any relevant information in the database.", "variables": {}}
            
            # Format response for Voice / API
            response_parts = []
            top_variables = {}
            
            for m in matches:
                md = m['metadata']
                # score = m.score # Removed score check for now as we trust top_k or add it to match dict earlier
                
                if md.get('type') == 'person':
                    # Structured Record Mapping
                    name = md.get('full_name')
                    
                    # Prioritize Mobile for Retell Target, fallback to Office
                    mobile = md.get('mobile_number')
                    office = md.get('phone_number')
                    target = mobile if mobile else office
                    
                    vcard = md.get('vcard_url') or "N/A"
                    
                    part = (
                        f"Target: {name}. Phone: {target}. vCard: {vcard}. "
                        f"Bio: {md.get('bio', '')[:200]}..."
                    )
                    # For Retell AI (Special top-level keys)
                    if not top_variables:
                        top_variables = {"target_phone": target, "vcard_url": vcard}
                    response_parts.append(part)
                    
                elif md.get('type') == 'property':
                    # Structured Record Mapping
                    addr = md.get('address')
                    brochure = md.get('brochure_url')
                    broker = md.get('primary_broker')
                    broker_phone = md.get('broker_phone')
                    
                    part = (
                        f"Property at {addr}. Brochure: {brochure}. "
                        f"Contact: {broker} ({broker_phone})."
                    )
                    # For Retell AI (Map listing agent phone)
                    if not top_variables:
                        top_variables = {
                            "target_phone": broker_phone,
                            "brochure_url": brochure,
                            "property_address": addr,
                            "primary_broker": broker
                        }
                    response_parts.append(part)
            
            if not response_parts:
                return {"text": "No relevant info found.", "variables": {}}
                
            return {
                "text": "Here is what I found:\n\n" + "\n---\n".join(response_parts),
                "variables": top_variables
            }
            
        except Exception as e:
            return {"text": f"Error querying database: {e}", "variables": {}}
