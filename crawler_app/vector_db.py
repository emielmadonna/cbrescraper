import os
import json
from pinecone import Pinecone, ServerlessSpec
from openai import OpenAI
import time

class VectorDB:
    def __init__(self):
        self.api_key = os.getenv("PINECONE_API_KEY")
        self.env = os.getenv("PINECONE_ENV") # Not strictly needed for new Pinecone SDK but good to have
        self.index_name = os.getenv("PINECONE_INDEX", "cbre-data")
        self.openai_key = os.getenv("OPENAI_API_KEY")
        
        self.pc = None
        self.index = None
        self.openai = None
        
        if self.api_key and self.openai_key:
            try:
                print("Initializing Vector DB connection...")
                self.pc = Pinecone(api_key=self.api_key)
                self.openai = OpenAI(api_key=self.openai_key)
                
                # Check if index exists, if not create (if we have permissions/serverless)
                # For now assume index exists or we just connect
                existing_indexes = [i.name for i in self.pc.list_indexes()]
                if self.index_name not in existing_indexes:
                    print(f"Index {self.index_name} not found. Creating...")
                    try:
                        self.pc.create_index(
                            name=self.index_name,
                            dimension=1536, # OpenAI text-embedding-3-small
                            metric='cosine',
                            spec=ServerlessSpec(
                                cloud='aws',
                                region='us-east-1'
                            )
                        )
                        time.sleep(5) # Wait for init
                    except Exception as e:
                        print(f"Could not create index (might check permissions): {e}")
                
                self.index = self.pc.Index(self.index_name)
                print(f"Connected to Pinecone Index: {self.index_name}")
                
            except Exception as e:
                print(f"Error initializing VectorDB: {e}")
        else:
            print("VectorDB Warning: Missing API Keys (PINECONE_API_KEY or OPENAI_API_KEY). Vector features disabled.")

    def get_embedding(self, text):
        if not self.openai:
            return None
        try:
            text = text.replace("\n", " ")
            return self.openai.embeddings.create(input=[text], model="text-embedding-3-small").data[0].embedding
        except Exception as e:
            print(f"Error generating embedding: {e}")
            return None

    def upsert_person(self, person_data):
        if not self.index:
            return
        
        try:
            url = person_data.get('URL', '')
            if not url: return

            # Create Text Blob for Search
            # "Joe Riley - Senior Vice President at CBRE. Seattle, WA. Experience: ..."
            name = f"{person_data.get('First Name', '')} {person_data.get('Last Name', '')}".strip()
            
            # Extract simple text from Experience (truncate if too long)
            experience = person_data.get('Experience', '')
            if len(experience) > 8000: experience = experience[:8000]
            
            text_blob = f"{name} - CBRE Professional. Located in {person_data.get('City', '')}, {person_data.get('State', '')}. \n\nExperience: {experience}"
            
            # Generate Embedding
            vector = self.get_embedding(text_blob)
            if not vector: return

            # Metadata
            metadata = {
                'type': 'person',
                'url': url,
                'first_name': person_data.get('First Name', ''),
                'last_name': person_data.get('Last Name', ''),
                'phones': person_data.get('Phone', ''),
                'email': '', # Scraper doesnt get email for main profile yet?
                'city': person_data.get('City', ''),
                'state': person_data.get('State', ''),
                'text': text_blob,
                'vcard': person_data.get('vCardURL', ''),
                'cbre_listings_url': person_data.get('ListingsURL', '')
            }
            
            # Upsert
            self.index.upsert(vectors=[(url, vector, metadata)])
            print(f"Successfully upserted person: {name}")
            
        except Exception as e:
            print(f"Error upserting person: {e}")

    def upsert_property(self, prop_data):
        if not self.index:
            return
            
        try:
            url = prop_data.get('URL', '')
            if not url: return

            name = prop_data.get('Property Name', 'Unknown Property')
            address = prop_data.get('Address', '')
            
            # Create Text Blob
            description = prop_data.get('Description', '')
            text_blob = f"Property: {name}. Located at: {address}. {description}"
            
            # Add broker names to text for searchability
            brokers = prop_data.get('Brokers', [])
            if brokers:
                broker_names = [b.get('Name') for b in brokers]
                text_blob += f" Brokers: {', '.join(broker_names)}."

            # Generate Embedding
            vector = self.get_embedding(text_blob)
            if not vector: return

            # Metadata
            metadata = {
                'type': 'property',
                'url': url,
                'name': name,
                'address': address,
                'text': text_blob,
                'broker_count': len(brokers),
                'brochure_url': prop_data.get('Brochure URL', ''),
                'description': description
            }
            
            self.index.upsert(vectors=[(url, vector, metadata)])
            print(f"Successfully upserted property: {name}")
            
        except Exception as e:
            print(f"Error upserting property: {e}")

    def search(self, query_text, top_k=3, filter_type=None):
        """
        Searches the vector DB and returns formatted text for a Voice Agent.
        :param filter_type: 'person' or 'property' or None
        """
        if not self.index:
            return "Vector database is not configured."
            
        try:
            vector = self.get_embedding(query_text)
            if not vector: return "Could not generate embedding for query."
            
            # Build filter dict
            query_filter = {}
            if filter_type:
                query_filter['type'] = filter_type
            
            results = self.index.query(
                vector=vector, 
                top_k=top_k, 
                include_metadata=True,
                filter=query_filter if query_filter else None
            )
            
            matches = results.get('matches', [])
            if not matches:
                # Fallback message?
                return "I couldn't find any relevant information in the database."
            
            # Format response for Voice / API
            response_parts = []
            
            for m in matches:
                md = m.metadata
                score = m.score
                if score < 0.70: continue # Threshold
                
                if md.get('type') == 'person':
                    # Detailed Person Response
                    first = md.get('first_name')
                    last = md.get('last_name')
                    phones = md.get('phones', 'No phone available')
                    vcard = md.get('vcard') or "No vCard"
                    city = md.get('city')
                    state = md.get('state')
                    listings_url = md.get('cbre_listings_url')
                    
                    # Format as a structured text block for the Voice Agent to consume
                    part = (
                        f"Name: {first} {last}\n"
                        f"Location: {city}, {state}\n"
                        f"Phones: {phones}\n"
                        f"vCard: {vcard}\n"
                        f"Listings: {listings_url}\n"
                    )
                    response_parts.append(part)
                    
                elif md.get('type') == 'property':
                    # Property Response
                    name = md.get('name')
                    address = md.get('address')
                    broker_count = md.get('broker_count', 0)
                    brochure = md.get('brochure_url') or "No brochure link"
                    desc = md.get('description') or ""
                    
                    part = (
                        f"Property: {name}\n"
                        f"Address: {address}\n"
                        f"Brochure: {brochure}\n"
                        f"Description: {desc[:200]}...\n" # Truncate for brevity
                        f"Listed by {broker_count} brokers."
                    )
                    response_parts.append(part)
            
            if not response_parts:
                return "I couldn't find any relevant information in the database for that query."
                
            # If specific person query, return clearer separation
            if filter_type == 'person':
                 return "Here are the details found:\n\n" + "\n---\n".join(response_parts)

            return "Here is what I found: " + " ".join(response_parts)
            
        except Exception as e:
            return f"Error querying database: {e}"
