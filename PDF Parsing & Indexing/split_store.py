import uuid
import os
import google.generativeai as genai
from astrapy import DataAPIClient

def split_md_by_delimiter(file_path, delimiter="pageseparator"):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        chunks = content.split(delimiter)
        
        chunks = [chunk.strip() for chunk in chunks if chunk.strip()]
        
        page_info = []
        for chunk in chunks:
            lines = chunk.split('\n')
            page_num = None
            if lines and lines[0].startswith("Page Number"):
                try:
                    page_num = lines[0].split('-')[1].strip()
                except IndexError:
                    page_num = "Unknown"
            
            page_info.append({
                "page": page_num,
                "text": chunk
            })
        
        print(f"Successfully split content into {len(page_info)} chunks")
        return page_info
    except Exception as e:
        print(f"Error reading or splitting file {file_path}: {e}")
        return []


def get_embeddings(text_chunks, api_key):

    genai.configure(api_key=api_key)
    
    embeddings = []
    for chunk in text_chunks:
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:

                embedding_result = genai.embed_content(
                    model="models/text-embedding-004",
                    content=chunk["text"],
                    task_type="RETRIEVAL_DOCUMENT"
                )
                
                chunk_with_embedding = {
                    "page": chunk["page"],
                    "text": chunk["text"],
                    "embedding": embedding_result["embedding"]
                }
                embeddings.append(chunk_with_embedding)
                print(f"Generated embedding for page {chunk['page']}")
                break
            except Exception as e:
                retries += 1
                print(f"Error generating embedding for page {chunk['page']} (attempt {retries}/{max_retries}): {e}")
                if retries >= max_retries:
                    print(f"Failed to generate embedding after {max_retries} attempts")
    
    return embeddings

def store_in_astra_db(embeddings, astra_token, api_endpoint):
    try:
        client = DataAPIClient(astra_token)
        db = client.get_database_by_api_endpoint(api_endpoint)
        
        collection_name = "textbook"
        
        collections = db.get_collections()
        collection_exists = any(col.get('name') == collection_name for col in collections)
        
        if not collection_exists:
            print(f"Creating new collection: {collection_name}")
            db.create_collection(collection_name, dimension=768)
        
        collection = db.get_collection(collection_name)
        
        if embeddings:
            documents_to_insert = []
            for item in embeddings:
                document = {
                    "_id": str(uuid.uuid4()),
                    "page": item["page"],
                    "text": item["text"],
                    "$vector": item["embedding"]
                }
                documents_to_insert.append(document)
            
            batch_size = 10
            for i in range(0, len(documents_to_insert), batch_size):
                batch = documents_to_insert[i:i+batch_size]
                collection.insert_many(batch)
                print(f"Stored batch {i//batch_size + 1}/{(len(documents_to_insert)-1)//batch_size + 1} in Astra DB")
            
            print(f"Successfully stored {len(documents_to_insert)} chunks in Astra DB")
        else:
            print("No embeddings to store. Please check the embedding generation step.")
    
    except Exception as e:
        print(f"Error connecting to or storing in Astra DB: {e}")
        raise e

def process_markdown_file(file_path, api_key, astra_token, api_endpoint):
    chunks = split_md_by_delimiter(file_path)
    if not chunks:
        print("Failed to process markdown file. Exiting.")
        return
    
    print(f"Split markdown into {len(chunks)} chunks")
    
    embeddings = get_embeddings(chunks, api_key)
    print(f"Generated embeddings for {len(embeddings)} chunks")
    
    if not embeddings:
        print("No embeddings were generated. Exiting.")
        return
    
    store_in_astra_db(embeddings, astra_token, api_endpoint)

if __name__ == "__main__":

    md_file_path = "numbered_output.md"
    
    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    astra_token = os.environ.get("ASTRA_TOKEN")
    api_endpoint = os.environ.get("ASTRA_API_ENDPOINT")
    
    process_markdown_file(md_file_path, gemini_api_key, astra_token, api_endpoint)