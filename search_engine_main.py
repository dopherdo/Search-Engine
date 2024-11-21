from search_engine import SearchEngine
import time


def run_query(query):
    start_time = time.time()
    index_path = "final_inverted_index.json"  # Path to the inverted index
    token_positions_path = "prefix_positions.json"
    search_engine = SearchEngine(index_path, token_positions_path)

    query = query.split()
    
        
    print(f"\nQuery {' '.join(query)}")

    print(query)

    doc_ids = search_engine.boolean_and_query(query)
    if not doc_ids:
        print("No results found.")
        return []

    ranked_results = search_engine.rank_results(query, doc_ids)
    top_5_docs = [doc_id for doc_id, score in ranked_results[:5]]
    urls = search_engine.fetch_urls(top_5_docs)


    print("Top 5 URLs:")
    for url in urls:
        print(url)
    
    # End timing
    end_time = time.time()
    
    # Calculate elapsed time
    elapsed_time = end_time - start_time
    print(f"Query execution time: {elapsed_time:.4f} seconds")
    
    print(urls)

    return urls, elapsed_time