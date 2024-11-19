

from searchEngine import SearchEngine

def main():
    index_path = "final_inverted_index.json"  # Path to the inverted index
    search_engine = SearchEngine(index_path)

    queries = [
        ["cristina", "lopes"],
        ["machine", "learning"],
        ["acm"],
        ["master", "of", "software", "engineering"]
    ]

    for i, query in enumerate(queries, 1):
        print(f"\nQuery {i}: {' '.join(query)}")
        doc_ids = search_engine.boolean_and_query(query)
        if not doc_ids:
            print("No results found.")
            continue

        ranked_results = search_engine.rank_results(query, doc_ids)
        top_5_docs = [doc_id for doc_id, score in ranked_results[:5]]
        urls = search_engine.fetch_urls(top_5_docs)

        print("Top 5 URLs:")
        for url in urls:
            print(url)

if __name__ == "__main__":
    main()