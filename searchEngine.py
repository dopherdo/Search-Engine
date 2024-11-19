import json
import math
from pathlib import Path
from collections import defaultdict, Counter

class SearchEngine:
    def __init__(self, index_path):
        self.index_path = Path(index_path)
        self.inverted_index = {}
        self.document_count = 0
        self.load_index()

    def load_index(self):
        """Loads the final inverted index and calculates document count."""
        try:
            with open(self.index_path, 'r', encoding='utf-8') as f:
                for line in f:
                    term_data = json.loads(line)
                    term, postings = list(term_data.items())[0]
                    self.inverted_index[term] = postings
            self.document_count = len({entry['doc_id'] for postings in self.inverted_index.values() for entry in postings})
        except Exception as e:
            print(f"Error loading index: {e}")

    def boolean_and_query(self, query_terms):
        """Performs a boolean 'AND' query using postings list intersection."""
        query_terms = [term.lower() for term in query_terms]
        results = None

        for term in query_terms:
            postings = self.get_postings(term)
            if postings is None:
                return []  # If any term is missing, the result is empty
            doc_ids = {entry['doc_id'] for entry in postings}
            results = doc_ids if results is None else results.intersection(doc_ids)

        return list(results) if results else []

    def get_postings(self, term):
        """Retrieves the postings list for a term."""
        return self.inverted_index.get(term, None)

    def rank_results(self, query_terms, result_docs):
        """Rank results by cosine similarity (tf-idf)."""
        term_idf = {term: self.compute_idf(term) for term in query_terms if term in self.inverted_index}
        doc_scores = defaultdict(float)

        for term, idf in term_idf.items():
            postings = self.get_postings(term)
            for entry in postings:
                if entry['doc_id'] in result_docs:
                    tf = entry['term_frequency']
                    doc_scores[entry['doc_id']] += tf * idf

        return sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)

    def compute_idf(self, term):
        """Calculates inverse document frequency (IDF) for a term."""
        postings = self.get_postings(term)
        if not postings:
            return 0
        return math.log(self.document_count / len(postings))

    def fetch_urls(self, doc_ids, url_map_path="url_map.json"):
        """Maps document IDs to URLs."""
        with open(url_map_path, 'r') as f:
            url_map = json.load(f)
        return [url_map[str(doc_id)] for doc_id in doc_ids if str(doc_id) in url_map]
