import json
import math
from pathlib import Path
from collections import defaultdict

class SearchEngine:
    def __init__(self, index_path, offset_path):
        self.index_path = Path(index_path)
        self.offset_path = Path(offset_path)
        self.document_count = 0
        self.offsets = self.load_offsets()

    def load_offsets(self):
        """Load the offset mappings from the offset file."""
        with open(self.offset_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def get_postings(self, term):
        """Retrieve the postings list for a term using .seek()."""
        prefix = term[:2]  # For example, 'ma' for 'machine'
        if prefix not in self.offsets:
            return None  # No postings for this prefix

        try:
            with open(self.index_path, 'r', encoding='utf-8') as f:
                f.seek(self.offsets[prefix])  # Jump to the offset where the term starts
                for line in f:
                    term_data = json.loads(line)
                    term_in_file, postings = list(term_data.items())[0]
                    if term_in_file == term:
                        return postings
                    elif term_in_file > term:
                        break  # No need to check further if terms are lexicographically greater
        except Exception as e:
            print(f"Error retrieving postings for term '{term}': {e}")
        return None

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

    def rank_results(self, query_terms, result_docs):
        """Rank results by cosine similarity (tf-idf)."""
        doc_scores = defaultdict(float)
        for term in query_terms:
            postings = self.get_postings(term)
            for entry in postings:
                if entry['doc_id'] in result_docs:
                    tf = entry['term_frequency']
                    doc_scores[entry['doc_id']] += entry["tf_idf"]

        return sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)

    def fetch_urls(self, doc_ids, url_map_path="url_map.json"):
        """Maps document IDs to URLs."""
        with open(url_map_path, 'r') as f:
            url_map = json.load(f)
        return [url_map[str(doc_id)][0] for doc_id in doc_ids if str(doc_id) in url_map]
