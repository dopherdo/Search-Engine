import threading
from datasketch import MinHashLSH
from collections import defaultdict
from scipy.sparse import csr_matrix
import numpy as np

class Utils:
    def __init__(self, partial_index_directory):
        self.docId = 0
        self.lock = threading.Lock()  # Add a lock for thread safety
        self.partial_index_directory = partial_index_directory
        self.url_map = {}
        self.seen_hashes = set() 
        self.lsh = MinHashLSH(threshold=0.85, num_perm=128)
        self.doc_count = 0
        self.link_graph = defaultdict(set)      # currently holds the adjacency list

    def build_sparse_matrix(self):
        """Convert the link graph into a sparse matrix."""
        url_to_id = {url: idx for idx, url in enumerate(self.link_graph.keys())}
        num_nodes = len(url_to_id)
        row_indices = []
        col_indices = []

        for from_url, to_urls in self.link_graph.items():
            from_id = url_to_id[from_url]
            for to_url in to_urls:
                if to_url in url_to_id:
                    to_id = url_to_id[to_url]
                    row_indices.append(to_id)
                    col_indices.append(from_id)

        data = np.ones(len(row_indices))
        sparse_matrix = csr_matrix((data, (row_indices, col_indices)), shape=(num_nodes, num_nodes))
        
        return sparse_matrix, url_to_id
    
    def update_link_graph(self, from_url, to_urls):
        '''
        Improve PageRank link graph construction
        '''
        with self.lock:
            # Filter out non-http(s) links and duplicates
            clean_urls = set()
            for url in to_urls:
                # Remove fragment identifiers and internal links
                if url.startswith(('http://', 'https://')) and url != from_url:
                    clean_urls.add(url)
            
            # Only add if there are clean outgoing links
            if clean_urls:
                if from_url not in self.link_graph:
                    self.link_graph[from_url] = set()
                self.link_graph[from_url].update(clean_urls)

    def normalize_url(self, url, base_url):
        '''
        PageRank: Normalize URLs to absolute form if needed.
        '''
        with self.lock:  
            return url  # Add logic to resolve relative URLs using base_url if needed
    
    def increment_docID(self, url, token_count):
        with self.lock:  # Acquire the lock before modifying the shared resource
            self.docId += 1
            self.url_map[self.docId] = (url, token_count)
            return self.docId
    
    def lsh_insert(self, minhash):
        with self.lock:
            self.lsh.insert(f"doc_{self.doc_count}", minhash)
            self.doc_count += 1 # incerment the doc count (Eg: doc_1 , doc2, etc)

    def add_seen_hashes(self, hash_value):
        with self.lock:
            if (hash_value) not in self.seen_hashes:
                self.seen_hashes.add(hash_value)
    
    def similar_docs(self, minhash):
        with self.lock:
            self.lsh.query(minhash)

                
    def check_duplicate_hash(self, hash_value):
        with self.lock:
            return hash_value in self.seen_hashes
