import threading
from datasketch import MinHashLSH

class Utils:
    def __init__(self, partial_index_directory):
        self.docId = 0
        self.lock = threading.Lock()  # Add a lock for thread safety
        self.partial_index_directory = partial_index_directory
        self.url_map = {}
        self.seen_hashes = set() 
        self.lsh = MinHashLSH(threshold=0.85, num_perm=128)
        self.doc_count = 0

    
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
