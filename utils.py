import threading

class Utils:
    def __init__(self, partial_index_directory):
        self.docId = 0
        self.lock = threading.Lock()  # Add a lock for thread safety
        self.partial_index_directory = partial_index_directory
        self.url_map = {}

    
    def increment_docID(self, url, token_count):
        with self.lock:  # Acquire the lock before modifying the shared resource
            self.docId += 1
            self.url_map[self.docId] = (url, token_count)
            return self.docId
    
    