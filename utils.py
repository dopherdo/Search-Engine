import threading

class Utils:
    def __init__(self):
        self.docId = 0
        self.lock = threading.Lock()  # Add a lock for thread safety
    
    def increment_docID(self):
        with self.lock:  # Acquire the lock before modifying the shared resource
            self.docId += 1
            return self.docId  # Optionally return the new docId