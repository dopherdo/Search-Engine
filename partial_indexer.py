import os
import json
from collections import defaultdict, Counter
from pathlib import Path
import heapq  # For priority queue to efficiently merge the sorted tokens
from worker import Worker
from utils import Utils


class PartialIndexer:
    def __init__(self, file_directory, partial_index_directory):
        self.utils = Utils(partial_index_directory)
        self.workers = []
        self.file_directory = file_directory


    def create_workers(self):
        '''
        Create a worker for each subdir and run create_partial_indices() on it
        '''
        # TODO: Path() does not work bc of something Sasson said
        for subdir in Path(self.file_directory).iterdir(): # Iterates through each subdir in DEV folder
            if subdir.is_dir():
                worker = Worker(subdir, self.utils)
                self.workers.append(worker)
        
        for worker in self.workers:
            worker.start()
        
        for worker in self.workers:
            worker.join()
        
        print(self.utils.url_map)
        print(f"Finished Partial Indexing")
    
    
    
