import os
import json
from collections import defaultdict, Counter
from pathlib import Path
import heapq  # For priority queue to efficiently merge the sorted tokens


class docID:
    def __init__(self):
        self.docId = 0
    
    def increment_docID(self):
        self.docId += 1


def create_workers(self):
    '''
    Create a worker for each subdir and run create_partial_indexes() on it
    '''
    # TODO: Path() does not work bc of something Sasson said
    for subs in Path(self.directory).iterdir(): # Iterates through each subdir in DEV folder
        # Create a worker/thread
        # worker.create_partial_indexes()
        pass

# Old implementation -- replaced by Michael's ChatGPT thing
"""def merge_partial_indexes(self, output_file="final_inverted_index.json"): 
    '''
    Merges all partial indexes into a final json file (disk)
    - Open the "final_inverted_index.json" where we will write our final inverted index
    - Open all partial_index jsons at once
        Loop:
        - Find the MIN token for the first item in all partial_index jsons
        - If there are multiple MIN tokens (meaning they are the same word), then combine the tuples mapped to that token(word)
            - The mapped tuples should be sorted by docID
        - Pop the min token(s) and write them to final_inverted_index.json
    '''
    final_index = defaultdict(list)

    for i in range(self.partial_index_count):
        partial_index_path = f'partial_index_{i}.json'

        with open(partial_index_path, 'r', encoding='utf-8') as file:
            partial_index = json.load(file)
            for token, postings in partial_index.items():
                final_index[token].extend(postings)
        os.remove(partial_index_path)  # Cleanup

    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(final_index, file)"""


def merge_partial_indexes(self, output_file="final_inverted_index.json"):
    final_index = defaultdict(list)
    
    # List to hold the file handlers and the tokens from each partial index
    partial_indexes = []
    for i in range(self.partial_index_count):
        partial_index_path = f'partial_index_{i}.json'
        with open(partial_index_path, 'r', encoding='utf-8') as file:
            partial_index = json.load(file)
            partial_indexes.append(iter(sorted(partial_index.items())))  # Sorted token list for each partial index

    # Priority queue to efficiently merge
    pq = []

    # Initialize the priority queue with the first token from each partial index
    for idx, partial_index_iter in enumerate(partial_indexes):
        token, postings = next(partial_index_iter, (None, None))
        if token:
            heapq.heappush(pq, (token, idx, postings, partial_index_iter))  # Push (token, index, postings, iterator)

    # Merge the partial indexes
    while pq:
        # Get the smallest token from the priority queue
        token, idx, postings, partial_index_iter = heapq.heappop(pq)

        # Add the token and its postings to the final index
        final_index[token].extend(postings)

        # Move the iterator for that partial index and push the next token if it exists
        next_token, next_postings = next(partial_index_iter, (None, None))
        if next_token:
            heapq.heappush(pq, (next_token, idx, next_postings, partial_index_iter))

    # Write the merged final index to a JSON file
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(final_index, file)

    # Clean up the partial index files
    for i in range(self.partial_index_count):
        os.remove(f'partial_index_{i}.json')  # Delete partial index file after merging



    
if __name__ == "__main__":
    dev_path = Path("PracticeDev")
    index = InvertedIndex(dev_path)
    index.run_indexer()
    
    
    
