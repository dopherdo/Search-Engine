
# {{Old implementation -- replaced by Michael's ChatGPT thing
"""def merge_partial_indices(self, output_file="final_inverted_index.json"): 
    '''
    Merges all partial indices into a final json file (disk)
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


def create_final_index_file():
    pass

def merge_partial_indices(self, output_file="final_inverted_index.json"):
    final_index = defaultdict(list)

    partial_indices = []
    for i in range(self.partial_index_count):
        partial_index_path = f'partial_indices/partial_index_{i}.json'
        with open(partial_index_path, 'r', encoding='utf-8') as file:
            partial_index = json.load(file)
            # Sort the token list and create an iterator for each partial index
            partial_indices.append(iter(partial_index.items()))  

    # WORK  IN PROGRESS 
    '''
    NOTES: 
    - all json files are in partial_indices folder 
    - partial_indices holds teh list of indices with postings 
    - take the first of every partial_indices item and merge toegther and then place in the final json file 
    - merge sort using multi way sort 
    '''
