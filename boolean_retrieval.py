import json

def create_token_position_index(json_file_path, output_file_path):
    """
    Create an index of where each letter/number token starts in the JSON file.
    For example, "00", "01", "a0", "aa", etc.
    """
    token_positions = {}
    
    # Generate all possible letter/number combinations we might find
    # Numbers: "00" through "99"
    for i in range(100):
        token_positions[f"{i:02d}"] = None
    
    # Single letters: "a" through "z"
    for i in range(97, 123):
        token_positions[chr(i)] = None
        
    # Two letters: "aa" through "zz"
    for i in range(97, 123):
        for j in range(97, 123):
            token_positions[chr(i) + chr(j)] = None
            
    # Letter-number combinations: "a0" through "z9"
    for i in range(97, 123):
        for j in range(10):
            token_positions[chr(i) + str(j)] = None
    
    # Now find the actual positions in the file
    with open(json_file_path, 'r') as f:
        # Skip the opening brace
        f.read(1)
        
        while True:
            pos = f.tell()
            chunk = f.read(4)  # Read enough to capture the token
            
            if not chunk:
                break
            
            # Reset to position before chunk read
            f.seek(pos)
            
            # When we find a quote, we're at the start of a token
            if '"' in chunk:
                # Skip to the opening quote
                while f.read(1) != '"':
                    continue
                
                # Read the token
                token = ""
                char = f.read(1)
                while char != '"':
                    token += char
                    char = f.read(1)
                
                # If this is one of our tokens, store its position
                if token in token_positions:
                    token_positions[token] = pos
                
                # Skip to the end of this posting list
                bracket_count = 0
                found_first_bracket = False
                while True:
                    char = f.read(1)
                    if not char:
                        break
                    if char == '[':
                        found_first_bracket = True
                        bracket_count += 1
                    elif char == ']' and found_first_bracket:
                        bracket_count -= 1
                        if bracket_count == 0:
                            break
            else:
                f.read(1)  # Move forward if we didn't find a quote
    
    # Remove tokens that weren't found in the file
    token_positions = {k: v for k, v in token_positions.items() if v is not None}
    
    # Save the token positions to a new file
    with open(output_file_path, 'w') as f:
        json.dump(token_positions, f)

def find_term_in_prefix_section(file_handle, prefix_pos, target_term):
    """
    Search for a specific term within a prefix section.
    Returns the posting list if found, None otherwise.
    """
    file_handle.seek(prefix_pos)
    
    while True:
        # Find next term
        while True:
            char = file_handle.read(1)
            if not char or char == '"':
                break
        
        if not char:  # End of file
            return None
            
        # Read the term
        term = ""
        char = file_handle.read(1)
        while char != '"':
            term += char
            char = file_handle.read(1)
        
        # If we've moved past our prefix section, term not found
        if not term.startswith(target_term[:2]):
            return None
            
        # If we found our term, return its posting list
        if term == target_term:
            # Skip to opening bracket
            while True:
                char = file_handle.read(1)
                if char == '[':
                    break
            
            # Read the posting list
            posting_list_str = '['
            bracket_count = 1
            
            while bracket_count > 0:
                char = file_handle.read(1)
                posting_list_str += char
                if char == '[':
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
            
            return json.loads(posting_list_str)
        
        # Skip this term's posting list
        bracket_count = 0
        found_first_bracket = False
        while True:
            char = file_handle.read(1)
            if not char:
                return None
            if char == '[':
                found_first_bracket = True
                bracket_count += 1
            elif char == ']' and found_first_bracket:
                bracket_count -= 1
                if bracket_count == 0:
                    break

def boolean_AND_search(terms, index_file_path, positions_file_path):
    """
    Perform boolean AND search using the prefix positions to speed up term lookup.
    """
    # Load the prefix positions
    with open(positions_file_path, 'r') as f:
        prefix_positions = json.load(f)
    
    # Get posting lists for all terms
    posting_lists = []
    with open(index_file_path, 'r') as f:
        for term in terms:
            if len(term) < 2:
                continue
                
            prefix = term[:2]
            if prefix not in prefix_positions:
                return []  # If any term's prefix isn't found, return empty list
            
            # Find the term's posting list in its prefix section
            posting_list = find_term_in_prefix_section(f, prefix_positions[prefix], term)
            if posting_list is None:
                return []  # If any term isn't found, return empty list
                
            posting_lists.append(posting_list)
    
    # If we didn't find all posting lists, return empty list
    if len(posting_lists) != len(terms):
        return []
    
    # Convert first posting list to set of doc_ids
    result_set = set(post['doc_id'] for post in posting_lists[0])
    
    # Intersect with all other posting lists
    for posting_list in posting_lists[1:]:
        current_set = set(post['doc_id'] for post in posting_list)
        result_set = result_set.intersection(current_set)
    
    return sorted(list(result_set))

# Example usage:
if __name__ == "__main__":
    # First, create the prefix index (only need to do this once)
    #create_token_position_index('final_inverted_index.json', 'prefix_positions.json')
    
    # Then, you can perform searches using the index
    # Example: search for documents containing "cristina" and "computer"
    search_terms = ['cristina', 'computer']
    results = boolean_AND_search(search_terms, 
                               'final_inverted_index.json',
                               'prefix_positions.json')
    print(f"Documents containing all terms {search_terms}: {results}")