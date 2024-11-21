import json
import itertools

import json

def create_prefix_position_index(json_file_path, output_file_path):
    """
    Creates a JSON file mapping each unique first two characters of tokens
    to the byte position of their first occurrence in the given sorted JSON file.
    """
    prefix_positions = {}  # Dictionary to store prefix and their byte positions
    current_prefix = None  # Track the current prefix (first two characters of token)
    byte_position = 0  # Initialize byte position counter

    with open(json_file_path, 'r') as f:
        while True:
            line = f.readline()
            if not line:  # End of file
                break

            token_start = line.find('"') + 1  # Locate the start of the token
            token_end = line.find('"', token_start)  # Locate the end of the token
            if token_start == -1 or token_end == -1:
                continue  # Skip lines that don't contain a valid token

            token = line[token_start:token_end]  # Extract the token
            prefix = token[:2]  # Get the first two characters of the token

            if prefix != current_prefix:
                # Found a new unique prefix, store the byte position
                prefix_positions[prefix] = byte_position
                current_prefix = prefix  # Update the current prefix

            byte_position += len(line)  # Update the byte position

    # Save the prefix positions to a file
    with open(output_file_path, 'w') as f:
        json.dump(prefix_positions, f, indent=2)



# Example usage:
if __name__ == "__main__":
    # First, create the prefix index (only need to do this once)
    create_prefix_position_index('final_inverted_index.json', 'prefix_positions.json')
    
    # Then, you can perform searches using the index
    # Example: search for documents containing "cristina" and "computer"
    #search_terms = ['cristina', 'computer']
    #results = boolean_AND_search(search_terms, 
    #                           'final_inverted_index.json',
    #                           'token_positions.json')
    #print(f"Documents containing all terms {search_terms}: {results}")