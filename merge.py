import heapq
import json
from pathlib import Path
import os
import math


def merge_partial_indices(output_file="final_inverted_index.json", url_map_file = "url_map.json"):
    lowest_names = []     # json_name : word
    indexed_documents = set() # track unique document ids
    token_counter = 0 # count unique tokens 
    

    with open(url_map_file, 'r', encoding='utf-8') as f:
        url_map = json.load(f)
        
    url_count = len(url_map)
    for json_file in Path("partial_index_folder").iterdir():
        with open(json_file, 'r') as file:
            data = json.load(file)
            json_file_name = os.path.basename(json_file)  # Get the name of the file without path
            json_iterator = iter(data.items())

            # Push the first term from the iterator into the heap
            #first_term, first_postings = next(json_iterator)

            try:
                first_term, first_postings = next(json_iterator)
                heapq.heappush(lowest_names, (first_term, json_file_name, json_iterator, first_postings))  # Push the first term
            except StopIteration:
                print(f"File {json_file} is empty or does not contain valid data: {data}")
                continue  # If the file is empty, skip it
        
            #heapq.heappush(lowest_names, (first_term, json_file_name, json_iterator, first_postings))    # Append it to lowest_names_heap as (word, json_file)
            

    with open(output_file, 'w', encoding = 'utf-8') as out_file:
        current_term = None
        current_posting = []
        while lowest_names:     # while there are still terms in any of our partial_index jsons
            try:
                term, json_file, json_iterator, posting = heapq.heappop(lowest_names)

            except TypeError as e:
                print(f"Error during heap operation: {e}")
                #print(f"Contents causing error: {lowest_names}")
                break
            
            # Found duplicate MIN word, need to combine both
            if term == current_term: # when the the current term is the same as the term we have saved  
                current_posting.extend(posting)

                try:
                    # already found the equal term, so we would go to the next term in the lowerst_names
                    next_term, next_postings = next(json_iterator) # json_iterator = json_iterator
                    heapq.heappush(lowest_names, (next_term, json_file, json_iterator, next_postings))
                except StopIteration:
                    print("WE FINISHED A FILE WOOOO")
                continue

            elif not current_term: # when the current term is not defined, we give the term we are looking at to the current term 
                current_term = term 
                current_posting = posting
            
            else: # when we couldn't find a match for the term we are looking at rn :(
                #heapq.heappush(lowest_names, (term, json_file, json_iterator, posting)) # pushes the term and posting back into the heap
                
                # counting tfidf 
                
                # total # of docs  -> len(utils.url_map)
                # total num of docs with that specific word -> len(current_posting)
                # term frequency of that specific doc -> current[posting]
                for i, post in enumerate(current_posting):
                    tf = 1 + math.log(post["term_frequency"])
                    idf = math.log(url_count / len(current_posting))
                    tf_idf = tf * idf
                    current_posting[i]["tf_idf"] = tf_idf

                current_posting.sort(key=lambda x: x["tf_idf"], reverse=True)

                if current_term:
                    # Dumps "token" : [{"url1" : "url", "freq1" : freq}         , {url2, freq2}, ...]
                    json.dump({current_term: current_posting}, out_file)
                    out_file.write('\n')
            
                
                current_term = term 
                current_posting = posting
                token_counter += 1 #unique word found

            for entries in posting:
                indexed_documents.add(entries["doc_id"]) # add the doc id to the indexed_documents
            
            try:
                next_term, next_postings = next(json_iterator) # json_iterator = json_iterator
                heapq.heappush(lowest_names, (next_term, json_file, json_iterator, next_postings))
            except StopIteration:
                print("WE FINISHED A FILE WOOOO")
                pass  # File is fully 
                