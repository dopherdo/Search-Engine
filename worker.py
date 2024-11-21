from collections import defaultdict, Counter
from threading import Thread
from bs4 import BeautifulSoup # type: ignore
import json
import os

class Worker(Thread):
    def __init__(self, dev_path, utils):
        super().__init__(daemon=True)
        self.directory = dev_path
        self.inverted_index = defaultdict(list) #stores inverted index
        self.batch_count = 100   # Set to 100 to 
        self.utils = utils
        self.disk_write_count = 0
        self.current_url = ""


    # open the files in a folder and get the text
    def process_html_file(self, filepath):
        with open(filepath, 'r', encoding='utf-8') as file:
            json_data = json.loads(file.read())
        
        self.current_url = json_data['url']
        # Get the HTML content from the JSON
        html_content = json_data['content']
        
        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')
        text = soup.get_text()
        tokens = self.tokenize(text)
    
        return tokens

    def tokenize(self, text):
        tokens = []
        current_token = ''
        for char in text: 
            if char.isalnum() and char.isascii():  # Alphanumeric characters only
                letter = char
                current_token += letter
            else: 
                if len(current_token) > 1:
                    token_lower = current_token.lower()  # Convert the token to lowercase
                    tokens.append(token_lower)
                    current_token = ''
        return tokens
    
    # When batch count is reached, save the partial index into the index_directory
    def download_partial_index(self):
        directory_name = os.path.basename(self.directory)
        index_path = os.path.join(self.utils.partial_index_directory, f"{directory_name}_{self.disk_write_count}.json")
        for token in self.inverted_index:
            self.inverted_index[token].sort(key=lambda x: x["term_frequency"], reverse=True)
        # Write to JSON file with sorted keys
        with open(index_path, 'w', encoding='utf-8') as file:
            json.dump(self.inverted_index, file, sort_keys=True)
        
        # Clear the index after saving
        self.inverted_index.clear()

    def create_partial_indices(self): #creates partial index with both the docID and   
        '''
        Each Worker Thread will run this on a folder ex. aiclub_ics_uci
        '''
        sub_directory = self.directory
        file_count = 0  # files that we went through 
        if sub_directory.is_dir(): #check if the sub directory exists           
            for json_file in sub_directory.glob('*.json'):
                file_count += 1
                # lock it to access teh docID 
                tokens = self.process_html_file(json_file)
                
                # print(f"file_count: {file_count} : {json_file}")
                doc_ID = self.utils.increment_docID(self.current_url, len(tokens))
                
                token_counts = Counter(tokens) 

                
                for token in sorted(token_counts.keys()):
                    word_freq = token_counts[token]
                    self.inverted_index[token].append({"doc_id": doc_ID, "term_frequency": word_freq}) 
                        
                if file_count >= self.batch_count:
                    self.disk_write_count += 1
                    self.download_partial_index()
                    file_count = 0
            
            if file_count > 0:
                self.disk_write_count += 1
                self.download_partial_index()
    
    def run(self):
        self.create_partial_indices()