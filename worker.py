from collections import defaultdict
from threading import Thread
from bs4 import BeautifulSoup # type: ignore


class Worker(Thread):
    def __init__(self, dev_path, docID):
         self.directory = dev_path
         self.inverted_index = defaultdict(list) #stores inverted index
         self.batch_count = 5   # Set to 100 to 
         self.partial_index_count = 0

    # open the files in a folder and get the text
    def process_html_file(self, filepath):
        with open(filepath, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file, 'html.parser')
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
                if len(current_token) > 0:
                    token_lower = current_token.lower()  # Convert the token to lowercase
                    tokens.append(token_lower)
                    current_token = ''
        return tokens
    
    # saves the partial index
    def download_partial_index(self):
        index_path = f"partial_index_{self.partial_index_count}.json"
        with open(index_path, 'w', encoding='utf-8') as file:
            json.dump(self.inverted_index, file)
        self.partial_index_count += 1
        self.inverted_index.clear()

    def create_partial_indexes(self, sub_directory): #creates partial index with both the docID and   
        '''
        Each Worker Thread will run this on a folder ex. aiclub_ics_uci
        - It will 
        '''
        file_count = 0  # files that we went through 
        if sub_directory.is_dir(): #check if the sub directory exists                
            for json_file in sub_directory.glob('*.json'):
                file_count += 1
                # lock it to access teh docID 
                docId += 1
                tokens = self.process_html_file(json_file)
                token_counts = Counter(tokens) 

                
                for token in sorted(token_counts.keys()):
                    word_freq = token_counts[token]
                    self.inverted_index[token].append({"doc_id": doc_ID, "term_frequency": word_freq}) 
                        
                if file_count >= self.batch_count:
                    self.download_partial_index()
                    file_count = 0
                        