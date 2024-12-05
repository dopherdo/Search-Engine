from collections import defaultdict, Counter
from threading import Thread
from bs4 import BeautifulSoup # type: ignore
import json
import os
import hashlib
import re
from datasketch import MinHash
from nltk.stem import PorterStemmer
import utils


class Worker(Thread):
    def __init__(self, dev_path, utils):
        super().__init__(daemon=True)
        self.directory = dev_path
        self.inverted_index = defaultdict(list) #stores inverted index
        self.batch_count = 100   # Set to 100 to 
        self.utils = utils
        self.disk_write_count = 0
        self.current_url = ""
        self.stemmer = PorterStemmer()


    # open the files in a folder and get the text
    def process_html_file(self, filepath):
        with open(filepath, 'r', encoding='utf-8') as file:
            json_data = json.loads(file.read())
        #loop through line by line 
        # - check if 
        self.current_url = json_data['url']
        # Get the HTML content from the JSON
        html_content = json_data['content']
        
        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')
        if self.is_duplicate(soup, self.utils):
            return list()
        
        # Function to repeat words 3 more times
        def repeat_words(tags):
            return ' '.join([word.get_text() * 3 for word in tags])
        
        # Extract words from bold, headers, and title
        bold_words = soup.find_all(['b', 'strong'])
        header_words = soup.find_all(['h1', 'h2', 'h3'])
        title_words = soup.find('title')
        
        # Repeat the extracted words
        bold_repeated = repeat_words(bold_words)
        header_repeated = repeat_words(header_words)
        title_repeated = title_words.get_text() * 3 if title_words else ''
        
        # Get the rest of the text
        text = soup.get_text()
        
        # Combine the repeated words with the normal text
        final_text = bold_repeated + ' ' + header_repeated + ' ' + title_repeated + ' ' + text
        
        # Tokenize the final combined text
        tokens = self.tokenize(final_text)
    
        return tokens
    
    def is_duplicate(self, soup, utils, threshold=0.85, perms=128):
        content = str(soup)
        # Generate a SHA-256 hash of the content
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        # Check for duplicate by verifying if hash is in `seen_hashes`
        if utils.check_duplicate_hash(content_hash):
            return True # Duplicate content
        
        # Get clean text
        text = soup.get_text(separator=' ', strip=True).lower()
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Create MinHash for current content
        minhash = MinHash(num_perm=perms)
        for i in range(len(text) - 3 + 1):
            shingle = text[i:i + 3]
            minhash.update(shingle.encode('utf-8'))
        
        # Check for near duplicates using `query`
        similar_docs = utils.similar_docs(minhash)
        
        # If similar documents are found, consider them duplicates
        if similar_docs:
            return True
        

        utils.add_seen_hashes(content_hash)
        utils.lsh_insert(minhash)
        return False


    def tokenize(self, text):
        tokens = []
        current_token = ''
        for char in text:
            if char.isalnum() and char.isascii():  # Alphanumeric characters only
                current_token += char
            else:
                if len(current_token) > 1:
                    token_lower = current_token.lower()  # Convert to lowercase
                    stemmed_token = self.stemmer.stem(token_lower)  # Apply Porter Stemming
                    tokens.append(stemmed_token)  # Append the stemmed token
                    current_token = ''  # Reset for the next token
        if len(current_token) > 1:  # In case the text ends without a non-alphanumeric char
            token_lower = current_token.lower()
            stemmed_token = self.stemmer.stem(token_lower)
            tokens.append(stemmed_token)
        
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
                
                # lock it to access the docID 
                tokens = self.process_html_file(json_file)

                if not tokens:
                    continue

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