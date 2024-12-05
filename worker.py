from collections import defaultdict, Counter
from threading import Thread
from bs4 import BeautifulSoup # type: ignore
import json
import os
import hashlib
import re
from datasketch import MinHash
from nltk.stem import PorterStemmer


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
        
        self.current_url = json_data['url']
        # Get the HTML content from the JSON
        html_content = json_data['content']
        
        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')
        
        if self.is_duplicate(soup, self.utils):
            return list()
        
        
        # PageRank: Extract outgoing links
        outgoing_links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            href = self.utils.normalize_url(href, self.current_url)
            outgoing_links.add(href)
        
        # Update the link graph in Utils
        self.utils.update_link_graph(self.current_url, outgoing_links)
    
    
        # Prepare lists to store words with their multipliers
        weighted_words = []
        
        # Add title words with high multiplier
        title = soup.find('title')
        if title:
            weighted_words.extend([(word, 5) for word in title.get_text().split()])
        
        # Add header words with decreasing multipliers
        headers = soup.find_all(['h1', 'h2', 'h3'])
        for header in headers:
            header_multiplier = 4 if header.name == 'h1' else 3 if header.name == 'h2' else 2
            weighted_words.extend([(word, header_multiplier) for word in header.get_text().split()])
        
        # Add bold words with moderate multiplier
        bold_texts = soup.find_all(['b', 'strong'])
        for bold in bold_texts:
            weighted_words.extend([(word, 2) for word in bold.get_text().split()])
        
        # Add main body text with multiplier of 1
        body_words = soup.get_text().split()
        weighted_words.extend([(word, 1) for word in body_words])
        
        # Tokenize with positions and multipliers
        tokens_with_positions = []
        current_position = 0
        
        for word, multiplier in weighted_words:
            # Tokenize each word
            tokenized_word = self.tokenize(word)
            
            # If the word tokenizes to something valid
            if tokenized_word:
                # Repeat the word based on its multiplier
                for _ in range(multiplier):
                    token = tokenized_word[0]
                    tokens_with_positions.append({
                        'token': token,
                        'position': current_position
                    })
                    current_position += 1
        
        return tokens_with_positions
    
    def is_duplicate(self, soup, utils, perms=128):
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

    def create_partial_indices(self):
        '''
        Each Worker Thread will run this on a folder ex. aiclub_ics_uci
        '''
        sub_directory = self.directory
        file_count = 0  # files that we went through 
        
        if sub_directory.is_dir(): #check if the sub directory exists           
            for json_file in sub_directory.glob('*.json'):
                file_count += 1
                
                # Process the HTML file and get tokens with positions
                tokens_with_positions = self.process_html_file(json_file)

                if not tokens_with_positions:
                    continue

                # Increment the document ID
                doc_ID = self.utils.increment_docID(self.current_url, len(tokens_with_positions))
                
                # Count total occurrences of each token
                token_counts = Counter(token_info['token'] for token_info in tokens_with_positions)
                
                # Group positions for each token
                token_positions = {}
                for token_info in tokens_with_positions:
                    token = token_info['token']
                    position = token_info['position']
                    
                    if token not in token_positions:
                        token_positions[token] = []
                    token_positions[token].append(position)
                
                # Update the inverted index
                for token, freq in token_counts.items():
                    self.inverted_index[token].append({
                        "doc_id": doc_ID, 
                        "term_frequency": freq,  # Actual number of times the token appears
                        "positions": token_positions[token]
                    }) 
                        
                if file_count >= self.batch_count:
                    self.disk_write_count += 1
                    self.download_partial_index()
                    file_count = 0
            
            # Handle any remaining files
            if file_count > 0:
                self.disk_write_count += 1
                self.download_partial_index()
    
    def run(self):
        self.create_partial_indices()