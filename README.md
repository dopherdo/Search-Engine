# Search-Engine

Raw HTML File
- Beautiful Soup to parse this file and turn it into useable raw words

import os
import re
import json
from collections import defaultdict, Counter
from bs4 import BeautifulSoup
from glob import glob

class Posting:
    def __init__(self, doc_id, term_frequency):
        self.doc_id = doc_id
        self.term_frequency = term_frequency

    def to_dict(self):
        return {"doc_id": self.doc_id, "term_frequency": self.term_frequency}

class InvertedIndex:
    def __init__(self, directory, batch_size=100):
        self.directory = directory
        self.batch_size = batch_size
        self.index = defaultdict(list)
        self.partial_index_count = 0

    def tokenize(self, text):
        text = re.sub(r'\W+', ' ', text.lower())  # Simple tokenizer
        return text.split()

    def process_html_file(self, filepath):
        with open(filepath, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file, 'html.parser')
            text = soup.get_text()
            tokens = self.tokenize(text)
            return tokens

    def create_partial_index(self, documents):
        for doc_id, tokens in documents.items():
            term_frequencies = Counter(tokens)
            for token, freq in term_frequencies.items():
                self.index[token].append(Posting(doc_id, freq).to_dict())
        
        self.save_partial_index()

    def save_partial_index(self):
        partial_index_path = f'partial_index_{self.partial_index_count}.json'
        with open(partial_index_path, 'w', encoding='utf-8') as file:
            json.dump(self.index, file)
        self.index.clear()  # Clear memory
        self.partial_index_count += 1

    def build_partial_indexes(self):
        documents = {}
        filepaths = glob(os.path.join(self.directory, '*.html'))

        for i, filepath in enumerate(filepaths, start=1):
            doc_id = os.path.basename(filepath)
            tokens = self.process_html_file(filepath)
            documents[doc_id] = tokens

            if i % self.batch_size == 0:
                self.create_partial_index(documents)
                documents.clear()  # Clear batch for next load

        if documents:
            self.create_partial_index(documents)

    def merge_partial_indexes(self, output_file="final_inverted_index.json"):
        final_index = defaultdict(list)

        for i in range(self.partial_index_count):
            partial_index_path = f'partial_index_{i}.json'
            with open(partial_index_path, 'r', encoding='utf-8') as file:
                partial_index = json.load(file)
                for token, postings in partial_index.items():
                    final_index[token].extend(postings)
            os.remove(partial_index_path)  # Cleanup

        with open(output_file, 'w', encoding='utf-8') as file:
            json.dump(final_index, file)

    def run(self):
        print("Building partial indexes...")
        self.build_partial_indexes()
        print("Merging partial indexes...")
        self.merge_partial_indexes()
        print("Indexing complete!")

# Usage example:
directory = "path/to/html/files"
inverted_index = InvertedIndex(directory)
inverted_index.run()


