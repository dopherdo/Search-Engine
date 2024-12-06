from partial_indexer import PartialIndexer
from pathlib import Path
from merge import merge_partial_indices
import os
import shutil
from boolean_retrieval import create_prefix_position_index
import utils
from utils import Utils


def create_partial_index_folder(folder_name):
    # Check if the folder exists
    if os.path.exists(folder_name):
        # Remove the folder and all of its contents
        try:
            shutil.rmtree(folder_name)
        except Exception as e:
            print(f'Failed to delete {folder_name}. Reason: {e}')
    
    # Create the folder again
    os.makedirs(folder_name)
    return folder_name


def main():
    '''
    Runs the Indexer
    '''
    file_path = Path("PracticeDev")
    partial_index_path = Path("partial_index_folder")
    index_directory = create_partial_index_folder(partial_index_path)
    # path = Path("DEV")
    utils = Utils(index_directory)
    indexer = PartialIndexer(file_path, utils)
    indexer.create_workers()

    merge_partial_indices(utils=utils) # pass in the index directory

    create_prefix_position_index('final_inverted_index.json', 'prefix_positions.json')
    
    
if __name__ == "__main__":
    main()