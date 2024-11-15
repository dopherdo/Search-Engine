from partial_indexer import PartialIndexer
from pathlib import Path
from merge import merge_partial_indices
import os
import shutil

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
    file_path = Path("DEV")
    partial_index_path = Path("partial_index_folder")
    index_directory = create_partial_index_folder(partial_index_path)
    # path = Path("DEV")
    indexer = PartialIndexer(file_path, index_directory)
    indexer.create_workers()

    merge_partial_indices() # pass in the index directory
    
    
if __name__ == "__main__":
    main()