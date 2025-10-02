import pandas as pd
import os
from glob import glob


def prepare_new_test(list_of_files, num_records_per_file):
    """
    Prepares a new test dataset by splitting existing CSV files into smaller chunks.
    Each chunk will contain a specified number of records.
    """
    output_dir = "./test_data_split/"
    os.makedirs(output_dir, exist_ok=True)
    
    for file_path in list_of_files:
        df = pd.read_csv(file_path)
        num_chunks = (len(df) + num_records_per_file - 1) // num_records_per_file
        
        for i in range(num_chunks):
            chunk = df[i * num_records_per_file : (i + 1) * num_records_per_file]
            base_name = os.path.basename(file_path)
            chunk_file_name = f"{os.path.splitext(base_name)[0]}_part{i+1}.csv"
            chunk.to_csv(os.path.join(output_dir, chunk_file_name), index=False)
            print(f"Created: {os.path.join(output_dir, chunk_file_name)}")

if __name__ == "__main__":
    prepare_new_test(num_records_per_file=20000,
                     list_of_files=glob("test_data/*.csv"))