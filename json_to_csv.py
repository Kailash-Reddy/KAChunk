import json
import csv
import pandas as pd

def json_file_to_csv(json_filepath, csv_filepath):
    """
    Convert a JSON file to CSV
    Supports JSON files with list of dictionaries or single dictionary
    """
    # Read JSON file
    with open(json_filepath, 'r') as jsonfile:
        data = json.load(jsonfile)
    
    # Ensure data is a list
    if not isinstance(data, list):
        data = [data]
    
    # Convert using pandas for robust handling
    df = pd.json_normalize(data)
    df.to_csv(csv_filepath, index=False)
    
    print(f"Converted {json_filepath} to {csv_filepath}")

# Example usage
json_file_to_csv('/home/kailash/ChunKanon/KanonMedicalData.json', 'KanonMedicalData.csv')