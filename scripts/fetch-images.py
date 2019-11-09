#!/usr/bin/env python3

import os
import csv
import json
from dotenv import load_dotenv
from pathlib import Path

# Load API Key from .env in project root dir
env_path = Path('..') / '.env'
load_dotenv(dotenv_path=env_path)

tmdb_api_key = os.getenv('TMDB_API_KEY')
if tmdb_api_key is None:
    print('No TMDB API Key found. Please set the TMDB_API_KEY environmental variable and try again.')

base_img_url = 'https://image.tmdb.org/t/p/w500'
movies_metadata_path = '../data/the-movies-dataset/movies_metadata.csv'

with open(movies_metadata_path, 'r') as movies_metadata:
    reader = csv.reader(movies_metadata)
    next(reader) # trash header
    
    count = 1
    for row in reader:
        collection_str = row[1]
        if collection_str:
            collection_str = collection_str.replace('\'', '"')
            try:
                collection = json.loads(collection_str)
                print(collection["poster_path"])
            except json.JSONDecodeError:
                print('decoding error')

        count += 1
        print(count)
