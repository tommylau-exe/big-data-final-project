import multiprocessing as mp
import os
import csv, json
from tqdm import tqdm

currentPath = os.path.dirname(os.path.realpath(__file__))
moviesMetadataPath = os.path.join(currentPath, '../data/the-movies-dataset/movies_metadata.csv')
moviesGenrePath = os.path.join(currentPath, '../data/etc/movies_genre.csv')

def generateOutputRows(inputRow):
  movieId = inputRow['id']
  genres = json.loads(inputRow['genres'].replace('\'', '"'))
  if not genres:
    return []
  
  return [[movieId, genre['name']] for genre in genres]

with open(moviesMetadataPath, 'r') as moviesMetadata, open(moviesGenrePath, 'w') as moviesGenre:
    reader = csv.DictReader(moviesMetadata)
    next(reader) # trash header
    writer = csv.writer(moviesGenre)
    writer.writerow(['id','genre'])
    inputRows = list(reader)
    
    with mp.Pool() as p:
      rows = tqdm(p.imap_unordered(generateOutputRows, inputRows, 10), total = len(inputRows))
      outputRows = [row for rowGroup in rows for row in rowGroup]
      writer.writerows(outputRows)
