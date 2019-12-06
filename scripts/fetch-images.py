#!/usr/bin/env python3

import multiprocessing as mp
import os, sys
import csv, json
import urllib.request
from tqdm import tqdm

currentPath = os.path.dirname(os.path.realpath(__file__))
baseImgUrl = 'https://image.tmdb.org/t/p/w92'
moviesMetadataPath = os.path.join(currentPath, '../data/the-movies-dataset/movies_metadata.csv')

def saveImage(posterPath):
    posterUrl = baseImgUrl + posterPath
    posterFilePath = os.path.join(currentPath, '../data/img/', posterPath[1:])

    if not os.path.exists(posterFilePath):
        # if img isn't already cached
        with open(posterFilePath, 'w'): # create file if it doesn't exist
            pass

        try:
            urllib.request.urlretrieve(posterUrl, posterFilePath)
        except:
            pass # I know, I'm terrible

with open(moviesMetadataPath, 'r') as moviesMetadata:
    reader = csv.DictReader(moviesMetadata)
    next(reader) # trash header
    posterPaths = [row['poster_path'] for row in reader if row['poster_path']]

    with mp.Pool() as p:
        tqdm(p.imap_unordered(saveImage, posterPaths, 20), total=len(posterPaths))
