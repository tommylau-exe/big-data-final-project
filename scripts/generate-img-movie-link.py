#!/usr/bin/env python3

import os, sys
import csv, json
import urllib.request

currentPath = os.path.dirname(os.path.realpath(__file__))
moviesMetadataPath = os.path.join(currentPath, '../data/the-movies-dataset/movies_metadata.csv')
imgMovieLinkPath = os.path.join(currentPath, '../data/etc/img_movie_link.csv')

with open(moviesMetadataPath, 'r') as moviesMetadata, open(imgMovieLinkPath, 'w') as imgMovieLink:
    reader = csv.DictReader(moviesMetadata)
    next(reader) # trash header
    writer = csv.writer(imgMovieLink)
    writer.writerow(['img_id', 'title'])
    
    for row in reader:
        posterPath = row['poster_path']
        if posterPath:
            imgId = os.path.splitext(posterPath)[0][1:]
            title = row['title']

            writer.writerow([imgId, title])
