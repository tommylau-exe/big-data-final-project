#!/usr/bin/env python3

import os, sys
import csv, json
import urllib.request

currentPath = os.path.dirname(os.path.realpath(__file__))
baseImgUrl = 'https://image.tmdb.org/t/p/w500'
moviesMetadataPath = os.path.join(currentPath, '../data/the-movies-dataset/movies_metadata.csv')

with open(moviesMetadataPath, 'r') as moviesMetadata:
    reader = csv.DictReader(moviesMetadata)
    next(reader) # trash header
    
    imgCount = 0
    totalCount = 0
    for row in reader:
        posterPath = row['poster_path']
        if posterPath:
            posterUrl = baseImgUrl + posterPath
            posterFilePath = os.path.join(currentPath, '../data/img/', posterPath[1:])

            if not os.path.exists(posterFilePath):
                # if img isn't already cached
                with open(posterFilePath, 'w'): # create file if it doesn't exist
                    pass

                try:
                    urllib.request.urlretrieve(posterUrl, posterFilePath)
                    print(f'img {imgCount}: saving to', posterFilePath)
                    imgCount += 1
                except:
                    pass
            else:
                print(f'img {imgCount}: already cached at', posterFilePath)
                imgCount += 1

        totalCount += 1

    imgPct = (imgCount / totalCount) * 100
    print(f'Found {imgCount} images out of {totalCount} ({imgPct:.2f}%)')
