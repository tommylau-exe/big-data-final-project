#!/usr/bin/env python3

import os, sys
import csv, json
import urllib.request

currentPath = os.path.dirname(os.path.realpath(__file__))
baseImgUrl = 'https://image.tmdb.org/t/p/w500'
moviesMetadataPath = os.path.join(currentPath, '../data/the-movies-dataset/movies_metadata.csv')

with open(moviesMetadataPath, 'r') as moviesMetadata:
    reader = csv.reader(moviesMetadata)
    next(reader) # trash header
    
    imgCount = 0
    errCount = 0
    totalCount = 0
    for row in reader:
        collectionStr = row[1]
        if collectionStr:
            collectionStr = collectionStr.replace('\'', '"') # make into valid JSON
            posterPath = None

            try:
                collection = json.loads(collectionStr)
                posterPath = collection['poster_path']
                imgCount += 1
            except json.JSONDecodeError:
                errCount += 1
            except:
                pass

            if posterPath is not None: # if no errors
                posterUrl = baseImgUrl + posterPath
                resp = urllib.request.urlopen(posterUrl)

                if resp.code != 200:
                    print(f'img {imgCount}: response code {resp.code}')
                else:
                    posterFilePath = os.path.join(currentPath, '../data/img/', posterPath[1:])
                    print(f'img {imgCount}: saving to', posterFilePath)

                    if not os.path.exists(posterFilePath):
                        with open(posterFilePath, 'w'): # create file if it doesn't exist
                            pass

                    urllib.request.urlretrieve(posterUrl, posterFilePath)

        totalCount += 1

    imgPct = (imgCount / totalCount) * 100
    errPct = (errCount / totalCount) * 100
    print(f'Found {imgCount} images ({imgPct:.2f}%) out of {totalCount} data points with {errCount} parse errors ({errPct:.2f}%)')
