#!/usr/bin/env python3

import multiprocessing as mp
from PIL import Image
import os
import csv
import colorsys
from tqdm import tqdm

currentPath = os.path.dirname(os.path.realpath(__file__))
moviesMetadataPath = os.path.join(currentPath, '../data/the-movies-dataset/movies_metadata.csv')
imgStatsPath = os.path.join(currentPath, '../data/etc/img_stats.csv')
imgDirPath = os.path.join(currentPath, '../data/img')

def avgTups(tups):
    if len(tups) is 0:
        return ()

    avg = [0 for _ in tups[0]]
    for e in tups:
        avg = [sum(z) for z in zip(avg,e)]
    cnt = len(tups)
    avg = [e/cnt for e in avg]

    return tuple(avg)

def calcStats(posterPath):
    imgId = os.path.splitext(posterPath)[0]
    posterPath = os.path.join(imgDirPath, posterPath)

    try:
        img = Image.open(posterPath).getdata() # rgb pixels in image
        avgRgb = avgTups(img)
        img = [colorsys.rgb_to_hsv(*rgb) for rgb in img]
        avgHsv = avgTups(img)

        return (imgId,) + avgRgb + avgHsv
    except: # not entirely sure why this happens
        # seeing TypeError and OSError
        return ()

with open(moviesMetadataPath, 'r') as moviesMetadata, open(imgStatsPath, 'w') as imgStats:
    reader = csv.DictReader(moviesMetadata)
    next(reader)
    writer = csv.writer(imgStats)
    writer.writerow(['img_id', 'avg_r', 'avg_g', 'avg_b', 'avg_h', 'avg_s', 'avg_v'])

    posterPaths = [row['poster_path'][1:] for row in reader if row['poster_path']]
    with mp.Pool() as p:
        posterStats = tqdm(p.imap_unordered(calcStats, posterPaths, 10), total=len(posterPaths))
        posterStats = [stats for stats in posterStats if posterStats]
        writer.writerows(posterStats)

