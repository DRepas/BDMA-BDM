#!/usr/bin/env python 

import json
import requests
from types import SimpleNamespace
from hdfs import InsecureClient
from datetime import date, datetime
from dateutil.rrule import rrule, DAILY, MONTHLY


# Load configuration
with open('../config/config.json', 'r') as f:
    cfg = json.load(f, object_hook=lambda d: SimpleNamespace(**d))

# Setup client
client = InsecureClient(cfg.hdfs.url, user=cfg.hdfs.user)

# Iterate datasets
for dataset in cfg.neo.datasets:
    
    # Create output directory in HDFS
    outdir = '{}/{}/{}'.format(
        cfg.hdfs.paths.landing.temporary, 
        cfg.neo.source,
        dataset.name
    )
    client.makedirs(outdir)

    # Iterate through dates
    start = datetime.strptime(dataset.dates.start, dataset.dates.format)
    end = datetime.strptime(dataset.dates.end, dataset.dates.format)
    
    freq = DAILY
    if hasattr(dataset.dates, 'freq') and dataset.dates.freq == "MONTHLY":
        freq = MONTHLY

    dates = rrule(freq, dtstart=start, until=end)
    if hasattr(dataset.dates, 'increment'):
        dates = dates[::dataset.dates.increment]

    for date_obj in dates:
        date = date_obj.strftime(dataset.dates.format)
        filename = '{}_{}.FLOAT.TIFF'.format(dataset.name, date)
        outfile = '{}/{}'.format(outdir, filename)
        download_link = (
            'https://neo.gsfc.nasa.gov/archive/geotiff.float/'
            '{}/{}'.format(dataset.name, filename)
        )
        print('Downloading {} to {}'.format(download_link, outfile))

        # NOTE: streaming is used to save IO
        download = requests.get(download_link, stream=True)
        client.write(
            outfile,
            data=download.iter_content(chunk_size=cfg.hdfs.chunk_size),
            overwrite=True
        )