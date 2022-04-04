#!/usr/bin/env python 

import json
import requests
from types import SimpleNamespace
from hdfs import InsecureClient


# Load configuration
with open('../config/config.json', 'r') as f:
    cfg = json.load(f, object_hook=lambda d: SimpleNamespace(**d))

# Setup client
client = InsecureClient(cfg.hdfs.url, user=cfg.hdfs.user)

# Create output directory in HDFS
outdir = '{}/{}/{}'.format(
    cfg.hdfs.paths.landing.temporary, 
    cfg.worldclim.source, 
    cfg.worldclim.version
)
client.makedirs(outdir)

# Download files
for resolution in cfg.worldclim.resolutions:
    for variable in cfg.worldclim.variables:
        download_link = (
            'https://biogeo.ucdavis.edu/data/worldclim/'
            'v{ver}/base/wc{ver}_{res}_{var}.zip'
            .format(
                ver=cfg.worldclim.version,
                res=resolution,
                var=variable
            )
        )
        zipfile = '{}/{}_{}.zip'.format(outdir, resolution, variable)
        print('Downloading {} to {}'.format(download_link, zipfile))

        # NOTE: streaming is used to save IO
        download = requests.get(download_link, stream=True)
        client.write(
            zipfile,
            data=download.iter_content(chunk_size=cfg.hdfs.chunk_size),
            overwrite=True
        )
