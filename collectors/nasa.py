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
    cfg.nasa.source,
    cfg.nasa.version
)
client.makedirs(outdir)

# Download files
download_link = (
    'https://neo.gsfc.nasa.gov/archive/geotiff.float/'
    'SRTM_RAMP2_TOPO/SRTM_RAMP2_TOPO_2000.FLOAT.TIFF'
)

outputfile = outdir + '/SRTM_RAMP2_TOPO_2000.FLOAT.TIFF'
print('Downloading {} to {}'.format(download_link, outputfile))

# NOTE: streaming is used to save IO
download = requests.get(download_link, stream=True)
client.write(
    outputfile,
    data=download.iter_content(chunk_size=cfg.hdfs.chunk_size),
    overwrite=True
    )
