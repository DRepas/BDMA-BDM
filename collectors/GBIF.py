#!/usr/bin/env python 

import json
import requests
from types import SimpleNamespace
from hdfs import InsecureClient
import time
from collections.abc import Iterable


# Deep conversion of SimpleNamespace to dictionary
def namespace_to_dict(ns):
    if isinstance(ns, SimpleNamespace):
        return namespace_to_dict(ns.__dict__)
    elif isinstance(ns, dict):
        return {k: namespace_to_dict(v) for k, v in ns.items()}
    elif isinstance(ns, list):
        return [namespace_to_dict(v) for v in ns]
    return ns


# Load configuration
with open('../config/config.json', 'r') as f:
    cfg = json.load(f, object_hook=lambda d: SimpleNamespace(**d))
    query = namespace_to_dict(cfg.gbif.query)

# Setup client
client = InsecureClient(cfg.hdfs.url, user=cfg.hdfs.user)

# Create output directory in HDFS
outdir = '{}/{}/{}'.format(
    cfg.hdfs.paths.landing.temporary, 
    cfg.gbif.source,
    cfg.gbif.version
)
client.makedirs(outdir)

# Perform download request
print("Requesting download")
download_key = requests.post(
    'https://api.gbif.org/v{}/occurrence/download/request'
    .format(cfg.gbif.version),
    json=query,
    auth=(cfg.gbif.username, cfg.gbif.password),
    headers={"Content-Type": "application/json"}
).text
print("Got key:", download_key)

# Actively wait for download to be done (ping every 10s)
download_status_link = 'https://api.gbif.org/v{}/occurrence/download/{}'.format(
    cfg.gbif.version, 
    download_key
)
while True:
    resp = requests.get(download_status_link).json()

    print("Download request {}: {}".format(
        download_key, 
        resp['status']
    ), end='\r')

    if resp['status'] == "SUCCEEDED":
        break

    time.sleep(10)

# Extract download link and storage destination
download_link = resp['downloadLink']
zipfile = '{}/{}.zip'.format(outdir, download_key)
print('Downloading {} to {}'.format(download_link, zipfile))

# Download to HDFS (streaming is used to save IO)
download = requests.get(download_link, stream=True)
client.write(
    zipfile,
    data=download.iter_content(chunk_size=cfg.hdfs.chunk_size),
    overwrite=True
)
print('Done!')
