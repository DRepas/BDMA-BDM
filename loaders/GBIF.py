#!/usr/bin/env python 

import json
from types import SimpleNamespace
from hdfs import InsecureClient
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile


def rmdir(directory):
    path = Path(directory)
    for item in path.iterdir():
        if item.is_dir():
            rmdir(item)
        else:
            item.unlink()
    path.rmdir()


# Load configuration
with open('../config/config.json', 'r') as f:
    cfg = json.load(f, object_hook=lambda d: SimpleNamespace(**d))

# Setup client
client = InsecureClient(cfg.hdfs.url, user=cfg.hdfs.user)

# Find all downloads in temporal landing zone
tmpdir = '{}/{}/{}'.format(
    cfg.hdfs.paths.landing.temporary, 
    cfg.gbif.source,
    cfg.gbif.version
)
zipfiles=client.list(tmpdir)

for zipfile in zipfiles:
    
    # Extract download key from filename
    download_key='.'.join(zipfile.split('.')[:-1])
    print("Handling", download_key)

    # Create timestamped local and persistent (HDFS) directories
    timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    outdir = '{}/{}/{}/{}'.format(
        cfg.hdfs.paths.landing.persistent, 
        cfg.gbif.source,
        cfg.gbif.version,
        timestamp
    )
    localdir = '/tmp/{}/{}/{}'.format(
        cfg.gbif.source,
        cfg.gbif.version,
        timestamp
    )
    client.makedirs(outdir)
    Path(localdir).mkdir(parents=True, exist_ok=True)

    # Download from HDFS to local file system
    print(
        "Downloading", 
        "hdfs://{}/{}".format(tmpdir, zipfile), 
        "to", 
        "{}/{}".format(localdir, zipfile)
    )
    client.download(
        "{}/{}".format(tmpdir, zipfile), 
        "{}/{}".format(localdir, zipfile), 
        overwrite=True
    )

    # Perform local operations on archive
    with ZipFile("{}/{}".format(localdir, zipfile)) as archive:

        # Check archive integrity
        print("Checking archive integrity")
        badfile = archive.testzip()
        if badfile is not None:
            print("Bad file:", badfile)
            continue
        
        # Extract archive locally
        extractdir = "{}/{}".format(localdir, download_key)
        print("Extracting archive to", extractdir)
        archive.extractall(path=extractdir)

    # Upload contents to HDFS
    print(
        "Uploading",
        extractdir,
        "to", 
        "hdfs://{}".format(outdir)
    )
    client.upload(outdir, extractdir)

    # Remove redundant zip files (local and hdfs)
    print("Cleaning up temporary files")
    rmdir(localdir)
    # client.delete("{}/{}".format(tmpdir, zipfile))
