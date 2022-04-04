#!/usr/bin/env python 

import json
from types import SimpleNamespace
from hdfs import InsecureClient
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile
import pyrasterframes
from pyrasterframes.utils import create_rf_spark_session


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

# Setup RasterFrames
spark = create_rf_spark_session()

# Setup HDFS client
client = InsecureClient(cfg.hdfs.url, user=cfg.hdfs.user)

# Iterate datasets
for dataset in cfg.neo.datasets[:1]:

    # Find all downloads in temporal landing zone
    tmpdir = '{}/{}/{}'.format(
        cfg.hdfs.paths.landing.temporary, 
        cfg.neo.source,
        dataset.name
    )
    try:
        files = client.list(tmpdir)
    except:
        continue
        
    # Create timestamped local and persistent (HDFS) directories
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    outdir = '{}/{}/{}/{}'.format(
        cfg.hdfs.paths.landing.persistent, 
        cfg.neo.source,
        dataset.name,
        timestamp
    )
    localdir = '/tmp/{}/{}/{}'.format(
        cfg.neo.source,
        dataset.name,
        timestamp
    )
    client.makedirs(outdir)
    Path(localdir).mkdir(parents=True, exist_ok=True)
    
    # Parse files
    for fname in files:
        
        # Extract date from filename
        date = fname.split('.')[0].split('_')[-1]
        print("Handling {} ({})".format(dataset.name, date))

        # NOTE: RasterFrames should be able to read/write directly from HDFS
        #       without requiring downloads. I/O could be saved that way.

        # Download from HDFS to local file system
        print(
            "Downloading", 
            "hdfs://{}/{}".format(tmpdir, fname), 
            "to", 
            "{}/{}".format(localdir, fname)
        )
        client.download(
            "{}/{}".format(tmpdir, fname), 
            "{}/{}".format(localdir, fname), 
            overwrite=True
        )

        # Read files into RasterFrames
        print("Reading file {}/{}".format(localdir, fname))
        rf = spark.read.raster("{}/{}".format(localdir, fname))

        # Convert to parquet
        localfile="{}/{}_{}.pq".format(localdir, dataset.name, date)
        hdfs_path="{}/{}.pq".format(outdir, date)
        print("Writing file", localfile)
        rf.write.parquet(localfile)

        # Upload contents to HDFS
        print(
            "Uploading",
            localfile,
            "to", 
            "hdfs://{}".format(hdfs_path)
        )
        client.upload(hdfs_path, localfile)

    # Remove artifacts (local and hdfs)
    print("Cleaning up temporary files")
    rmdir(localdir)
    client.delete(tmpdir)
