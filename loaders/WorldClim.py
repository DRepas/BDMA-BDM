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


# Setup RasterFrames
spark = create_rf_spark_session()

# Load configuration
with open('../config/config.json', 'r') as f:
    cfg = json.load(f, object_hook=lambda d: SimpleNamespace(**d))

# Setup client
client = InsecureClient(cfg.hdfs.url, user=cfg.hdfs.user)

# Find all downloads in temporal landing zone
tmpdir = '{}/{}/{}'.format(
    cfg.hdfs.paths.landing.temporary, 
    cfg.worldclim.source,
    cfg.worldclim.version
)
zipfiles=client.list(tmpdir)

for zipfile in zipfiles:
    
    # Extract resolution and variable from filename
    resolution=zipfile.split('.')[0].split('_')[0]
    variable=zipfile.split('.')[0].split('_')[1]
    print("Handling", resolution, variable)

    # Create timestamped local and persistent (HDFS) directories
    timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    outdir = '{}/{}/{}/{}'.format(
        cfg.hdfs.paths.landing.persistent, 
        cfg.worldclim.source,
        cfg.worldclim.version,
        timestamp
    )
    localdir = '/tmp/{}/{}/{}'.format(
        cfg.worldclim.source,
        cfg.worldclim.version,
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
        extractdir = Path(localdir, resolution, variable)
        print("Extracting archive to", extractdir)
        archive.extractall(path=extractdir)

    # Parse files
    for geotiff in extractdir.iterdir():

        if geotiff.suffix != '.tif':
            continue

        print("Parsing", geotiff)

        # Read files into RasterFrames
        rf = spark.read.raster(str(geotiff))

        # Convert to parquet
        band=str(geotiff).split('_')[-1].split('.')[0]
        localfile="{}/{}_{}_{}.pq".format(localdir, resolution, variable, band)
        hdfs_path="{}/{}/{}/{}.pq".format(outdir, resolution, variable, band)
        rf.write.parquet(localfile)

        # Upload contents to HDFS
        print(
            "Uploading",
            localfile,
            "to", 
            "hdfs://{}".format(hdfs_path)
        )
        client.upload(hdfs_path, localfile)

    # Remove redundant zip files (local and hdfs)
    print("Cleaning up temporary files")
    rmdir(localdir)
    client.delete("{}/{}".format(tmpdir, zipfile))
