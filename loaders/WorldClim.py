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

# Timestamp this loading operation
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Create timestamped local and persistent (HDFS) directories
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

# Find all downloads in temporal landing zone
tmpdir = '{}/{}/{}'.format(
    cfg.hdfs.paths.landing.temporary, 
    cfg.worldclim.source,
    cfg.worldclim.version
)
zipfiles=client.list(tmpdir)

for zipfile in zipfiles:
    
    # Extract resolution and variable from filename
    resolution=zipfile.split('.zip')[0].split('_')[0]
    variable=zipfile.split('.zip')[0].split('_')[1]
    print("Handling", resolution, variable)

    # Download from HDFS to local file system
    print(
        "Downloading", 
        "{}{}/{}".format(cfg.hdfs.dfs, tmpdir, zipfile), 
        "to", 
        "{}/{}".format(localdir, zipfile)
    )
    Path(localdir).mkdir(parents=True, exist_ok=True)
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
        
        # Upload contents to HDFS
        band=str(geotiff).split('_')[-1].split('.')[0]
        hdfs_path_tif="{}/{}/{}/{}.{}".format(outdir, resolution, variable, band, "tif")
        print("Uploading to", hdfs_path_tif)
        client.upload(hdfs_path_tif, str(geotiff))

        # Read files into RasterFrames
        rf = spark.read.raster(str(geotiff))

        # Convert to parquet
        hdfs_path_pq="{}{}/{}/{}/{}.{}".format(cfg.hdfs.dfs, outdir, resolution, variable, band, "pq")

        # Upload contents to HDFS
        print("Writing to", hdfs_path_pq)
        rf.write.parquet(hdfs_path_pq)

    # Remove redundant zip files (local and hdfs)
    # print("Cleaning up temporary files")
    # rmdir(localdir)
    # client.delete("{}/{}".format(tmpdir, zipfile))
