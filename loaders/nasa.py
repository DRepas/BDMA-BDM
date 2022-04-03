#!/usr/bin/env python

import json
from types import SimpleNamespace
from hdfs import InsecureClient
from datetime import datetime
from pathlib import Path
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
    cfg.nasa.source,
    cfg.nasa.version
)
files = client.list(tmpdir)

for file in files:

    # Create timestamped local and persistent (HDFS) directories
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    outdir = '{}/{}/{}/{}'.format(
        cfg.hdfs.paths.landing.persistent,
        cfg.nasa.source,
        cfg.nasa.version,
        timestamp
    )
    localdir = '/tmp/{}/{}/{}'.format(
        cfg.nasa.source,
        cfg.nasa.version,
        timestamp
    )
    client.makedirs(outdir)
    Path(localdir).mkdir(parents=True, exist_ok=True)

    # Download from HDFS to local file system
    print(
        "Downloading",
        "hdfs://{}/{}".format(tmpdir, file),
        "to",
        "{}/{}".format(localdir, file)
    )
    client.download(
        "{}/{}".format(tmpdir, file),
        "{}/{}".format(localdir, file),
        overwrite=True
    )

    # Parse files
    for geotiff in localdir.iterdir():

       # TODO: I couldn't install the library to parse

    # Remove redundant zip files (local and hdfs)
    print("Cleaning up temporary files")
    rmdir(localdir)
    client.delete("{}/{}".format(tmpdir, file))
