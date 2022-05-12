import json
from types import SimpleNamespace
import pyrasterframes
from pyrasterframes.utils import create_rf_spark_session
from pyspark.sql.functions import col


# Load configuration
with open('../config/config.json', 'r') as f:
    cfg = json.load(f, object_hook=lambda d: SimpleNamespace(**d))

# Setup spark
spark = create_rf_spark_session()

# TODO: Get most recent timestamp and download key by sorting files in following list
client.list('{}/{}/{}/{}/{}/occurrence.parquet'.format(
    cfg.hdfs.paths.landing.persistent, 
    cfg.gbif.source,
    cfg.gbif.version
)

timestamp = "2022-05-11_13-12-51"
download_key = "0273021-210914110416597"

occurrences_hdfs = '{}/{}/{}/{}/{}/{}/occurrence.parquet'.format(
    cfg.hdfs.dfs, 
    cfg.hdfs.paths.landing.persistent, 
    cfg.gbif.source,
    cfg.gbif.version,
    timestamp,
    download_key
)

# Get occurrences from persistent landing zone
occurrences = spark.read.parquet(occurrences_hdfs)

# Extract the 10 most iconic endangered species in the Yucatán Peninsula
# Source: https://www.biologicaldiversity.org/programs/international/mexico/pdfs/English-Top-10-Endangered-Mexico.pdf
occurrences = occurrences.select("species", "decimallongitude", "decimallatitude", "elevation", "day", "month", "year")
occurrences = occurrences.filter(col("species").isin(cfg.swd.species_of_interest))

# TODO: Join occurrences with most appropriate raster for each feature at each point in time and space. Samples With Data (SWD) format
# TODO: Read most appropriate raster data

# TODO: Given a grid of geolocation points, sample a specific set of rasters with the most current data (background)

# Get most recent rasters

# Most recent collection timestamp
client.list('{}/{}/{}/{}/{}/occurrence.parquet'.format(
    cfg.hdfs.paths.landing.persistent, 
    cfg.gbif.source,
    cfg.gbif.version
)

# Most recent files with required variables at specified resolution
worldclim_files = client.list('{}/{}/{}'.format(
    cfg.hdfs.paths.landing.persistent, 
    cfg.gbif.source,
    cfg.gbif.version
)

neo_files = []

raster_files = worldclim_files + neo_files

# Create a 100 x 100 coordinate grid for environmental data
for lat in np.linspace(cfg.swd.extent.min_lat,cfg.swd.extent.max_lat, 100):
    for lon in np.linspace(cfg.swd.extent.min_lon,cfg.swd.extent.max_lon, 100):
        for rasterfile in raster_files:
            rf = spark.read.raster(rasterfile)
        
# TODO: For each coordinate, sample all rasters? Or is it faster to do it per raster?