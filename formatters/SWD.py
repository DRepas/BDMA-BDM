import datetime as dt
import numpy as np
import pandas as pd
import json
from types import SimpleNamespace
from hdfs import InsecureClient
import pyrasterframes
from pyrasterframes.rasterfunctions import *
from pyrasterframes.utils import create_rf_spark_session
from pyrasterframes import *
import pyrasterframes.rf_ipython
from IPython.display import display
import os.path
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.window import Window
from shapely.geometry import Point


def join_with_raster(df, rf, col_name):
    orig_columns = df.columns

    tmp_df = df.join(rf, st_intersects(
        st_reproject(rf_geometry(col('proj_raster')), rf_crs(col('proj_raster')).crsProj4, rf_mk_crs('EPSG:4326')),
        col('geometry')
    ))

    tmp_df = tmp_df.withColumn("extent", rf_extent(col("proj_raster")))
    tmp_df = tmp_df.withColumn("dims", rf_dimensions(col("proj_raster")))

    calc_col = round((col("extent").xmax - st_x(col('geometry')))/(col("extent").xmax - col("extent").xmin) * col("dims").cols).cast("Integer")
    calc_row = round((col("extent").ymax - st_y(col('geometry')))/(col("extent").ymax - col("extent").ymin) * col("dims").rows).cast("Integer")
    tmp_df = tmp_df.select("*", rf_explode_tiles(col('proj_raster').alias(col_name))).filter(calc_col == col("column_index")).filter(calc_row == col("row_index"))
    return tmp_df.select(*orig_columns, col_name)


# Load configuration
with open('/home/bdm/repo/config/config.json', 'r') as f:
    cfg = json.load(f, object_hook=lambda d: SimpleNamespace(**d))

# Setup spark with RasterFrames
spark = create_rf_spark_session()

# Setup HDFS client
client = InsecureClient(cfg.hdfs.url, user=cfg.hdfs.user)

print("Loading occurrences...")

# Get most recent timestamp and download key by sorting file paths
occurrences_hdfs = '{}/{}/{}/'.format(
    cfg.hdfs.paths.landing.persistent, 
    cfg.gbif.source,
    cfg.gbif.version
)
occurrences_hdfs += sorted(client.list(occurrences_hdfs))[-1] + '/'
occurrences_hdfs += sorted(client.list(occurrences_hdfs))[-1] + '/occurrence.parquet'

# Get occurrences from persistent landing zone
occurrences = spark.read.parquet(cfg.hdfs.dfs + occurrences_hdfs)

# Get relevant columns
occurrences = occurrences.select("species", "decimallongitude", "decimallatitude", "day", "month", "year")

# Pre-process data for spacial join
occurrences = occurrences.withColumn("geometry", st_point(col("decimallongitude").cast("Double"), col("decimallatitude").cast("Double")))
occurrences = occurrences.withColumnRenamed("decimallongitude", "longitude").withColumnRenamed("decimallatitude", "latitude")

# Extract the 10 most iconic endangered species in the Yucatán Peninsula
# Source: https://www.biologicaldiversity.org/programs/international/mexico/pdfs/English-Top-10-Endangered-Mexico.pdf
occurrences = occurrences.filter(col("species").isin(cfg.swd.species_of_interest))

# Sort occurrences by date
occurrences = occurrences.withColumn("date", concat_ws("-", col("year"), col("month"), col("day")).cast("date"))
occurrences = occurrences.withColumn("date", when(col("date").isNull(), dt.date.today()).otherwise(col("date")))
# occurrences = occurrences.sort('date')


# Background: Given a grid of geolocation points, sample a specific set of rasters with the most current data
# Create a 100 x 100 coordinate grid for background data
background_rows = []
for lat in np.linspace(cfg.swd.extent.min_lat,cfg.swd.extent.max_lat, 100):
    for lon in np.linspace(cfg.swd.extent.min_lon,cfg.swd.extent.max_lon, 100):
        background_rows.append(Row(species='background', longitude=lon.item(), latitude=lat.item(), geometry=Point(lon.item(), lat.item()), date=dt.date.today()))

background = spark.createDataFrame(background_rows)

print("Loading raster catalogs...")

# Load current NEO datasets
neo_catalogs = {}

# Date parsers for each dataset
def date_parser(dataset, filename):    
    # Find correct format
    ds = {}
    for d in cfg.neo.datasets:
        if d.name == dataset:
            ds = d
            break
        
    return dt.datetime.strptime(filename.split(".pq")[0], ds.dates.format)


for dataset in cfg.neo.datasets:
    hdfs_path = '{}/{}/{}/'.format(
        cfg.hdfs.paths.landing.persistent, 
        cfg.neo.source,
        dataset.name
    )
    
    # Get most recent load timestamp
    hdfs_path += sorted(client.list(hdfs_path))[-1] + '/'
    
    # Generate catalog for dataset
    neo_catalogs[dataset.name] =  [{
        "path": cfg.hdfs.dfs + hdfs_path + scene,
        "date": date_parser(dataset.name, scene),
        "raster": spark.read.parquet(cfg.hdfs.dfs + hdfs_path + scene)
        } for scene in client.list(hdfs_path)]
    
    
# Load current WorldClim datasets
worldclim_catalogs = {}

# Date parsers for each variable type
def date_parser(variable, filename):
    today = dt.date.today()
    day = today.day
    month = today.month
    year = today.year
    
    if variable not in ("bio", "elev"):
        month = int(filename.split(".pq")[0])
        
    return dt.date(year, month, day)


# Most recent files with required variables at specified resolution (max)
resolution = cfg.worldclim.resolutions[-1]
variables = cfg.worldclim.variables
for variable in variables:
    hdfs_path = '{}/{}/{}/'.format(
        cfg.hdfs.paths.landing.persistent, 
        cfg.worldclim.source,
        cfg.worldclim.version
    )
    # Get most recent load timestamp
    hdfs_path += '{}/{}/{}/'.format(
        sorted(client.list(hdfs_path))[-1], 
        resolution,
        variable
    )        
    # Generate catalog for dataset
    worldclim_catalogs[variable] = [{
        "path": cfg.hdfs.dfs + hdfs_path + scene,
        "date": date_parser(variable, scene),
        "raster": spark.read.parquet(cfg.hdfs.dfs + hdfs_path + scene)
    } for scene in client.list(hdfs_path) if scene[-3:] == ".pq"]
    
print("Setup SWD format pipeline...")
    
# Prepare DFs (select only necessary columns)
background = background.select('date', 'geometry', 'species')
occurrences = occurrences.select('date', 'geometry', 'species')
df = occurrences.union(background)

for var_name, rasters in neo_catalogs.items():
    # TODO: Select most appropriate raster to join by date
    # Here we are getting the most recent raster
    rf = sorted(rasters, key=lambda d: d['date'])[-1]["raster"]
    df = join_with_raster(df, rf, var_name)

for var_name, rasters in worldclim_catalogs.items():
    # TODO: Select most appropriate raster to join by date
    # Here we are getting the most recent raster
    rf = sorted(rasters, key=lambda d: d['date'])[-1]["raster"]
    df = join_with_raster(df, rf, var_name)
    
print("Write to disk...")
    
client.makedirs(cfg.hdfs.paths.formatted.swd)

print("Writing background.csv")
background_df = (
    df.select('species', 
              st_x('geometry').alias('longitude'), 
              st_y('geometry').alias('latitude'), 
              *worldclim_catalogs, *neo_catalogs)
    .filter(col('species') == lit('background'))
)
background_df.write.mode('overwrite').option("header", True).csv(cfg.hdfs.dfs + cfg.hdfs.paths.formatted.swd + "/background.csv")
    
print("Writing samples.csv")
samples_df = (
    df.select('species', 
              st_x('geometry').alias('longitude'), 
              st_y('geometry').alias('latitude'), 
              *worldclim_catalogs, *neo_catalogs)
    .filter(col('species') != lit('background'))
)
samples_df.write.mode('overwrite').option("header", True).csv(cfg.hdfs.dfs + cfg.hdfs.paths.formatted.swd + "/samples.csv")

# Create spark session and set up the database driver
from pyspark.sql import SparkSession
spark = (SparkSession
         .builder
         .config("spark.jars", cfg.psql.jdbc.location)
         .master("local")
         .appName("species_rasters")
         .enableHiveSupport()
         .getOrCreate())

print("Saving to PostgreSQL: background")
(background_df.write.format("jdbc")
 .option("url", cfg.psql.jdbc.url)
 .option("driver", "org.postgresql.Driver")
 .option("dbtable", "background")
 .option("user", cfg.psql.user)
 .option("password", cfg.psql.password)
 .save())

print("Saving to PostgreSQL: samples")
(samples_df.write.format("jdbc")
 .option("url", cfg.psql.jdbc.url)
 .option("driver", "org.postgresql.Driver")
 .option("dbtable", "samples")
 .option("user", cfg.psql.user)
 .option("password", cfg.psql.password)
 .save())

print("Done!")
