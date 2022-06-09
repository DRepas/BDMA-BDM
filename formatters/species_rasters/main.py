import json

from types import SimpleNamespace

from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType

# Load configuration
with open('./config/config.json', 'r') as f:
    cfg = json.load(f, object_hook=lambda d: SimpleNamespace(**d))

# Setup client
client = InsecureClient(cfg.hdfs.url, user=cfg.hdfs.user)

# Create spark session and set up the database driver
spark = (SparkSession
         .builder
         .config("spark.jars", cfg.psql.jdbc.location)
         .master("local")
         .appName("species_rasters")
         .enableHiveSupport()
         .getOrCreate())

# Read files from HDFS
background = spark.read.csv("hdfs://pikachu.fib.upc.es:27000/user/bdm/swd/background.csv", header=True)
samples = spark.read.csv("hdfs://pikachu.fib.upc.es:27000/user/bdm/swd/samples.csv", header=True)

# Format datatypes
samples = samples.filter(samples.tmin != 'NaN')

samples = (samples
           .withColumn("longitude", samples["longitude"].cast(FloatType()))
           .withColumn("latitude", samples["latitude"].cast(FloatType()))
           .withColumn("tmin", samples["tmin"].cast(FloatType()))
           .withColumn("tmax", samples["tmax"].cast(FloatType()))
           .withColumn("tavg", samples["tavg"].cast(FloatType()))
           .withColumn("prec", samples["prec"].cast(FloatType()))
           .withColumn("srad", samples["srad"].cast(FloatType()))
           .withColumn("wind", samples["wind"].cast(FloatType()))
           .withColumn("vapr", samples["vapr"].cast(FloatType()))
           .withColumn("bio", samples["bio"].cast(FloatType()))
           .withColumn("elev", samples["elev"].cast(FloatType()))
           .withColumn("SRTM_RAMP2_TOPO", samples["SRTM_RAMP2_TOPO"].cast(FloatType()))
           .withColumn("MOD_LSTD_M", samples["MOD_LSTD_M"].cast(FloatType()))
           )

# Save the table in the database (dbtable)
(background
 .select(background.columns).write.format("jdbc")  
 .option("url", cfg.psql.jdbc.url) 
 .option("driver", "org.postgresql.Driver")  
 .option("dbtable", "background")  
 .option("user", cfg.psql.user)
 .option("password", cfg.psql.password)
 .save())

(samples
 .select(samples.columns).write.format("jdbc")  
 .option("url", cfg.psql.jdbc.url)  
 .option("driver", "org.postgresql.Driver")
 .option("dbtable", "samples") 
 .option("user", cfg.psql.user)
 .option("password", cfg.psql.password)
 .save())
