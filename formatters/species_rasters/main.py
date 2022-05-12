import json

from types import SimpleNamespace

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Load configuration
with open('../../config/config.json', 'r') as f:
    cfg = json.load(f, object_hook=lambda d: SimpleNamespace(**d)).formatters.species_rasters

# Cria a sessão e configura o driver do banco
spark = (SparkSession
         .builder
         .config("spark.jars", cfg.psql.jdbc.location)
         .master("local")
         .appName("species_rasters")
         .getOrCreate())

# tabela exemplo -- remover estes comentários depois
studentDf = spark.createDataFrame([
    Row(id=1, name='Luiz', marks=67),
    Row(id=2, name='Diogo', marks=88),
    Row(id=3, name='Nicole', marks=79),
    Row(id=4, name='Andres', marks=67),
])

# Salva o dataframe como tabela (dbtable)
(studentDf
 .select("id", "name", "marks").write.format("jdbc")  # selecione as colunas aqui
 .option("url", cfg.psql.jdbc.url) # isso não muda
 .option("driver", "org.postgresql.Driver")   # nem isso
 .option("dbtable", "students")   # aqui vc muda o nome da tabela (students)
 .option("user", cfg.psql.user)
 .option("password", cfg.psql.password)
 .save())
