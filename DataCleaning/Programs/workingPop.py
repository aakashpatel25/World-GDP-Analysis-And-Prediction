from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructField,StructType,DateType,DoubleType,IntegerType,StringType
from pyspark.sql.functions import round

conf = SparkConf().setAppName('Working Population Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

gdpschema = StructType([
    StructField("country", StringType()),
    StructField("year", IntegerType()),
    StructField("gdp", DoubleType()),
    StructField("gdpNote", IntegerType()),
    StructField("oilPrice", DoubleType()),
    StructField("gasPrice", DoubleType())
])

schema = StructType([
    StructField("country", StringType()),
    StructField("year", IntegerType()),
    StructField("workingPop", DoubleType())
])

gdp = spark.read.csv("gdpCleaned.csv",header=True,mode="DROPMALFORMED",schema=gdpschema)
work = spark.read.csv("workPop.csv", header=True, mode="DROPMALFORMED",schema=schema)
gdp = gdp.join(work,(gdp.year==work.year)&(gdp.country==work.country),"left").drop(work.year).drop(work.country)

gdp = gdp.select("country","year","gdp","gdpNote","oilPrice","gasPrice",round("workingPop",4).alias("workingPop")).orderBy("country","year")

gdp = gdp.coalesce(1)

gdp.write.csv("gdpCleanedD.csv", header=True)