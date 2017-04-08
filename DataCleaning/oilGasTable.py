from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructField,StructType,DateType,DoubleType,IntegerType,StringType
from pyspark.sql.functions import year,round

conf = SparkConf().setAppName('Oil and Gas Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

gdpschema = StructType([
    StructField("country", StringType()),
    StructField("year", IntegerType()),
    StructField("gdp", DoubleType()),
    StructField("gdpNote", IntegerType()),
])

schema = StructType([
    StructField("date", DateType()),
    StructField("Price", DoubleType()),
])

gdp = spark.read.csv("gdpCleaned.csv",header=True,mode="DROPMALFORMED",schema=gdpschema)
oil = spark.read.csv("oil.csv", header=True, mode="DROPMALFORMED",schema=schema)
gas = spark.read.csv("natgas.csv",header=True,mode="DROPMALFORMED", schema=schema)

oil = oil.withColumnRenamed("Price","oilPrice")
gas = gas.withColumnRenamed("Price","gasPrice")

joinedData = oil.join(gas,oil.date==gas.date,'outer').drop(gas.date)

joinedData = joinedData.select(year(joinedData.date).alias("year"),joinedData.oilPrice,joinedData.gasPrice)

joinedData = (joinedData.groupBy("year").avg("oilPrice","gasPrice")
                        .withColumnRenamed("avg(oilPrice)","oilPrice")
                        .withColumnRenamed("avg(gasPrice)","gasPrice"))

oilngasData = (joinedData.select(
                                "year",
                                round("oilPrice",3).alias("oilPrice"),
                                round("gasPrice",3).alias("gasPrice")).orderBy("year"))

gdp = gdp.join(oilngasData,gdp.year==oilngasData.year,'left').drop(oilngasData.year).orderBy("country","year")
gdp = gdp.coalesce(1)

gdp.write.csv("gdpCleanedD.csv", header=True)