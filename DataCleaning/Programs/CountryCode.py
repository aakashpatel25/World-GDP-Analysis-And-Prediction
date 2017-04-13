from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructField,StructType,DateType,DoubleType,IntegerType,StringType
from pyspark.sql import Row
from pyspark.sql.functions import levenshtein
import unicodedata
import pycountry

conf = SparkConf().setAppName('Country Code Entity Resolution')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

countries=  []

schema = StructType([
    StructField("country", StringType()),
    StructField("year", IntegerType()),
    StructField("gdp", DoubleType()),
    StructField("gdpNote", IntegerType()),
    StructField("debt", DoubleType()),
    StructField("inflation", DoubleType()),
    StructField("unemployment", DoubleType()),
    StructField("lendingInterest", DoubleType()),
    StructField("techExport", DoubleType()),
    StructField("totalReserves", DoubleType()),
    StructField("fdi", DoubleType()),
    StructField("cropProduction", DoubleType()),
    StructField("oilPrice", DoubleType()),
    StructField("gasPrice", DoubleType()),
    StructField("workingPop", DoubleType()),
])

gdp = spark.read.csv("final.csv",header=True,mode="DROPMALFORMED",schema=schema)

gdpCountry = gdp.select("country").distinct()

for country in pycountry.countries:
    countries.append([unicodedata.normalize('NFKD', country.name).encode('ascii','ignore'),\
                      unicodedata.normalize('NFKD', country.alpha_3).encode('ascii','ignore')])

countries = sc.parallelize(countries).toDF(['name','code'])

joinedDF = gdpCountry.join(countries)

joinedDF = joinedDF.select("country","name","code",levenshtein("country","name").alias("lev"))

cleanedData = joinedDF.filter(joinedDF.lev==0)

arr = cleanedData.select("name").rdd.map(lambda data:data["name"]).collect()
con = cleanedData.select("country").rdd.map(lambda data:data["country"]).collect()

countries = countries.filter(countries.name.isin(*arr)==False)
gdpCountry = gdpCountry.filter(gdpCountry.country.isin(*con)==False)

cleanedData.coalesce(1).write.csv("countryClean.csv", header=True)
countries.coalesce(1).write.csv("ccode.csv", header=True)
gdpCountry.coalesce(1).write.csv("leftover.csv",header=True)