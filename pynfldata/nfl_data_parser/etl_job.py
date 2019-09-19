from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('Kaggle ETL').setMaster('local[4]')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.master("local").appName("Kaggle ETL").getOrCreate()

nfl_csv = "C:\\Users\\TrevHP\\Downloads\\nflplaybyplay2009to2016\\NFL Play by Play 2009-2018 (v5).csv"

df = spark.read.csv(nfl_csv, header=True, inferSchema=True)

bad_game = df.filter(df.game_id == '2013092903')\
             .filter(df.drive >= '22').filter(df.drive <= '23')

for line in bad_game.toLocalIterator():
    print(line)
