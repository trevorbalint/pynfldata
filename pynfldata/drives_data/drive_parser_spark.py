"""File to get raw data from nfl.com and process drives out of it.
Will likely be useful but need to be refactored later when I need to get more data than just drive outcomes
"""
# todo better documentation
import logging
from pynfldata.data_tools import functions as f
import pandas as pd
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext

conf = SparkConf().setAppName('Drive Parser').setMaster('local[4]')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.master("local").appName("Drive Parser").getOrCreate()
sqlContext = SQLContext(sc)

# setup logging
logger = logging.getLogger('drive_parser_spark.py')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


bad_games = ['2016080751',  # preseason game that wasn't actually played
             '2011120406'  # NO/DET game with a super-broken drive  # todo fix this drive/game
             ]


# Still slower than the non-spark version
def build_and_save_json():
    # get all schedule files 2009+, process games in each year separately
    for year in range(2009, 2019):
        games = f.get_games(year)

        games_par = sc.parallelize(games, 16)
        games_par = games_par.filter(lambda x: x.season_type != 'PRO' and x.game_id not in bad_games)
        games_dicts = games_par.map(lambda x: x.get_game_details()).map(lambda x: x.export())

        drives_df = pd.DataFrame(games_dicts.collect())

        # if the folder doesn't exist, create it
        if not os.path.exists('output'):
            os.makedirs('output')

        drives_df.to_json('output/drives_{year}.json'.format(year=str(year)), orient='records', lines=True)
        logger.info('Completed processing JSON for {}'.format(str(year)))


def convert_json_to_pq():
    json_df = sqlContext.read.json(['output/drives_{}.json'.format(str(x)) for x in range(2009, 2019)])
    print(json_df.head(5))
    json_df.write.parquet('output/drives.pq')


# build_and_save_json()
convert_json_to_pq()
