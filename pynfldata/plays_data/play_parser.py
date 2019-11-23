"""File to get raw data from nfl.com and process drives out of it.
Will likely be useful but need to be refactored later when I need to get more data than just drive outcomes

3-Oct-2019: Fixed issue with drive start position. Added "bad" games that are unprocessable. Removed arrow.
    Added Game helper functions. Refactored dataclasses out of this module.
4-Sep-2019: Mostly functional. A few small bugs that are noted, but most data is in and correct.
"""
# todo better documentation
import logging
from pynfldata.data_tools import functions as f
import pandas as pd
import os

# setup logging
logger = logging.getLogger('plays_parser.py')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def build_and_save_json():
    # get all schedule files 2009+, process games in each year separately
    for year in range(2009, 2019):
        games = f.get_games_for_years(year, year+1)

        # using list of games, export game details and append to that year's list
        games_dicts = []
        for g in games:
            games_dicts.append(g.export())
        drives_df = pd.DataFrame(games_dicts)

        # if the folder doesn't exist, create it
        if not os.path.exists('output'):
            os.makedirs('output')

        drives_df.to_json('output/plays_{year}.json'.format(year=str(year)), orient='records', lines=True)
        logger.info('Completed processing JSON for {}'.format(str(year)))


build_and_save_json()
