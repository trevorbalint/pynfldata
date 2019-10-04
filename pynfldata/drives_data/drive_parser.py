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

# setup logging
logger = logging.getLogger('drive_parser.py')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


bad_games = ['2016080751',  # preseason game that wasn't actually played
             '2011120406'  # NO/DET game with a super-broken drive  # todo fix this drive/game
             ]
# get all schedule files 2009+, process games in each year separately
for year in range(2009, 2019):
    games = f.get_games(year)

    # using list of games, get game details and append full Game.export() dict to new list
    games_dicts = []
    for g in games:
        if g.season_type != 'PRO' and g.game_id not in bad_games:  # exclude pro bowl and bad games
            g.get_game_details()
            logger.info(g)
            games_dicts.append(g.export())
    drives_df = pd.DataFrame(games_dicts)
    drives_df.to_json('drives_{year}.json'.format(year=str(year)), orient='records', lines=True)
    print(drives_df.head())
