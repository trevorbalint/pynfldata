"""File to get raw data from nfl.com and process drives out of it.
Will likely be useful but need to be refactored later when I need to get more data than just drive outcomes

3-Oct-2019: Fixed issue with drive start position. Added "bad" games that are unprocessable. Removed arrow.
    Added Game helper functions.
4-Sep-2019: Mostly functional. A few small bugs that are noted, but most data is in and correct.
"""
# todo better documentation
import xmltodict
import logging
from pynfldata.nfl_data_parser import functions as f
import pandas as pd
from pynfldata.nfl_data_parser.nfl_types import Game

# setup logging
logger = logging.getLogger('drive_parser.py')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


# function to get all games from a schedule file and build Game objects
def get_games(game_year: int):
    schedule_url = "http://www.nfl.com/feeds-rs/schedules/{}".format(game_year)
    xml_string = f.download_xml(schedule_url)

    game_dict = xmltodict.parse(xml_string)['gameSchedulesFeed']['gameSchedules']['gameSchedule']

    games_list = [Game(int(x['@season']),
                       x['@seasonType'],
                       int(x['@week']),
                       x['@homeTeamAbbr'],
                       x['@visitorTeamAbbr'],
                       x['@gameId']) for x in game_dict]

    return games_list


bad_games = ['2016080751',  # preseason game that wasn't actually played
             '2011120406'  # NO/DET game with a super-broken drive  # todo fix this drive/game
             ]
# get all schedule files 2009+, process games in each year separately
for year in range(2009, 2019):
    games = get_games(year)

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
