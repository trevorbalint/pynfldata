"""General functions usable by any script

"""
import urllib3
import time
import xmltodict
from pynfldata.data_tools.nfl_types import Game
import json
from pathlib import Path
import os


bad_games = ['2016080751',  # preseason game that wasn't actually played
             '2011120406'  # NO/DET game with a super-broken drive  # todo fix this drive/game
             ]


# function to get the xml and ensure that status 200 is returned
def download_xml(path: str, timeout_secs: int = 2):
    http = urllib3.PoolManager()
    r = http.request('GET', path)
    assert r.status == 200
    time.sleep(timeout_secs)
    return r.data


# function to get data for other scripts
# Gets the requested data from local if possible, downloads as xml and saves it as json if not
def get_data(path: str, timeout_secs: int = 2, xml_args: dict = dict):
    # gets the type of data requested, coaches, teams, boxscorepbp, etc, and the rest of the path
    short_path = path.split('feeds-rs/')[1].split('/')

    # split short_path into folder (nested under /data) and file_path (everything but .xml because we'll be saving json)
    folder = 'data/{}'.format(short_path[0])
    file_path = '{}.json'.format(short_path[1].split('.')[0])

    # if the folder doesn't exist, create it
    if not os.path.exists(folder):
        os.makedirs(folder)

    # build a Path object and check if it's a file. if it is, use it. If not, download and convert the xml.
    filename = Path('{}//{}'.format(folder, file_path))
    if filename.is_file():
        with open(filename, 'r') as infile:
            json_data = json.load(infile)
    else:
        xml_string = download_xml(path, timeout_secs)
        json_data = xmltodict.parse(xml_string, **xml_args)
        with open(filename, 'w') as outfile:
            json.dump(json_data, outfile)
    return json_data


# function to get all games from a schedule file and build Game objects
def get_games_from_schedule(game_year: int):
    schedule_url = "http://www.nfl.com/feeds-rs/schedules/{}".format(game_year)
    xml_string = download_xml(schedule_url)

    game_dict = xmltodict.parse(xml_string)['gameSchedulesFeed']['gameSchedules']['gameSchedule']

    games_list = [Game(int(x['@season']),
                       x['@seasonType'],
                       int(x['@week']),
                       x['@homeTeamAbbr'],
                       x['@visitorTeamAbbr'],
                       x['@gameId']) for x in game_dict]

    return games_list


def get_games_for_years(start_year: int, end_year: int):
    games_list = []
    for year in range(start_year, end_year):
        games = get_games_from_schedule(year)

        # using list of games, get game details and append full Game.export() dict to new list
        for g in games:
            if g.season_type != 'PRO' and g.game_id not in bad_games:  # exclude pro bowl and bad games
                g.get_game_details()
                games_list.append(g)

    return games_list
