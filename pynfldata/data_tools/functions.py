"""General functions usable by any script

"""
import urllib3
import time
import xmltodict
from pynfldata.data_tools.nfl_types import Game
import json
from pathlib import Path
import os
import datetime


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


# Takes a NFL date string "MM/DD/YYYY" and converts it to a datetime.date object
def get_game_date(date_string: str):
    date_list = list(map(lambda x: int(x), date_string.split('/')))
    game_date = datetime.date(date_list[2], date_list[0], date_list[1])
    return game_date


# get full game information from the scores feeds-rs object. downloads week by week
def get_game_score(season_year: int, season_type: str, week: int):
    score_url = "http://www.nfl.com/feeds-rs/scores/{y}/{t}/{w}".format(y=season_year, t=season_type, w=week)
    score_xml_string = download_xml(score_url, 1)

    score_game_dict_raw = xmltodict.parse(score_xml_string)['scoresFeed']['gameScores']

    # If there is only one element in the XML list, Python does wonky things. Force it to make a list in this case.
    # this occurs if there is only one game in a week, like preseason week 0 or super bowl week
    if 'gameSchedule' in score_game_dict_raw['gameScore']:
        return [score_game_dict_raw['gameScore']]
    else:
        return score_game_dict_raw['gameScore']


# function to get all games from a schedule file and build Game objects
def get_games_from_schedule(game_year: int):
    # get all games from the year's schedule file
    schedule_url = "http://www.nfl.com/feeds-rs/schedules/{}".format(game_year)
    schedule_xml_string = download_xml(schedule_url)
    schedule_game_dict = xmltodict.parse(schedule_xml_string)['gameSchedulesFeed']['gameSchedules']['gameSchedule']

    # build a tuple of year/type/week using every game in the schedule
    game_weeks = set()
    for game in schedule_game_dict:
        game_weeks.add((int(game['@season']), game['@seasonType'], int(game['@week'])))

    # get the scores for each week, add to a list, then remove scheduled games that don't have a score
    # this occurs if a game is scheduled but hasn't happened yet
    scores = []
    for week in game_weeks:
        scores += get_game_score(*week)
    scores = [x for x in scores if x.get('score')]  # Filter out games that don't have a score object - haven't happened
    scores = [x for x in scores if 'FINAL' in x['score']['@phase']]  # Filter out games that aren't final

    # build Game objects using scores list
    games_list = []
    for game in scores:
        score_dict = game.get('score')
        schedule_dict = game.get('gameSchedule')
        games_list.append(Game(int(schedule_dict['@season']),
                               schedule_dict['@seasonType'],
                               int(schedule_dict['@week']),
                               schedule_dict['@homeTeamAbbr'],
                               schedule_dict['@visitorTeamAbbr'],
                               schedule_dict['@gameId'],
                               score_dict['homeTeamScore']['@pointTotal'],
                               score_dict['visitorTeamScore']['@pointTotal']))

    return games_list


# given a year range, get Game objects and return in a list
def get_games_for_years(start_year: int, end_year: int):
    games_list = []
    for year in range(start_year, end_year):
        games = get_games_from_schedule(year)

        # using list of games, get game details and append full Game.export() dict to new list
        for g in games:
            if g.season_type != 'PRO' and g.game_id not in bad_games:  # exclude pro bowl and bad games
                try:
                    g.get_game_details()
                    games_list.append(g)
                except KeyError:
                    print('ERROR WITH GAME {}'.format(g.game_id))

    return games_list
