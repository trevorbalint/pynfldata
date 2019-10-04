import urllib3
import time
import xmltodict
from pynfldata.data_tools.nfl_types import Game


# helper function to get the xml and ensure that status 200 is returned
def download_xml(path: str, timeout_secs: int = 2):
    http = urllib3.PoolManager()
    r = http.request('GET', path)
    assert r.status == 200
    time.sleep(timeout_secs)
    return r.data


# function to get all games from a schedule file and build Game objects
def get_games(game_year: int):
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

