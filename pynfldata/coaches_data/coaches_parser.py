from pynfldata import nfl_data_parser as f
import xmltodict
import pandas as pd
import logging
import os


# setup logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

TEAMS_FILENAME = 'teams.csv'
COACHES_FILENAME = 'coaches.csv'


def get_teams_json(team_year: int):
    logger.debug('Getting team data for {}'.format(str(team_year)))
    url = "http://www.nfl.com/feeds-rs/teams/{year}.xml".format(year=team_year)
    xml_string = f.download_xml(url, 1)
    data_dict = xmltodict.parse(xml_string)

    return data_dict['teamsFeed']['teams']['team']


def get_coaches_json(team_year: int, team_id: int):
    logger.debug('Getting coach info for {team:04}, {year:04}'.format(team=team_id, year=team_year))
    url = "http://www.nfl.com/feeds-rs/coach/byTeam/{team:04}/{year:04}".format(team=team_id, year=team_year)
    xml_string = f.download_xml(url, 1)
    data_dict = xmltodict.parse(xml_string)

    return [data_dict['coach']]


def save_teams_df():
    teams_list = []
    for year in range(1969, 2019):
        teams_list += get_teams_json(year)

    df = pd.DataFrame().from_dict(teams_list)
    bad_team_abbrs = ['AFE', 'AFW', 'NFE', 'NFW']  # all-pro teams should be excluded
    df = df[df['@teamType'] != 'PRO']
    df = df[~df['@abbr'].isin(bad_team_abbrs)][['@season', '@teamId', '@abbr']]
    df = df.rename(columns={'@season': 'season', '@teamId': 'teamId', '@abbr': 'abbr'})

    df = df.set_index(['season', 'teamId'])

    df.to_csv(TEAMS_FILENAME)


def get_teams_data():
    if not os.path.isfile(TEAMS_FILENAME):
        save_teams_df()

    return pd.read_csv(TEAMS_FILENAME)


def save_coaches_df():
    teams_df = get_teams_data().set_index(['season', 'teamId'])

    team_list = [x[0] for x in teams_df.iterrows()]

    for team_tuple in team_list:
        raw_coach_data = get_coaches_json(team_tuple[0], team_tuple[1])
        logger.debug('Coaches found: {}'.format(str(len(raw_coach_data))))

        coach_data = raw_coach_data[0]
        teams_df.loc[team_tuple, 'coachName'] = coach_data['@displayName']
        teams_df.loc[team_tuple, 'coachNFLID'] = coach_data['@nflId']

    teams_df.to_csv(COACHES_FILENAME)


def get_coaches_data():
    if not os.path.isfile(COACHES_FILENAME):
        save_coaches_df()

    return pd.read_csv(COACHES_FILENAME)
