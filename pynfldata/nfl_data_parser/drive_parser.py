"""File to get raw data from nfl.com and process drives out of it.
Will likely be useful but need to be refactored later when I need to get more data than just drive outcomes

3-Oct-2019: Fixed issue with drive start position. Added "bad" games that are unprocessable. Removed arrow.
    Added Game helper functions.
4-Sep-2019: Mostly functional. A few small bugs that are noted, but most data is in and correct.
"""
# todo better documentation
import xmltodict
import dataclasses as dc
from dataclasses import dataclass
import logging
from pynfldata.nfl_data_parser import functions as f
import pandas as pd

# setup logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


# class to contain yardline data. It comes to us as "TEAM yardline" like SEA 25, so include int (-50:50)
@dataclass
class Yardline:
    side: str
    side_pos: int
    yard_int: int


__scoring_dict__ = {'TD': 6, 'FG': 3, 'PAT': 1, 'PAT2': 2, 'SFTY': 2}

# class to contain Play data, uses above dict to calculate points
@dataclass
class Play:
    play_id: int
    pos_team: str
    description: str
    yardline: Yardline
    play_type: str
    scoring_type: str = None
    scoring_team_abbr: str = None
    points: int = 0

    def calculate_points(self):
        if self.scoring_type is not None:
            self.points = __scoring_dict__.get(self.scoring_type)


# class to contain Drive data. Contains all plays in drive, and some post_init calculated fields
@dataclass
class Drive:
    drive_id: int
    start_time: str
    end_time: str
    plays: list
    pos_team: str
    drive_start: Yardline = dc.field(default=None, init=False)
    scoring_team: str = dc.field(default=None, init=False)
    points: int = dc.field(default=None, init=False)

    def __post_init__(self):
        [x.calculate_points() for x in self.plays]
        self.calculate_scoring()
        self.drive_start = self.calculate_drive_start(self.plays)

    # Function to calculate the starting yardline of a drive given the drive's plays
    # Complicated as some drives' first play has no yardline, hence the recursion
    def calculate_drive_start(self, plays_list):
        if len(plays_list) == 1:  # occurs when a game ends concurrent with a turnover/change of posession/kickoff
            return Yardline(None, None, None)
        elif plays_list[0].play_type == 'KICK_OFF':
            return self.calculate_drive_start(plays_list[1:])
        elif plays_list[0].yardline is not None:
            return plays_list[0].yardline
        else:
            logger.debug("Drive's first play has no yardline: {}".format(plays_list[0]))
            return self.calculate_drive_start(plays_list[1:])

    def calculate_scoring(self):
        if any([x.points for x in self.plays]):
            self.points = sum([x.points for x in self.plays])
            scoring_team_list = [x.scoring_team_abbr for x in self.plays if x.points != 0]
            if all(x != scoring_team_list[0] for x in scoring_team_list):
                logger.error("Different teams are listed as scoring in this drive! {}".format(scoring_team_list))
            self.scoring_team = scoring_team_list[0]


def _process_play(play):  # DRY
    return Play(int(play.get('@playId', None)),
                play.get('@teamId', None),
                play.get('playDescription', None),
                Yardline(play.get('@yardlineSide'),
                         play.get('@yardlineNumber'),
                         (-1 if play['@teamId'] == play['@yardlineSide'] else 1) * (
                                 50 - int(play['@yardlineNumber'])))
                if play.get('@yardlineSide') else None,
                play.get('@playType', None),
                play.get('@scoringType', None),
                play.get('@scoringTeamId', None))


def _get_drive_details(full_dict):
    drives_dict = full_dict['drives']['drive']

    drives_list = [Drive(int(float(x['@sequence'])),  # Python has some dumb bugs man
                         x['@startTime'],
                         x['@endTime'],
                         [_process_play(y) for y in x['plays'].get('play')],
                         x['@possessionTeamAbbr']) for x in drives_dict]

    return drives_list

# class to hold Game data. Has some fields meant to be input on init (from the NFL schedule XML)
# and some fields added later (from boxscorePbP XML)
@dataclass
class Game:
    season_year: int
    season_type: str
    game_week: int
    home_team: str
    away_team: str
    game_id: str
    drives: list = dc.field(default=None, init=False)
    home_score: int = dc.field(default=None, init=False)
    away_score: int = dc.field(default=None, init=False)

    def __repr__(self):
        str_rep = """{year}_{type}_{week}, id={game_id}\t{away} ({away_score}) vs. {home} ({home_score})"""\
            .format(year=self.season_year, type=self.season_type,
                    week=self.game_week, game_id=self.game_id,
                    away=self.away_team, home=self.home_team,
                    away_score=self.away_score, home_score=self.home_score)
        return str_rep

    # The NFL data doesn't include conversion attempts after a fumble/pick-six
    def _remedy_incorrect_scoreline(self, full_dict):
        # To remedy this, first get a list of all detected plays in all drives
        detected_plays = [(x.drive_id, y) for x in self.drives for y in x.plays]

        # The NFL JSON does include a full list of scoring plays separate from the drives object - get all plays here
        scoring_plays = [_process_play(x) for x in full_dict['scoringPlays']['play']]
        [x.calculate_points() for x in scoring_plays]

        # Get any scoring plays not found in Drives
        undetected_plays = [x for x in scoring_plays if x not in [y[1] for y in detected_plays]]

        # If any scoring plays were unaccounted for, add them to the drive here
        if len(undetected_plays) > 0:
            for play in undetected_plays:
                # find the play that comes right before the undetected play
                max_play_id = max([x[1].play_id for x in detected_plays if x[1].play_id < play.play_id])
                # get drive that includes that play
                max_drive_id = [x[0] for x in detected_plays if x[1].play_id == max_play_id][0]
                # add scoring play to the end of that drive
                self.drives[max_drive_id - 1].plays.append(play)
                # recalculate drive's points
                self.drives[max_drive_id - 1].calculate_scoring()

    # given the game id, get boxscorePbP XML and populate Drives objects and all other Game fields
    def get_game_details(self):
        # Build URL, get XML, convert to dict, get out and store easy values
        game_url = "http://www.nfl.com/feeds-rs/boxscorePbp/{}.xml".format(self.game_id)
        logger.debug('Getting game details {}'.format(game_url))
        xml_string = f.download_xml(game_url)
        game_dict = xmltodict.parse(xml_string, force_list={'play': True})['boxScorePBPFeed']
        self.home_score = game_dict['score']['homeTeamScore']['@pointTotal']
        self.away_score = game_dict['score']['visitorTeamScore']['@pointTotal']

        # Extract the 'drives'/'drive' dict and all data from within it
        self.drives = _get_drive_details(game_dict)
        # Check to see if plays/drives score matches game final score. If not, fix.
        if not self.check_score_integrity():
            self._remedy_incorrect_scoreline(full_dict=game_dict)
            print(self.check_score_integrity())

        # Check to see if any duplicate plays
        for drive in self.drives:
            distinct_plays = set(x.play_id for x in drive.plays)
            if len(distinct_plays) != len(drive.plays):
                logger.warning('Duplicate play_ids found')

    # check to make sure that the number of points recorded within drives matches the top-level given result
    def check_score_integrity(self):
        drives_points = sum([x.points for y in self.drives for x in y.plays])
        game_points = int(self.home_score) + int(self.away_score)
        return drives_points == game_points

    # smart export - since I only need drive result, make this a drive-level line-output for file storage
    def export(self):
        game_data = {'game_id': self.game_id,
                     'season_year': self.season_year,
                     'season_type': self.season_type,
                     'game_week': self.game_week,
                     'home_team': self.home_team,
                     'away_team': self.away_team,
                     'drives': [{'drive_id': drive.drive_id,
                                 'drive_pos_team': drive.pos_team,
                                 'drive_start': drive.drive_start.yard_int,
                                 'drive_num_plays': len(drive.plays),
                                 'drive_scoring_team': drive.scoring_team,
                                 'drive_points': drive.points} for drive in self.drives]
                     }
        return game_data


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
