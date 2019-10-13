from dataclasses import dataclass
import dataclasses as dc
from pynfldata.data_tools import functions as f
import logging

# setup logging
logger = logging.getLogger('nfl_types.py')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclass
class Clock:
    quarter: int
    quarter_clock: str

    def __repr__(self):
        return 'Q{} {}'.format(str(self.quarter), self.quarter_clock)

# class to contain yardline data. It comes to us as "TEAM yardline" like SEA 25, so include int (-50:50)
@dataclass
class Yardline:
    yard_int: int
    side: str = dc.field(default=None, init=False)
    side_pos: int = dc.field(default=None, init=False)

    def __post_init__(self):
        if self.yard_int is None:
            self.side = None
            self.side_pos = None
        elif self.yard_int == 0:
            self.side = 'OWN'
            self.side_pos = 50
        else:
            self.side = 'OWN' if self.yard_int <= 0 else 'OPP'
            self.side_pos = 50 + self.yard_int if self.yard_int <= 0 else 50 - self.yard_int

    def __repr__(self):
        if self.yard_int == 0:
            return 'MIDFIELD'
        else:
            return '{} {}'.format(self.side, str(self.side_pos))


__scoring_dict__ = {'TD': 6, 'FG': 3, 'PAT': 1, 'PAT2': 2, 'SFTY': 2}
__fake_plays__ = ['TIMEOUT', 'END_QUARTER', 'END_HALF', 'END_GAME', 'COMMENT']

# class to contain Play data, uses above dict to calculate points
@dataclass
class Play:
    play_id: int
    pos_team: str
    description: str
    yardline: Yardline
    play_time: Clock
    play_type: str
    real_play: bool  # False if it's a end of quarter, timeout, etc
    scoring_type: str = None
    scoring_team_abbr: str = None
    points: int = 0

    # Given a scoring play, calculate points using the above dictionary
    def calculate_points(self):
        if self.scoring_type is not None:
            self.points = __scoring_dict__.get(self.scoring_type)


# class to contain Drive data. Contains all plays in drive, and some post_init calculated fields
@dataclass
class Drive:
    drive_id: int
    plays: list
    pos_team: str
    drive_start: Yardline = dc.field(default=None, init=False)
    start_time: Clock = dc.field(default=None, init=False)
    scoring_team: str = dc.field(default=None, init=False)
    points: int = dc.field(default=None, init=False)

    # After being initialized, calculate points for all plays then the drive, then calculate the drive start
    def __post_init__(self):
        [x.calculate_points() for x in self.plays]
        self.calculate_scoring()
        self.drive_start, self.start_time = self.calculate_drive_start(self.plays)

    # Function to calculate the starting yardline of a drive given the drive's plays
    # Complicated as some drives' first play has no yardline or is a kickoff, hence the recursion
    def calculate_drive_start(self, plays_list):
        if len(plays_list) == 1 and plays_list[0].play_type == 'KICK_OFF':
            return Yardline(None), Clock(None, None)
        elif plays_list[0].play_type == 'KICK_OFF':  # drives after a score have play 1 being a kickoff - discard
            return self.calculate_drive_start(plays_list[1:])
        elif plays_list[0].yardline is not None:
            return plays_list[0].yardline, plays_list[0].play_time
        else:
            logger.debug("Drive's first play has no yardline: {}".format(plays_list[0]))
            return self.calculate_drive_start(plays_list[1:])

    # Given the drive's plays, calculate drive's score and ensure only one team scored
    def calculate_scoring(self):
        if any([x.points for x in self.plays]):
            self.points = sum([x.points for x in self.plays])
            scoring_team_list = [x.scoring_team_abbr for x in self.plays if x.points != 0]
            if all(x != scoring_team_list[0] for x in scoring_team_list):
                logger.warning("Different teams are listed as scoring in this drive! {}".format(scoring_team_list))
            self.scoring_team = scoring_team_list[0]

    # For some stupid reason, newly-downloaded files have some issues with team names being post-change ones
    # This should fix that
    def correct_drive_team(self, season_year):
        if self.pos_team == 'JAX' and season_year < 2013:
            self.pos_team = 'JAC'
        if self.scoring_team == 'JAX' and season_year < 2013:
            self.scoring_team = 'JAC'

        if self.pos_team == 'LAC' and season_year <= 2017:
            self.pos_team = 'SD'
        if self.scoring_team == 'LAC' and season_year <= 2017:
            self.scoring_team = 'SD'

        if self.pos_team == 'LA' and season_year <= 2016:
            self.pos_team = 'STL'
        if self.scoring_team == 'LA' and season_year <= 2016:
            self.scoring_team = 'STL'


# Return a Play object, given a play dictionary
def _process_play_dict(play: dict):  # DRY
    # calculate Yardline - there are issues with midfield so do this separately
    if play.get('@playType') in __fake_plays__:
        yardline = Yardline(None)
    elif play.get('@yardlineNumber', None) == '50':
        yardline = Yardline(0)
    elif play.get('@yardlineSide', None) is not None:
        yardline = Yardline((-1 if play['@teamId'] == play['@yardlineSide'] else 1) * (
                                 50 - int(play['@yardlineNumber'])))
    else:
        yardline = Yardline(None)

    return Play(int(play.get('@playId', None)),
                play.get('@teamId', None),
                play.get('playDescription', None),
                yardline,
                Clock(play.get('@quarter', None), play.get('@time', None)),
                play.get('@playType', None),
                play.get('@playType', '') not in __fake_plays__,
                play.get('@scoringType', None),
                play.get('@scoringTeamId', None))

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
        scoring_plays = [_process_play_dict(x) for x in full_dict['scoringPlays']['play']]
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

    def _get_drive_details(self, full_dict):
        drives_dict = full_dict['drives']['drive']

        drives_list = [Drive(int(float(x['@sequence'])),  # Python has some dumb bugs man
                             [_process_play_dict(y) for y in x['plays'].get('play')],
                             x['@possessionTeamAbbr']) for x in drives_dict]

        for drive in drives_list:
            drive.correct_drive_team(self.season_year)

        self.drives = drives_list

    # given the game id, get boxscorePbP XML and populate Drives objects and all other Game fields
    def get_game_details(self):
        # Build URL, get XML, convert to dict, get out and store easy values
        game_url = "http://www.nfl.com/feeds-rs/boxscorePbp/{}.xml".format(self.game_id)
        logger.debug('Getting game details {}'.format(game_url))
        game_dict = f.get_data(game_url, 2, {'force_list': {'play': True}})['boxScorePBPFeed']
        self.home_score = game_dict['score']['homeTeamScore']['@pointTotal']
        self.away_score = game_dict['score']['visitorTeamScore']['@pointTotal']

        # Extract the 'drives'/'drive' dict and all data from within it
        self._get_drive_details(game_dict)
        # Check to see if plays/drives score matches game final score. If not, fix.
        if not self.check_score_integrity():
            self._remedy_incorrect_scoreline(full_dict=game_dict)
            if not self.check_score_integrity():
                logger.warning('Game has incorrect scoreline!: {}'.format(self))

        # Check to see if any duplicate plays
        for drive in self.drives:
            distinct_plays = set(x.play_id for x in drive.plays)
            if len(distinct_plays) != len(drive.plays):
                logger.warning('Duplicate play_ids found')

        return self

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
                                 'drive_start_time': str(drive.start_time),
                                 'drive_num_plays': sum([1 for x in drive.plays if x.real_play]),
                                 'drive_scoring_team': drive.scoring_team,
                                 'drive_points': drive.points} for drive in self.drives]
                     }
        return game_data
