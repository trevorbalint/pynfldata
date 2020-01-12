"""File to get raw data from nfl.com and process plays out of it and upload them to existing BigQuery tables
"""
# todo better documentation
import logging
from pynfldata.data_tools import functions as f
import pandas as pd
from google.cloud import bigquery as bq
import io
import time

# setup logging
logger = logging.getLogger('plays_parser.py')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

bq_client = bq.Client()


def get_existing_games_from_bq(season_year: int):
    logger.debug('Getting games from BQ for {}'.format(str(season_year)))
    query = """SELECT DISTINCT game_id 
                 FROM `pynfldata.drives.plays_nested_v1` 
                WHERE season_year = {}""".format(season_year)
    query_job = bq_client.query(query)
    return [str(row.game_id) for row in query_job.result()]


def get_finished_games(season_year: int):
    logger.debug('Getting games from NFL feeds-rs for {}'.format(str(season_year)))
    games = f.get_games_for_years(season_year, season_year + 1)
    return games


def export_new_games_to_json():
    # Get a list of all finished games from feeds-rs, and all games currently in BigQuery
    current_year = f.get_current_game_year()
    bq_games = get_existing_games_from_bq(current_year)
    finished_games = get_finished_games(current_year)
    finished_game_ids = [x.game_id for x in finished_games]

    # Determine which finished games aren't in BQ
    new_game_ids = [x for x in finished_game_ids if x not in bq_games]
    new_games = [x.export() for x in finished_games if x.game_id in new_game_ids]

    if len(new_games) == 0:
        return None
    else:
        logger.debug('Found {} games'.format(str(len(new_games))))
        drives_df = pd.DataFrame(new_games)
        return drives_df.to_json(orient='records', lines=True)


def upload_to_bq(json_str):
    load_config = bq.job.LoadJobConfig(write_disposition='WRITE_APPEND', source_format='NEWLINE_DELIMITED_JSON')
    load_job = bq_client.load_table_from_file(json_str,
                                              "pynfldata.drives.plays_nested_v1",
                                              job_config=load_config)
    while load_job.running():
        time.sleep(2)

    return load_job


json_string = export_new_games_to_json()
if json_string:
    logger.debug('Loading {} games to BQ'.format(str(len(json_string.split('\n')))))
    job = upload_to_bq(io.StringIO(json_string))

    if job.exception():
        logger.warning('Exception in BQ job! {}'.format(job.exception()))

    if job.done():
        logger.info('Load to BQ complete')
else:
    logger.info('No games found to upload')
