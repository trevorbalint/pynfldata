"""Functions that apply to a whole team to make some sort of change to the team's data"""
import pandas as pd
import functools


# function meant to account for team moves by changing the historical name to match the current name
# also corrects for team abbr changes, like BOSton Patriots -> NE Patriots
# takes the dataframe, and the names of the two columns involved in this change
def make_teams_continuous(df: pd.DataFrame, name_col: str, year_col: str):
    pass_func = functools.partial(_process_team_continuity, name_col, year_col)
    df = df.apply(pass_func, axis=1)
    return df


def _process_team_continuity(name_col: str, year_col: str, row: pd.Series):
    # process the easy name-change-only ones first
    if row[name_col] == 'BOS':
        row[name_col] = 'NE'
    if row[name_col] == 'JAC':
        row[name_col] = 'JAX'
    if row[name_col] == 'PHO':
        row[name_col] = 'ARI'
    if row[name_col] == 'SD':
        row[name_col] = 'LAC'
    if row[name_col] == 'RAI':
        row[name_col] = 'OAK'

    # Now process moves
    # Colts BAL-> IND
    if row[name_col] == 'BAL' and row[year_col] < 1984:
        row[name_col] = 'IND'

    # Cardinals STL -> ARI
    if row[name_col] == 'STL' and row[year_col] < 1988:
        row[name_col] = 'ARI'

    # Oilers HOU -> TEN
    if row[name_col] == 'HOU' and row[year_col] < 1997:
        row[name_col] = 'TEN'

    # CLE Browns -> BAL Ravens
    if row[name_col] == 'CLE' and row[year_col] < 1996:
        row[name_col] = 'BAL'

    # Rams clusterfuck
    if row[name_col] == 'RAM' or (row[name_col] == 'STL' and row[year_col] > 1994):
        row[name_col] = 'LA'

    return row
