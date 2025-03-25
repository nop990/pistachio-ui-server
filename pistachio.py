#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Pistachio is a player projection calculator for OOTP 2026 - it was originally developed for OOTP 2024
# See explanatory video on the 'squirrel plays' YouTube channel
# To configure export of data from game: OOTP 2026 and do Game > Game Settings > Database > Database Tools > Configure Data Export to CSV Files
# Then to export data: Game > Game Settings > Database > Database Tools > Export Data to CSV Files
import pandas as pd
import numpy as np
import toml
import os



# In[ ]:


# this cell includes various things to configure the code to your saved game

# specify the folder where the game saves csv files
# Go to Game > Game Settings > Database > Database Tools > Open data import/export folder to find this
# Go to Game > Game Settings > Database > Database Tools > Open data import\export folder to find this

# specify the folder in which this .ipynb file, flagged.txt and club_lookup.csv are saved
base_dir = os.path.dirname(os.path.abspath(__file__))
config = toml.load(base_dir + '/config/settings.toml')

filepath = config['Settings']['csv_path']

# specify the folder in which to save the outputs - this where the player lists will go once the code has done its calculations
export_filepath = base_dir + '/reports'

# identify the ID for your sporting director
# look in the coaches.csv file for this - it is in the 'coach_id' column (look up the name of the sportng director in the 'last_name' column)
ID = config['Settings']['scout_id']

# identify the team being managed
# this ensures all players for this team are included in the outputs - for other teams there are WAR-based cut-offs to prevent the outputs being too large
# look in the 'club_lookup.csv' to see a list of team codes
team_managed = config['Settings']['team_id']

# set the minimum groundball percentage for a pitcher to be included in the outputs
# setting this to 59 will include groundball and extreme groundball pitchers only; set this lower to include other types of pitchers (54 is league average)
min_gb = config['Settings']['gb_weight']


# In[ ]:


# read in players from CSVs and remove retired players from dataframe
df1 = pd.read_csv(filepath + '/players.csv')
df1 = df1[df1.retired != 1]


# In[ ]:


# bring in scouted ratings - scouting coach id needs to be updated to the correct id for my team's scouting director
df2 = pd.read_csv(filepath + '/players_scouted_ratings.csv')
df2 = df2[df2.scouting_coach_id == ID]


# In[ ]:


# merge the dataframes
merged_df = pd.merge(df1, df2, on='player_id')
merged_df.rename(columns={'team_id_x': 'team_id'}, inplace=True)
merged_df.rename(columns={'league_id_x': 'league_id'}, inplace=True)
merged_df.rename(columns={'position_x': 'position'}, inplace=True)
merged_df.rename(columns={'role_x': 'role'}, inplace=True)


# In[ ]:


# Read the player career stats csv file for hitters
stats_df = pd.read_csv(filepath + '/players_career_batting_stats.csv')


# In[ ]:


# Filtering the dataframe for level_id = 1 and split_id = 1 (this means MLB stats and all pa not just for left or right handers)
career_stats_df = stats_df[(stats_df['level_id'] == 1) & (stats_df['split_id'] == 1)]

# summing the MLB career stats for each player id
career_stats_df = career_stats_df.groupby('player_id')[['pa', 'bb', 'k', 'h', 'd', 't', 'hr', 'hp', 'pitches_seen']].sum().reset_index()

# calculate MLB rate stats (hp = hit by pitch)
career_stats_df['bb%_mlb'] = career_stats_df['bb'] / career_stats_df['pa']
career_stats_df['k%_mlb'] = career_stats_df['k'] / career_stats_df['pa']
career_stats_df['1b%_mlb'] = career_stats_df['h'] / career_stats_df['pa']
career_stats_df['2b%_mlb'] = career_stats_df['d'] / career_stats_df['pa']
career_stats_df['3b%_mlb'] = career_stats_df['t'] / career_stats_df['pa']
career_stats_df['hr%_mlb'] = career_stats_df['hr'] / career_stats_df['pa']
career_stats_df['hp%_mlb'] = career_stats_df['hp'] / career_stats_df['pa']
career_stats_df['pitches/plate_appearance_mlb'] = career_stats_df['pitches_seen'] / career_stats_df['pa']

# rename pa to pa_mlb (to prevent confusion with current single-season pa, which is just called pa further down)
career_stats_df.rename(columns={'pa': 'pa_mlb'}, inplace=True)
career_stats_df = career_stats_df.round(3)


# In[ ]:


# Merging career_stats_df into merged_df based on player_id
columns_to_add = ['player_id', 'pa_mlb', 'bb%_mlb', 'k%_mlb', '1b%_mlb', '2b%_mlb', '3b%_mlb', 'hr%_mlb', 'hp%_mlb', 'pitches/plate_appearance_mlb']
merged_df = merged_df.merge(career_stats_df[columns_to_add], on='player_id', how='left')


# In[ ]:


# Filter to get the latest year only and where split_id and level_id are both 1 (this means MLB stats and all pa not just for left or right handers)
stats_df = stats_df[(stats_df['level_id'] == 1) & (stats_df['split_id'] == 1)]
max_year = stats_df['year'].max()
stats_df = stats_df[stats_df['year'] == max_year]
stats_df = stats_df.groupby('player_id')[['ab', 'h', 'k', 'pa', 'pitches_seen', 'g', 'gs', 'd', 't', 'hr', 'r', 'rbi', 'sb', 'cs', 'bb', 'ibb', 'gdp', 'sh', 'sf', 'hp', 'ci', 'wpa', 'stint', 'ubr', 'war']].sum().reset_index()


# In[ ]:


# add single-season 'pa' and 'war' to merged_df and standardize war to 650 pa
merged_df = pd.merge(merged_df, stats_df[['player_id', 'pa', 'war']], on='player_id', how='left')
merged_df = merged_df.rename(columns={'war': 'WAR_actual'})
merged_df['sWAR_actual'] = (650 / merged_df['pa']) * merged_df['WAR_actual']


# In[ ]:


# same idea but pulling out innings pitched for pitchers
stats_df = pd.read_csv(filepath + '/players_career_pitching_stats.csv')
stats_df = stats_df[(stats_df['level_id'] == 1) & (stats_df['split_id'] == 1)]
max_year = stats_df['year'].max()
stats_df = stats_df[stats_df['year'] == max_year]
stats_df = stats_df.groupby('player_id')[['ip', 'war', 'ra9war']].sum().reset_index()
merged_df = pd.merge(merged_df, stats_df[['player_id', 'ip', 'war', 'ra9war']], on='player_id', how='left')
merged_df = merged_df.rename(columns={'war': 'WAR_actual_p'})
merged_df['sWAR_actual_p'] = (180 / merged_df['ip']) * merged_df['WAR_actual_p']

# replace NaN with blank in ip column
merged_df['ip'].fillna('', inplace=True)


# In[ ]:


# drop columns from dataframe that are not needed

# List of columns to drop
columns_to_drop = [
    "nick_name", "city_of_birth_id", "nation_id", "second_nation_id", "last_league_id",
    "last_team_id", "last_organization_id", "language_ids0", "language_ids1", "uniform_number",
    "experience", "person_type", "historical_id", "historical_team_id", "best_contract_offer_id",
    "injury_is_injured", "injury_dtd_injury", "injury_career_ending", "injury_dl_left",
    "injury_dl_playoff_round", "injury_left", "dtd_injury_effect", "dtd_injury_effect_hit",
    "dtd_injury_effect_throw", "dtd_injury_effect_run", "injury_id", "injury_id2",
    "injury_dtd_injury2", "injury_left2", "dtd_injury_effect2", "dtd_injury_effect_hit2",
    "dtd_injury_effect_throw2", "dtd_injury_effect_run2", "prone_overall", "prone_leg",
    "prone_back", "prone_arm", "fatigue_pitches0", "fatigue_pitches1", "fatigue_pitches2",
    "fatigue_pitches3", "fatigue_pitches4", "fatigue_pitches5", "fatigue_points",
    "fatigue_played_today", "running_ratings_speed_x", "running_ratings_stealing_x",
    "running_ratings_baserunning_x", "college", "school",
    "commit_school", "hidden", "turned_coach", "hall_of_fame", "rust", "inducted",
    "strategy_override_team", "strategy_stealing", "strategy_running", "strategy_bunt_for_hit",
    "strategy_sac_bunt", "strategy_hit_run", "strategy_hook_start", "strategy_hook_relief",
    "strategy_pitch_count", "strategy_pitch_around", "strategy_never_pinch_hit",
    "strategy_defensive_sub", "strategy_dtd_sit_min", "strategy_dtd_allow_ph", "local_pop",
    "national_pop", "draft_protected", "morale", "morale_player_performance",
    "morale_team_performance", "morale_team_transactions", "expectation", "morale_player_role",
    "on_loan", "loan_league_id", "loan_team_id", "team_id_y", "league_id_y", "position_y", "role_y",
    "acquired", "acquired_date", "draft_year", "draft_round", "draft_supplemental", "draft_pick",
    "draft_overall_pick", "draft_eligible", "hsc_status", "redshirt", "picked_in_draft",
    "draft_league_id", "draft_team_id", "morale_mod", "morale_team_chemistry",
    "scouting_coach_id", "scouting_team_id"
]

# Drop the columns
merged_df = merged_df.drop(columns=columns_to_drop, errors='ignore')


# In[ ]:


# create new columns to preseve the 20-80 scale ratings exported by OOTP 26 (actually this is on a 20-100 scale as super-ratings of 85, 90, 95 and 100 are possible)
# Mapping of original column names to new names
column_mapping = {
    "batting_ratings_overall_eye": "eye2080",
    "batting_ratings_overall_strikeouts": "avK2080",
    "batting_ratings_overall_power": "pow2080",
    "batting_ratings_overall_gap": "gap2080",
    "batting_ratings_overall_babip": "babip2080",
    "batting_ratings_talent_eye": "eye2080p",
    "batting_ratings_talent_strikeouts": "avK2080p",
    "batting_ratings_talent_power": "pow2080p",
    "batting_ratings_talent_gap": "gap2080p",
    "batting_ratings_talent_babip": "babip2080p",
    "fielding_ratings_catcher_ability": "cabi2080",
    "fielding_ratings_catcher_arm": "carm2080",
    "fielding_ratings_infield_range": "ifrng2080",
    "fielding_ratings_infield_error": "iferr2080",
    "fielding_ratings_infield_arm": "ifarm2080",
    "fielding_ratings_turn_doubleplay": "turndp2080",
    "fielding_ratings_outfield_arm": "ofarm2080",
    "fielding_ratings_outfield_range": "ofrng2080",
    "fielding_ratings_outfield_error": "oferr2080",
    "pitching_ratings_pitches_fastball": "fb2080",
    "pitching_ratings_pitches_slider": "sl2080",
    "pitching_ratings_pitches_curveball": "crv2080",
    "pitching_ratings_pitches_screwball": "scrw2080",
    "pitching_ratings_pitches_forkball": "frk2080",
    "pitching_ratings_pitches_changeup": "chng2080",
    "pitching_ratings_pitches_sinker": "sink2080",
    "pitching_ratings_pitches_splitter": "spli2080",
    "pitching_ratings_pitches_knuckleball": "knuc2080",
    "pitching_ratings_pitches_cutter": "cut2080",
    "pitching_ratings_pitches_circlechange": "cchng2080",
    "pitching_ratings_pitches_knucklecurve": "kcurv2080",
    "pitching_ratings_misc_stamina": "stam2080",
    "pitching_ratings_overall_stuff": "stuff2080",
    "pitching_ratings_overall_control": "ctrl2080",
    "pitching_ratings_overall_movement": "mvt2080",
    "pitching_ratings_overall_hra": "hra2080",
    "pitching_ratings_overall_pbabip": "pbabip2080",
    "pitching_ratings_pitches_talent_fastball": "fb2080p",
    "pitching_ratings_pitches_talent_slider": "sl2080p",
    "pitching_ratings_pitches_talent_curveball": "crv2080p",
    "pitching_ratings_pitches_talent_screwball": "scrw2080p",
    "pitching_ratings_pitches_talent_forkball": "frk2080p",
    "pitching_ratings_pitches_talent_changeup": "chng2080p",
    "pitching_ratings_pitches_talent_sinker": "sink2080p",
    "pitching_ratings_pitches_talent_splitter": "spli2080p",
    "pitching_ratings_pitches_talent_knuckleball": "knuc2080p",
    "pitching_ratings_pitches_talent_cutter": "cut2080p",
    "pitching_ratings_pitches_talent_circlechange": "cchng2080p",
    "pitching_ratings_pitches_talent_knucklecurve": "kcurv2080p",
    "pitching_ratings_talent_stuff": "stuff2080p",
    "pitching_ratings_talent_control": "ctrl2080p",
    "pitching_ratings_talent_movement": "mvt2080p",
    "pitching_ratings_talent_hra": "hra2080p",
    "pitching_ratings_talent_pbabip": "pbabip2080p"
}

# Create duplicate columns in merged_df with new names
for original_col, new_col in column_mapping.items():
    if original_col in merged_df.columns:
        merged_df[new_col] = merged_df[original_col]


# In[ ]:


# replace the 20-100 ratings with ratings on a 1-250 scale in line with the export from OOTP 2024, which the calculations below are based on

# Define the find-replace mapping between the 20-100 scale and the 1-250 scale
replace_map = {
    20: 6,  25: 20,  30: 35,  35: 52,  40: 69,
    45: 85,  50: 101,  55: 117,  60: 134,  65: 150,
    70: 166,  75: 181,  80: 201,  85: 213,  90: 225,
    95: 238,  100: 250
}

# List of columns to apply the replacement
columns_to_replace = [
    "batting_ratings_overall_eye", "batting_ratings_overall_strikeouts",
    "batting_ratings_overall_power", "batting_ratings_overall_gap",
    "batting_ratings_overall_babip", "batting_ratings_talent_eye",
    "batting_ratings_talent_strikeouts", "batting_ratings_talent_power",
    "batting_ratings_talent_gap", "batting_ratings_talent_babip",
    "fielding_ratings_catcher_ability", "fielding_ratings_catcher_arm", "fielding_ratings_catcher_framing",
    "fielding_ratings_infield_range", "fielding_ratings_infield_error",
    "fielding_ratings_infield_arm", "fielding_ratings_turn_doubleplay",
    "fielding_ratings_outfield_arm", "fielding_ratings_outfield_range",
    "fielding_ratings_outfield_error", "pitching_ratings_pitches_fastball",
    "pitching_ratings_pitches_slider", "pitching_ratings_pitches_curveball",
    "pitching_ratings_pitches_screwball", "pitching_ratings_pitches_forkball",
    "pitching_ratings_pitches_changeup", "pitching_ratings_pitches_sinker",
    "pitching_ratings_pitches_splitter", "pitching_ratings_pitches_knuckleball",
    "pitching_ratings_pitches_cutter", "pitching_ratings_pitches_circlechange",
    "pitching_ratings_pitches_knucklecurve", "pitching_ratings_misc_stamina",
    "pitching_ratings_overall_stuff", "pitching_ratings_overall_control",
    "pitching_ratings_overall_movement", "pitching_ratings_pitches_talent_fastball",
    "pitching_ratings_pitches_talent_slider", "pitching_ratings_pitches_talent_curveball",
    "pitching_ratings_pitches_talent_screwball", "pitching_ratings_pitches_talent_forkball",
    "pitching_ratings_pitches_talent_changeup", "pitching_ratings_pitches_talent_sinker",
    "pitching_ratings_pitches_talent_splitter", "pitching_ratings_pitches_talent_knuckleball",
    "pitching_ratings_pitches_talent_cutter", "pitching_ratings_pitches_talent_circlechange",
    "pitching_ratings_pitches_talent_knucklecurve", "pitching_ratings_talent_stuff",
    "pitching_ratings_talent_control", "pitching_ratings_talent_movement"
]

# Apply the replacement
merged_df[columns_to_replace] = merged_df[columns_to_replace].replace(replace_map)


# In[ ]:


# calculate standardized WAR for hitters based on the MOPS projection system by Sgt Mushroom

# calculate bb%
def calculate_bb(row):
    if row['batting_ratings_overall_eye'] <= 100:
        return ((row['batting_ratings_overall_eye'] * 0.0007268758188) + 0.001460739)
    elif row['batting_ratings_overall_eye'] > 100:
        return ((row['batting_ratings_overall_eye'] * 0.0012280964) - 0.0469974639)

merged_df['bb%'] = merged_df.apply(calculate_bb, axis=1)


# In[ ]:


# calculate k% (with fudge factor to account for high avK players having too high a OPS+ projection vs career performance - this is based on OOTP 24 gameplay experience)
# 0.1 is the fudge factor - this is applied here and in the potential calculations
# strikeout rating also capped at 180 to prevent sky-high k% projections
# before adding this players with high avK and gap power but low HR power were getting too high of an OPS+ projection

def calculate_k(row):
    # Cap batting_ratings_overall_strikeouts at 180
    strikeouts = min(row['batting_ratings_overall_strikeouts'], 180)

    # Adjusted strikeouts calculation
    adjusted_strikeouts = strikeouts + ((100 - strikeouts) * 0.1)

    # Apply formula based on strikeout level
    if strikeouts <= 100:
        return (adjusted_strikeouts * -0.002454367) + 0.4655792299
    elif 101 <= strikeouts <= 220:
        return (adjusted_strikeouts * -0.0016592514) + 0.383395059
    else:
        return (adjusted_strikeouts * 0) + 0.02385

# Apply function to calculate 'k%'
merged_df['k%'] = merged_df.apply(calculate_k, axis=1)


# In[ ]:


# add new function to calculate hr%
def calculate_hr(row):
    if row['batting_ratings_overall_power'] <= 100:
        return (row['batting_ratings_overall_power'] * 0.0001965717055) + 0.0057097943
    elif row['batting_ratings_overall_power'] > 100:
        return (row['batting_ratings_overall_power'] * 0.0005767110238) - 0.0305087264

merged_df['hr%'] = merged_df.apply(calculate_hr, axis=1)


# In[ ]:


# Adjusted function to calculate part1_2b with fudge factor to reduce impact of batting_ratings_overall_gap (0.6666 is the fudge factor)
def calculate_part1_2b(row):
    # Reduce impact by adjusting the distance to 100
    adjusted_gap = row['batting_ratings_overall_gap'] - ((row['batting_ratings_overall_gap'] - 100) * 0.6666)
    return (adjusted_gap * 0.0005759923464) + 0.0046460781

# Function to calculate part2_2b
def calculate_part2_2b(row):
    if row['batting_ratings_overall_power'] <= 100:
        return (row['batting_ratings_overall_power'] * -0.0000508547503) + 0.0669597896 - 0.0628
    else:
        return (row['batting_ratings_overall_power'] * -0.00008542726043) + 0.071154717 - 0.0628

# Function to calculate part3_2b
def calculate_part3_2b(row):
    if row['batting_ratings_overall_strikeouts'] <= 100:
        return (row['batting_ratings_overall_strikeouts'] * -0.0002084865135) + 0.0828934273 - 0.0628
    elif 101 <= row['batting_ratings_overall_strikeouts'] <= 220:
        return (row['batting_ratings_overall_strikeouts'] * -0.000008259599351) + 0.0708287518 - 0.0628
    else:
        return (row['batting_ratings_overall_strikeouts'] * 0) + 0.053 - 0.0628

# Apply calculations to dataframe
merged_df['part1_2b'] = merged_df.apply(calculate_part1_2b, axis=1)
merged_df['part2_2b'] = merged_df.apply(calculate_part2_2b, axis=1)
merged_df['part3_2b'] = merged_df.apply(calculate_part3_2b, axis=1)

# Compute final 2b% value
merged_df['2b%'] = merged_df['part1_2b'] + merged_df['part2_2b'] + merged_df['part3_2b']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_2b', 'part2_2b', 'part3_2b'])


# In[ ]:


# add new function to calculate 3b%

def calculate_part1_3b(row):
    return (row['batting_ratings_overall_gap'] * 0.00004451978242) + 0.00007767274633

def calculate_part2_3b(row):
    if row['batting_ratings_overall_power'] <= 100:
        return (row['batting_ratings_overall_power'] * -0.00000206286281) + 0.0046134367 - 0.0044
    else:
        return (row['batting_ratings_overall_power'] * -0.000007041275071) + 0.0051236727 - 0.0044

def calculate_part3_3b(row):
    if row['batting_ratings_overall_strikeouts'] <= 100:
        return (row['batting_ratings_overall_strikeouts'] * -0.00001098275967) + 0.0055735013 - 0.0044
    elif 101 <= row['batting_ratings_overall_strikeouts'] <= 220:
        return (row['batting_ratings_overall_strikeouts'] * -0.00000526736139) + 0.0048976614 - 0.0044
    else:
        return (row['batting_ratings_overall_strikeouts'] * 0) + 0.0037 - 0.0044

merged_df['part1_3b'] = merged_df.apply(calculate_part1_3b, axis=1)
merged_df['part2_3b'] = merged_df.apply(calculate_part2_3b, axis=1)
merged_df['part3_3b'] = merged_df.apply(calculate_part3_3b, axis=1)

merged_df['3b%'] = merged_df['part1_3b'] + merged_df['part2_3b'] + merged_df['part3_3b']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_3b', 'part2_3b', 'part3_3b'])


# In[ ]:


# calculate 1b%

def calculate_part1_1b(row):
    if row['batting_ratings_overall_babip'] <= 100:
        return (row['batting_ratings_overall_babip'] * 0.0015140038) + 0.1281801944
    else:
        return (row['batting_ratings_overall_babip'] * 0.000964994955) + 0.1837822012

def calculate_part2_1b(row):
    return (row['batting_ratings_overall_gap'] * -0.0003887320573) + 0.3178756912 - 0.28

def calculate_part3_1b(row):
    if row['batting_ratings_overall_strikeouts'] <= 100:
        return (row['batting_ratings_overall_strikeouts'] * 0.000149985378) + 0.2648525907 - 0.28
    elif 101 <= row['batting_ratings_overall_strikeouts'] <= 220:
        return (row['batting_ratings_overall_strikeouts'] * 0.00005179135613) + 0.2754044069 - 0.28
    else:
        return (row['batting_ratings_overall_strikeouts'] * 0) + 0.286 - 0.28

merged_df['part1_1b'] = merged_df.apply(calculate_part1_1b, axis=1)
merged_df['part2_1b'] = merged_df.apply(calculate_part2_1b, axis=1)
merged_df['part3_1b'] = merged_df.apply(calculate_part3_1b, axis=1)

merged_df['1b%'] = merged_df['part1_1b'] + merged_df['part2_1b'] + merged_df['part3_1b']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_1b', 'part2_1b', 'part3_1b'])


# In[ ]:


# calculate Offensive Runs Created per game (orc_per_game)
merged_df['orc_per_game'] = ((merged_df['bb%'] - 0.0738) / 0.875) + ((merged_df['k%'] - 0.2195) / -1.217) + ((merged_df['hr%'] - 0.0272) / 0.219) + ((merged_df['2b%'] - 0.0628) / 0.693) + ((merged_df['3b%'] - 0.0044) / 0.0519) + ((merged_df['1b%'] - 0.28) / 0.594)


# In[ ]:


# calculate offensive WAR
merged_df['toWAR'] = (merged_df['orc_per_game'] * 162) / 10


# In[ ]:


# calculate c_def (catcher defense)

def calculate_part1_c_def(row):
    if row['fielding_ratings_catcher_framing'] <= 40:
        return (row['fielding_ratings_catcher_framing'] * 0) + 5.311
    elif 41 <= row['fielding_ratings_catcher_framing'] <= 61:
        return (row['fielding_ratings_catcher_framing'] * -0.0204) + 6.125
    else:
        return (row['fielding_ratings_catcher_framing'] * -0.0028608333) + 4.998622222

def calculate_part2_c_def(row):
    return (row['fielding_ratings_catcher_arm'] * -0.0006034965035) + 4.712621212

merged_df['part1_c_def'] = merged_df.apply(calculate_part1_c_def, axis=1)
merged_df['part2_c_def'] = merged_df.apply(calculate_part2_c_def, axis=1)

merged_df['c_def'] = 4.6385 - merged_df['part1_c_def'] + 4.6385 - merged_df['part2_c_def']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_c_def', 'part2_c_def'])


# In[ ]:


# calculate 1b_def

def calculate_part1_1b_def(row):
    return (row['height'] * -0.0014708625) + 4.917895105

def calculate_part2_1b_def(row):
    return (row['fielding_ratings_infield_range'] * -0.0001325174825) + 4.645893939

def calculate_part3_1b_def(row):
    return (row['fielding_ratings_infield_error'] * -0.0001685314685) + 4.658242424

def calculate_part4_1b_def(row):
    return (row['fielding_ratings_infield_arm'] * 0) + 4.6385

def calculate_part5_1b_def(row):
    return (row['fielding_ratings_turn_doubleplay'] * 0) + 4.6385

merged_df['part1_1b_def'] = merged_df.apply(calculate_part1_1b_def, axis=1)
merged_df['part2_1b_def'] = merged_df.apply(calculate_part2_1b_def, axis=1)
merged_df['part3_1b_def'] = merged_df.apply(calculate_part3_1b_def, axis=1)
merged_df['part4_1b_def'] = merged_df.apply(calculate_part4_1b_def, axis=1)
merged_df['part5_1b_def'] = merged_df.apply(calculate_part5_1b_def, axis=1)

merged_df['1b_def'] = 4.6385 - merged_df['part1_1b_def'] + 4.6385 - merged_df['part2_1b_def'] + 4.6385 - merged_df['part3_1b_def'] + 4.6385 - merged_df['part4_1b_def'] + 4.6385 - merged_df['part5_1b_def']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_1b_def', 'part2_1b_def', 'part3_1b_def', 'part4_1b_def', 'part5_1b_def'])


# In[ ]:


# calculate 2b_def

def calculate_part1_2b_def(row):
    if row['fielding_ratings_turn_doubleplay'] <= 200:
        return (row['fielding_ratings_turn_doubleplay'] * -0.0012715152) + 4.825866667
    else:
        return (row['fielding_ratings_turn_doubleplay'] * 0) + 4.569020596

def calculate_part2_2b_def(row):
    return (row['fielding_ratings_infield_range'] * -0.0016293706) + 4.844484848

def calculate_part3_2b_def(row):
    if row['fielding_ratings_infield_error'] <= 160:
        return (row['fielding_ratings_infield_error'] * -0.0006464285714) + 4.720428571
    else:
        return (row['fielding_ratings_infield_error'] * 0) + 4.628635714

def calculate_part4_2b_def(row):
    return (row['fielding_ratings_infield_arm'] * -0.0002284965035) + 4.658287879

merged_df['part1_2b_def'] = merged_df.apply(calculate_part1_2b_def, axis=1)
merged_df['part2_2b_def'] = merged_df.apply(calculate_part2_2b_def, axis=1)
merged_df['part3_2b_def'] = merged_df.apply(calculate_part3_2b_def, axis=1)
merged_df['part4_2b_def'] = merged_df.apply(calculate_part4_2b_def, axis=1)

merged_df['2b_def'] = 4.6385 - merged_df['part1_2b_def'] + 4.6385 - merged_df['part2_2b_def'] + 4.6385 - merged_df['part3_2b_def'] + 4.6385 - merged_df['part4_2b_def']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_2b_def', 'part2_2b_def', 'part3_2b_def', 'part4_2b_def'])


# In[ ]:


# calculate 3b_def

def calculate_part1_3b_def(row):
    return (row['fielding_ratings_turn_doubleplay'] * 0) + 4.6385

def calculate_part2_3b_def(row):
    return (row['fielding_ratings_infield_range'] * -0.0015907343) + 4.808545455

def calculate_part3_3b_def(row):
    if row['fielding_ratings_infield_error'] <= 180:
        return (row['fielding_ratings_infield_error'] * -0.0008091666667) + 4.748583333
    else:
        return (row['fielding_ratings_infield_error'] * 0) + 4.61

def calculate_part4_3b_def(row):
    if row['fielding_ratings_infield_arm'] <= 60:
        return (row['fielding_ratings_infield_arm'] * 0) + 4.788
    else:
        return (row['fielding_ratings_infield_arm'] * -0.0021283333) + 4.963644444

merged_df['part1_3b_def'] = merged_df.apply(calculate_part1_3b_def, axis=1)
merged_df['part2_3b_def'] = merged_df.apply(calculate_part2_3b_def, axis=1)
merged_df['part3_3b_def'] = merged_df.apply(calculate_part3_3b_def, axis=1)
merged_df['part4_3b_def'] = merged_df.apply(calculate_part4_3b_def, axis=1)

merged_df['3b_def'] = 4.6385 - merged_df['part1_3b_def'] + 4.6385 - merged_df['part2_3b_def'] + 4.6385 - merged_df['part3_3b_def'] + 4.6385 - merged_df['part4_3b_def']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_3b_def', 'part2_3b_def', 'part3_3b_def', 'part4_3b_def'])



# In[ ]:


# calculate ss_def

def calculate_part1_ss_def(row):
    if row['fielding_ratings_turn_doubleplay'] <= 200:
        return (row['fielding_ratings_turn_doubleplay'] * -0.0007603030303) + 4.7435333333
    else:
        return (row['fielding_ratings_turn_doubleplay'] * 0) + 4.597

def calculate_part2_ss_def(row):
    if row['fielding_ratings_infield_range'] <= 60:
        return (row['fielding_ratings_infield_range'] * 0) + 4.985
    else:
        return (row['fielding_ratings_infield_range'] * -0.0045308333) + 5.330155556

def calculate_part3_ss_def(row):
    if row['fielding_ratings_infield_error'] <= 180:
        return (row['fielding_ratings_infield_error'] * -0.0011291667) + 4.793027778
    else:
        return (row['fielding_ratings_infield_error'] * 0) + 4.588

def calculate_part4_ss_def(row):
    return (row['fielding_ratings_infield_arm'] * -0.0011823427) + 4.809787879

merged_df['part1_ss_def'] = merged_df.apply(calculate_part1_ss_def, axis=1)
merged_df['part2_ss_def'] = merged_df.apply(calculate_part2_ss_def, axis=1)
merged_df['part3_ss_def'] = merged_df.apply(calculate_part3_ss_def, axis=1)
merged_df['part4_ss_def'] = merged_df.apply(calculate_part4_ss_def, axis=1)

merged_df['ss_def'] = 4.6385 - merged_df['part1_ss_def'] + 4.6385 - merged_df['part2_ss_def'] + 4.6385 - merged_df['part3_ss_def'] + 4.6385 - merged_df['part4_ss_def']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_ss_def', 'part2_ss_def', 'part3_ss_def', 'part4_ss_def'])


# In[ ]:


# calculate lf_def

def calculate_part1_lf_def(row):
    return (row['fielding_ratings_outfield_arm'] * -0.000190034965) + 4.665287879

def calculate_part2_lf_def(row):
    if row['fielding_ratings_outfield_range'] <= 40:
        return (row['fielding_ratings_outfield_range'] * 0) + 4.9135
    elif 41 <= row['fielding_ratings_outfield_range'] <= 80:
        return (row['fielding_ratings_outfield_range'] * -0.000825) + 4.9445
    elif 81 <= row['fielding_ratings_outfield_range'] <= 100:
        return (row['fielding_ratings_outfield_range'] * -0.01135) + 5.787
    elif 101 <= row['fielding_ratings_outfield_range'] <= 180:
        return (row['fielding_ratings_outfield_range'] * -0.000625) + 4.661
    else:
        return (row['fielding_ratings_outfield_range'] * 0) + 4.54

def calculate_part3_lf_def(row):
    return (row['fielding_ratings_outfield_error'] * 0) + 4.6385

merged_df['part1_lf_def'] = merged_df.apply(calculate_part1_lf_def, axis=1)
merged_df['part2_lf_def'] = merged_df.apply(calculate_part2_lf_def, axis=1)
merged_df['part3_lf_def'] = merged_df.apply(calculate_part3_lf_def, axis=1)

merged_df['lf_def'] = 4.6385 - merged_df['part1_lf_def'] + 4.6385 - merged_df['part2_lf_def'] + 4.6385 - merged_df['part3_lf_def']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_lf_def', 'part2_lf_def', 'part3_lf_def'])


# In[ ]:


# calculate cf_def

def calculate_part1_cf_def(row):
    return (row['fielding_ratings_outfield_arm'] * -0.000190034965) + 4.665287879

def calculate_part2_cf_def(row):
    if row['fielding_ratings_outfield_range'] <= 80:
        return (row['fielding_ratings_outfield_range'] * 0) + 4.86
    else:
        return (row['fielding_ratings_outfield_range'] * -0.0030625) + 5.15075

def calculate_part3_cf_def(row):
    return (row['fielding_ratings_outfield_error'] * -0.0001664335664) + 4.659636364

merged_df['part1_cf_def'] = merged_df.apply(calculate_part1_cf_def, axis=1)
merged_df['part2_cf_def'] = merged_df.apply(calculate_part2_cf_def, axis=1)
merged_df['part3_cf_def'] = merged_df.apply(calculate_part3_cf_def, axis=1)

merged_df['cf_def'] = 4.6385 - merged_df['part1_cf_def'] + 4.6385 - merged_df['part2_cf_def'] + 4.6385 - merged_df['part3_cf_def']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_cf_def', 'part2_cf_def', 'part3_cf_def'])


# In[ ]:


# calculate rf_def

def calculate_part1_rf_def(row):
    if row['fielding_ratings_outfield_arm'] <= 60:
        return (row['fielding_ratings_outfield_arm'] * 0) + 4.683
    elif 61 <= row['fielding_ratings_outfield_arm'] <= 180:
        return (row['fielding_ratings_outfield_arm'] * -0.0005428571429) + 4.716142857
    else:
        return (row['fielding_ratings_outfield_arm'] * 0) + 4.618

def calculate_part2_rf_def(row):
    if row['fielding_ratings_outfield_range'] <= 80:
        return (row['fielding_ratings_outfield_range'] * -0.000455) + 4.89
    elif 81 <= row['fielding_ratings_outfield_range'] <= 160:
        return (row['fielding_ratings_outfield_range'] * -0.004385) + 5.1866
    else:
        return (row['fielding_ratings_outfield_range'] * 0) + 4.5

def calculate_part3_rf_def(row):
    return (row['fielding_ratings_outfield_error'] * 0) + 4.6385

merged_df['part1_rf_def'] = merged_df.apply(calculate_part1_rf_def, axis=1)
merged_df['part2_rf_def'] = merged_df.apply(calculate_part2_rf_def, axis=1)
merged_df['part3_rf_def'] = merged_df.apply(calculate_part3_rf_def, axis=1)

merged_df['rf_def'] = 4.6385 - merged_df['part1_rf_def'] + 4.6385 - merged_df['part2_rf_def'] + 4.6385 - merged_df['part3_rf_def']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_rf_def', 'part2_rf_def', 'part3_rf_def'])


# In[ ]:


# calculate defensive WAR (tdWAR) for each position
merged_df['c_tdWAR'] = ((merged_df['c_def'] * 162) / 10) + 1.5
merged_df['1b_tdWAR'] = ((merged_df['1b_def'] * 162) / 10) + 0.5
merged_df['2b_tdWAR'] = ((merged_df['2b_def'] * 162) / 10) + 1.75
merged_df['3b_tdWAR'] = ((merged_df['3b_def'] * 162) / 10) + 1.8
merged_df['ss_tdWAR'] = ((merged_df['ss_def'] * 162) / 10) + 2
merged_df['lf_tdWAR'] = ((merged_df['lf_def'] * 162) / 10) + 0.3
merged_df['cf_tdWAR'] = ((merged_df['cf_def'] * 162) / 10) + 2.5
merged_df['rf_tdWAR'] = ((merged_df['rf_def'] * 162) / 10) + 0.6
merged_df['dh_tdWAR'] = 0


# In[ ]:


# calculate standardised WAR (sWAR) at each position
merged_df['c_sWAR'] = merged_df['toWAR'] + merged_df['c_tdWAR']
merged_df['1b_sWAR'] = merged_df['toWAR'] + merged_df['1b_tdWAR']
merged_df['2b_sWAR'] = merged_df['toWAR'] + merged_df['2b_tdWAR']
merged_df['3b_sWAR'] = merged_df['toWAR'] + merged_df['3b_tdWAR']
merged_df['ss_sWAR'] = merged_df['toWAR'] + merged_df['ss_tdWAR']
merged_df['lf_sWAR'] = merged_df['toWAR'] + merged_df['lf_tdWAR']
merged_df['cf_sWAR'] = merged_df['toWAR'] + merged_df['cf_tdWAR']
merged_df['rf_sWAR'] = merged_df['toWAR'] + merged_df['rf_tdWAR']
merged_df['dh_sWAR'] = merged_df['toWAR'] + merged_df['dh_tdWAR']


# In[ ]:


# calculate the best value of sWAR across all the positions for each player
merged_df['best_sWAR'] = merged_df[['c_sWAR', '1b_sWAR', '2b_sWAR', '3b_sWAR', 'ss_sWAR', 'lf_sWAR', 'cf_sWAR', 'rf_sWAR', 'dh_sWAR']].max(axis=1)

# add a column to say which position the player should play based on the best value of sWAR (remove the (_sWAR) part of the column name)
merged_df['best_sWAR_pos'] = merged_df[['c_sWAR', '1b_sWAR', '2b_sWAR', '3b_sWAR', 'ss_sWAR', 'lf_sWAR', 'cf_sWAR', 'rf_sWAR', 'dh_sWAR']].idxmax(axis=1).str.replace('_sWAR', '')


# In[ ]:


# merge first name and last name into a column called name
merged_df['name'] = merged_df['first_name'] + " " + merged_df['last_name']


# In[ ]:


# following cells recalculate the MOPS methodology for all batters, but using talent not overall ratings

# add new function to calculate bb%_potential
def calculate_bb_pot(row):
    if row['batting_ratings_talent_eye'] <= 100:
        return ((row['batting_ratings_talent_eye'] * 0.0007268758188) + 0.001460739)
    elif row['batting_ratings_talent_eye'] >= 101:
        return ((row['batting_ratings_talent_eye'] * 0.0012280964) - 0.0469974639)

merged_df['bb%_pot'] = merged_df.apply(calculate_bb_pot, axis=1)


# In[ ]:


# k%_potential (using the same 'fudge factor' adjustments as for k%)
def calculate_k_pot(row):
    # Cap batting_ratings_talent_strikeouts at 180
    strikeouts = min(row['batting_ratings_talent_strikeouts'], 180)

    # Adjusted strikeouts calculation
    adjusted_strikeouts = strikeouts + ((100 - strikeouts) * 0.1)

    # Apply formula based on strikeout level
    if strikeouts <= 100:
        return (adjusted_strikeouts * -0.002454367) + 0.4655792299
    elif 101 <= strikeouts <= 220:
        return (adjusted_strikeouts * -0.0016592514) + 0.383395059
    else:
        return (adjusted_strikeouts * 0) + 0.02385

# Apply function to calculate 'k%_pot'
merged_df['k%_pot'] = merged_df.apply(calculate_k_pot, axis=1)


# In[ ]:


# calculate hr%_potential
def calculate_hr_pot(row):
    if row['batting_ratings_talent_power'] <= 100:
        return (row['batting_ratings_talent_power'] * 0.0001965717055) + 0.0057097943
    elif row['batting_ratings_talent_power'] > 100:
        return (row['batting_ratings_talent_power'] * 0.0005767110238) - 0.0305087264

merged_df['hr%_pot'] = merged_df.apply(calculate_hr_pot, axis=1)


# In[ ]:


# calculate 2b% potential with same fudge factor adjustment as 2b%
def calculate_part1_2b_pot(row):
    # Reduce impact by adjusting the distance to 100 (multiplying by 0.6666 instead of direct value)
    adjusted_gap = row['batting_ratings_talent_gap'] - ((row['batting_ratings_talent_gap'] - 100) * 0.6666)
    return (adjusted_gap * 0.0005759923464) + 0.0046460781

# Function to calculate part2_2b_pot
def calculate_part2_2b_pot(row):
    if row['batting_ratings_talent_power'] <= 100:
        return (row['batting_ratings_talent_power'] * -0.0000508547503) + 0.0669597896 - 0.0628
    else:
        return (row['batting_ratings_talent_power'] * -0.00008542726043) + 0.071154717 - 0.0628

# Function to calculate part3_2b_pot
def calculate_part3_2b_pot(row):
    if row['batting_ratings_talent_strikeouts'] <= 100:
        return (row['batting_ratings_talent_strikeouts'] * -0.0002084865135) + 0.0828934273 - 0.0628
    elif 101 <= row['batting_ratings_talent_strikeouts'] <= 220:
        return (row['batting_ratings_talent_strikeouts'] * -0.000008259599351) + 0.0708287518 - 0.0628
    else:
        return (row['batting_ratings_talent_strikeouts'] * 0) + 0.053 - 0.0628

# Apply calculations to dataframe
merged_df['part1_2b_pot'] = merged_df.apply(calculate_part1_2b_pot, axis=1)
merged_df['part2_2b_pot'] = merged_df.apply(calculate_part2_2b_pot, axis=1)
merged_df['part3_2b_pot'] = merged_df.apply(calculate_part3_2b_pot, axis=1)

# Compute final 2b%_pot value
merged_df['2b%_pot'] = merged_df['part1_2b_pot'] + merged_df['part2_2b_pot'] + merged_df['part3_2b_pot']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_2b_pot', 'part2_2b_pot', 'part3_2b_pot'])


# In[ ]:


# calculate 3b%_potential

def calculate_part1_3b_pot(row):
    return (row['batting_ratings_talent_gap'] * 0.00004451978242) + 0.00007767274633

def calculate_part2_3b_pot(row):
    if row['batting_ratings_talent_power'] <= 100:
        return (row['batting_ratings_talent_power'] * -0.00000206286281) + 0.0046134367 - 0.0044
    else:
        return (row['batting_ratings_talent_power'] * -0.000007041275071) + 0.0051236727 - 0.0044

def calculate_part3_3b_pot(row):
    if row['batting_ratings_talent_strikeouts'] <= 100:
        return (row['batting_ratings_talent_strikeouts'] * -0.00001098275967) + 0.0055735013 - 0.0044
    elif 101 <= row['batting_ratings_talent_strikeouts'] <= 220:
        return (row['batting_ratings_talent_strikeouts'] * -0.00000526736139) + 0.0048976614 - 0.0044
    else:
        return (row['batting_ratings_talent_strikeouts'] * 0) + 0.0037 - 0.0044

merged_df['part1_3b_pot'] = merged_df.apply(calculate_part1_3b_pot, axis=1)
merged_df['part2_3b_pot'] = merged_df.apply(calculate_part2_3b_pot, axis=1)
merged_df['part3_3b_pot'] = merged_df.apply(calculate_part3_3b_pot, axis=1)

merged_df['3b%_pot'] = merged_df['part1_3b_pot'] + merged_df['part2_3b_pot'] + merged_df['part3_3b_pot']

# Drop the intermediate calculations from the dataframe
merged_df = merged_df.drop(columns=['part1_3b_pot', 'part2_3b_pot', 'part3_3b_pot'])


# In[ ]:


# calculate 1b%_pot

def calculate_part1_1b_pot(row):
    if row['batting_ratings_talent_babip'] <= 100:
        return (row['batting_ratings_talent_babip'] * 0.0015140038) + 0.1281801944
    else:
        return (row['batting_ratings_talent_babip'] * 0.000964994955) + 0.1837822012

def calculate_part2_1b_pot(row):
    return (row['batting_ratings_talent_gap'] * -0.0003887320573) + 0.3178756912 - 0.28

def calculate_part3_1b_pot(row):
    if row['batting_ratings_talent_strikeouts'] <= 100:
        return (row['batting_ratings_talent_strikeouts'] * 0.000149985378) + 0.2648525907 - 0.28
    elif 101 <= row['batting_ratings_talent_strikeouts'] <= 220:
        return (row['batting_ratings_talent_strikeouts'] * 0.00005179135613) + 0.2754044069 - 0.28
    else:
        return (row['batting_ratings_talent_strikeouts'] * 0) + 0.286 - 0.28

merged_df['part1_1b_pot'] = merged_df.apply(calculate_part1_1b_pot, axis=1)
merged_df['part2_1b_pot'] = merged_df.apply(calculate_part2_1b_pot, axis=1)
merged_df['part3_1b_pot'] = merged_df.apply(calculate_part3_1b_pot, axis=1)

merged_df['1b%_pot'] = merged_df['part1_1b_pot'] + merged_df['part2_1b_pot'] + merged_df['part3_1b_pot']


# In[ ]:


# calculate Offensive Runs Created Potential per game (orc_per_game_pot)
merged_df['orc_per_game_pot'] = ((merged_df['bb%_pot'] - 0.0738) / 0.875) + ((merged_df['k%_pot'] - 0.2195) / -1.217) + ((merged_df['hr%_pot'] - 0.0272) / 0.219) + ((merged_df['2b%_pot'] - 0.0628) / 0.693) + ((merged_df['3b%_pot'] - 0.0044) / 0.0519) + ((merged_df['1b%_pot'] - 0.28) / 0.594)


# In[ ]:


# calculate offensive WAR potential
merged_df['toWAR_pot'] = (merged_df['orc_per_game_pot'] * 162) / 10


# In[ ]:


# calculate standardised WAR potential (sWAR_pot) at each position
merged_df['c_sWAR_pot'] = merged_df['toWAR_pot'] + merged_df['c_tdWAR']
merged_df['1b_sWAR_pot'] = merged_df['toWAR_pot'] + merged_df['1b_tdWAR']
merged_df['2b_sWAR_pot'] = merged_df['toWAR_pot'] + merged_df['2b_tdWAR']
merged_df['3b_sWAR_pot'] = merged_df['toWAR_pot'] + merged_df['3b_tdWAR']
merged_df['ss_sWAR_pot'] = merged_df['toWAR_pot'] + merged_df['ss_tdWAR']
merged_df['lf_sWAR_pot'] = merged_df['toWAR_pot'] + merged_df['lf_tdWAR']
merged_df['cf_sWAR_pot'] = merged_df['toWAR_pot'] + merged_df['cf_tdWAR']
merged_df['rf_sWAR_pot'] = merged_df['toWAR_pot'] + merged_df['rf_tdWAR']
merged_df['dh_sWAR_pot'] = merged_df['toWAR_pot'] + merged_df['dh_tdWAR']


# In[ ]:


# calculate the best value of sWAR_pot across all the positions for each player
merged_df['best_sWAR_pot'] = merged_df[['c_sWAR_pot', '1b_sWAR_pot', '2b_sWAR_pot', '3b_sWAR_pot', 'ss_sWAR_pot', 'lf_sWAR_pot', 'cf_sWAR_pot', 'rf_sWAR_pot', 'dh_sWAR_pot']].max(axis=1)

# add a column to say which position the player should play based on the best value of sWAR (remove the (_sWAR) part of the column name)
merged_df['best_sWAR_pot_pos'] = merged_df[['c_sWAR_pot', '1b_sWAR_pot', '2b_sWAR_pot', '3b_sWAR_pot', 'ss_sWAR_pot', 'lf_sWAR_pot', 'cf_sWAR_pot', 'rf_sWAR_pot', 'dh_sWAR_pot']].idxmax(axis=1).str.replace('_sWAR', '')

print(merged_df.head())


# In[ ]:


# calculated modified best position based on fielding ratings - i.e. whether a hitter 'has a position' or is just a 1b/dh

def determine_positions(row):
    positions = []  # List to store all eligible positions

    # Apply position rules
    if row['fielding_ratings_catcher_framing'] >= 150:
        positions.append('C')
    if row['fielding_ratings_infield_range'] > 160:
        positions.append('SS')
    if 133 < row['fielding_ratings_infield_range'] < 159:
        positions.append('2B')
    if row['fielding_ratings_infield_range'] > 111 and row['fielding_ratings_infield_arm'] > 133:
        positions.append('3B')
    if row['fielding_ratings_outfield_range'] > 160:
        positions.append('CF')
    if 133 < row['fielding_ratings_outfield_range'] < 159:
        positions.append('RF')
    if 111 < row['fielding_ratings_outfield_range'] < 133:
        positions.append('LF')

    # Determine 'has_pos' column (yes if qualified for any position, else blank)
    has_pos = "yes" if positions else ""

    # Convert list of positions into a string (comma-separated)
    field_positions = ", ".join(positions) if positions else ""

    return pd.Series([has_pos, field_positions])

# Apply function to dataframe
merged_df[['has_pos', 'field']] = merged_df.apply(determine_positions, axis=1)


# In[ ]:


# determining how many pitches with rating over 45 (on 20-80 scale; equivalent to 85 on 0-250 scale) a pitcher has
# the pitch quality threshold in OOTP 24 was 50; this has been lowered to 45 for OOTP 26 as pitch ratings look lower (1-250 scale 45 = 85, 50 = 101 as per above THIS MAY CHANGE)
pitch_minimum_rating = 85
pitch_columns = ['pitching_ratings_pitches_fastball', 'pitching_ratings_pitches_slider', 'pitching_ratings_pitches_curveball', 'pitching_ratings_pitches_screwball', 'pitching_ratings_pitches_forkball', 'pitching_ratings_pitches_changeup', 'pitching_ratings_pitches_sinker', 'pitching_ratings_pitches_splitter', 'pitching_ratings_pitches_knuckleball', 'pitching_ratings_pitches_cutter', 'pitching_ratings_pitches_circlechange', 'pitching_ratings_pitches_knucklecurve']
merged_df['no_of_pitches'] = merged_df[pitch_columns].apply(lambda row: sum(row >= pitch_minimum_rating), axis=1)


# In[ ]:


# determining whether a pitcher is a starter (NB needs to be a groundball pitcher with stamina >= 40 on 20-80 scale and at least 3 pitches)
# new threshold added for OOTP 26 of pbabip >= 45
merged_df['is_sp'] = ((merged_df['pitching_ratings_misc_ground_fly'] >= min_gb) &
                      (merged_df['pitching_ratings_misc_stamina'] >= 68) &
                      (merged_df['pbabip2080'] >= 45) &
                      (merged_df['no_of_pitches'] >= 3))
merged_df['is_sp'] = merged_df['is_sp'].astype(int)


# In[ ]:


# determining whether a pitcher is a reliever (NB needs to have at least 2 pitches and be a groundball pitcher, and not a starter as defined above)
# new threshold added for OOTP 26 of pbabip >= 45
merged_df['is_rp'] = ((merged_df['pitching_ratings_misc_ground_fly'] >= min_gb) &
                      (merged_df['no_of_pitches'] >= 2) &
                      (merged_df['pbabip2080'] >= 45) &
                      (merged_df['is_sp'] == 0))
merged_df['is_rp'] = merged_df['is_rp'].astype(int)


# In[ ]:


# halve ratings for use in Donkeykong FIP calculation which is on a 1-125 scale
merged_df['donkeykong_stuff'] = merged_df['pitching_ratings_overall_stuff'] / 2
merged_df['donkeykong_control'] = merged_df['pitching_ratings_overall_control'] / 2
merged_df['donkeykong_movement'] = merged_df['pitching_ratings_overall_movement'] / 2


# In[ ]:


# preserve here the 'donkeykong FIP' used in prior versions of pistachio
merged_df['donkeyFIP'] = 8.661141 - (0.01747 * merged_df['donkeykong_stuff']) - (0.03291 * merged_df['donkeykong_movement']) - (0.01737 * merged_df['donkeykong_control'])

# calculate FIP projection for each pitcher based on v2 weightings (22% stuff, 22% control, 51% home run allowed, 5% pbabip)
# first calculate blended pitcher rating
# then map onto FIP scale (roughly so that blended pitcher ratings of 65, 50 and 45 correspond to FIP- of 70, 100 and 130 and FIP of 2.75, 4.1 and 5.45 respectively)
# assumes league average FIP is 4.1, and a league-leading FIP is about 2.75
merged_df['pitcher_rtg'] = ((0.25 * merged_df['stuff2080']) + (0.19 * merged_df['ctrl2080']) + (0.51 * merged_df['hra2080']) + (0.05 * merged_df['pbabip2080']))

# FIP is one value when pitcher rating above 50 and another when below
merged_df['FIP'] = np.where(
    merged_df['pitcher_rtg'] > 50,
    4.1 - ((merged_df['pitcher_rtg'] - 50) * ((4.1 - 2.75) / 15)),
    4.1 + ((50 - merged_df['pitcher_rtg']) * ((5.45 - 4.1) / 5))
)


# In[ ]:


# allocate FIP to sp and rp pitchers (starters and relievers)
merged_df['sp_FIP'] = merged_df['is_sp'] * merged_df['FIP']
merged_df['rp_FIP'] = merged_df['is_rp'] * merged_df['FIP']


# In[ ]:


# calculate starting pitcher standardised WAR from FIP assuming 180 IP (approach per OOTP calculator)

# Calculate 'fipr9', 'rpw', and 'sp_sWAR'
merged_df['fipr9'] = merged_df['FIP'] + 4.62 - 4.25
merged_df['rpw'] = ((((12.375 * 4.62) + (5.625 * merged_df['fipr9'])) / 18) + 2) * 1.5
merged_df['p_sWAR'] = (((((4.62-merged_df['fipr9']) / merged_df['rpw']) + 0.12) * 180) / 9)
merged_df['sp_sWAR'] = merged_df['p_sWAR'] * merged_df['is_sp']

# calculate relief pitcher standardised WAR equal to one-third of sp_sWAR only for pitchers where is_rp is 1
merged_df['rp_sWAR'] = (merged_df['p_sWAR'] / 3) * merged_df['is_rp']


# In[ ]:


# determining how many pitches a pitcher potentially has based on minimum potential pitch ratings (same logic as for current ratings)
pitch_pot_columns = ['pitching_ratings_pitches_talent_fastball', 'pitching_ratings_pitches_talent_slider', 'pitching_ratings_pitches_talent_curveball', 'pitching_ratings_pitches_talent_screwball', 'pitching_ratings_pitches_talent_forkball', 'pitching_ratings_pitches_talent_changeup', 'pitching_ratings_pitches_talent_sinker', 'pitching_ratings_pitches_talent_splitter', 'pitching_ratings_pitches_talent_knuckleball', 'pitching_ratings_pitches_talent_cutter', 'pitching_ratings_pitches_talent_circlechange', 'pitching_ratings_pitches_talent_knucklecurve']
merged_df['no_of_pitches_pot'] = merged_df[pitch_pot_columns].apply(lambda row: sum(row >= pitch_minimum_rating), axis=1)


# In[ ]:


# determining whether a pitcher is potentially a starter
# new threshold added for OOTP 26 of pbabip potential >= 45
merged_df['is_sp_pot'] = ((merged_df['pitching_ratings_misc_ground_fly'] >= min_gb) &
                      (merged_df['pitching_ratings_misc_stamina'] >= 69) &
                      (merged_df['pbabip2080p'] >= 45) &
                      (merged_df['no_of_pitches_pot'] >= 3))
merged_df['is_sp_pot'] = merged_df['is_sp_pot'].astype(int)


# In[ ]:


# determining whether a pitcher is potentially a reliever
# new threshold added for OOTP 26 of pbabip potential >= 45
merged_df['is_rp_pot'] = ((merged_df['pitching_ratings_misc_ground_fly'] >= min_gb) &
                      (merged_df['no_of_pitches_pot'] >= 2) &
                      (merged_df['pbabip2080p'] >= 45) &
                      (merged_df['is_sp_pot'] == 0))
merged_df['is_rp_pot'] = merged_df['is_rp_pot'].astype(int)


# In[ ]:


# halve potential ratings for use in Donkeykong FIP calculation which is on a 1-125 scale
merged_df['donkeykong_stuff_pot'] = merged_df['pitching_ratings_talent_stuff'] / 2
merged_df['donkeykong_control_pot'] = merged_df['pitching_ratings_talent_control'] / 2
merged_df['donkeykong_movement_pot'] = merged_df['pitching_ratings_talent_movement'] / 2


# In[ ]:


# preserve here the 'donkeykong FIP' potential used in prior versions of pistachio
merged_df['donkeyFIP_pot'] = 8.661141 - (0.01747 * merged_df['donkeykong_stuff_pot']) - (0.03291 * merged_df['donkeykong_movement_pot']) - (0.01737 * merged_df['donkeykong_control_pot'])

# calculate blended pitcher rating and FIP projection based on potential in same way as for current ratings
merged_df['pitcher_rtg_pot'] = ((0.25 * merged_df['stuff2080p']) + (0.19 * merged_df['ctrl2080p']) + (0.51 * merged_df['hra2080p']) + (0.05 * merged_df['pbabip2080p']))
merged_df['FIP_pot'] = np.where(
    merged_df['pitcher_rtg_pot'] > 50,
    4.1 - ((merged_df['pitcher_rtg_pot'] - 50) * ((4.1 - 2.75) / 15)),
    4.1 + ((50 - merged_df['pitcher_rtg_pot']) * ((5.45 - 4.1) / 5))
)


# In[ ]:


# allocate potential FIP to potential sp and rp pitchers (starters and relievers)
merged_df['sp_FIP_pot'] = merged_df['is_sp_pot'] * merged_df['FIP_pot']
merged_df['rp_FIP_pot'] = merged_df['is_rp_pot'] * merged_df['FIP_pot']


# In[ ]:


# calculate potential starting pitcher standardised WAR from FIP assuming 180 IP (approach per OOTP calculator)

# Calculate 'fipr9', 'rpw', and 'sp_sWAR'
merged_df['fipr9_pot'] = merged_df['FIP_pot'] + 4.62 - 4.25
merged_df['rpw_pot'] = ((((12.375 * 4.62) + (5.625 * merged_df['fipr9_pot'])) / 18) + 2) * 1.5
merged_df['p_sWAR_pot'] = (((((4.62-merged_df['fipr9_pot']) / merged_df['rpw_pot']) + 0.12) * 180) / 9)
merged_df['sp_sWAR_pot'] = merged_df['p_sWAR_pot'] * merged_df['is_sp_pot']

# calculate relief pitcher standardised WAR equal to one-third of sp_sWAR only for pitchers where is_rp is 1
merged_df['rp_sWAR_pot'] = (merged_df['p_sWAR_pot'] / 3) * merged_df['is_rp_pot']


# In[ ]:


# calculate HRs per 650, OBP and OPS+ for both current and future ratings
merged_df['bb650'] = (merged_df['bb%'] * 650)
merged_df['hr650'] = (merged_df['hr%'] * ((650 - merged_df['bb650'])))
merged_df['k650'] = (merged_df['k%'] * ((650 - merged_df['bb650'])))
merged_df['2b'] = (merged_df['2b%'] * ((650 - merged_df['bb650']-merged_df['hr650']-merged_df['k650'])))
merged_df['3b'] = (merged_df['3b%'] * ((650 - merged_df['bb650']-merged_df['hr650']-merged_df['k650'])))
merged_df['1b'] = (merged_df['1b%'] * ((650 - merged_df['bb650']-merged_df['hr650']-merged_df['k650']-merged_df['2b']-merged_df['3b'])))
merged_df['obp'] = ((merged_df['bb650'] + merged_df['hr650'] + merged_df['2b'] + merged_df['3b'] + merged_df['1b']) / 650)
merged_df['slg'] = (((merged_df['1b'])+(2*merged_df['2b'])+(3*merged_df['3b'])+(4*merged_df['hr650']))/(650-merged_df['bb650']))
merged_df['ops'] = (merged_df['obp']+merged_df['slg'])
merged_df['OPS+'] = (((merged_df['ops'])/0.734)*100).round(0)
merged_df['HR'] = merged_df['hr650'].round(0)
merged_df['OBP'] = merged_df['obp'].round(3)

merged_df['bb650_pot'] = (merged_df['bb%_pot'] * 650)
merged_df['hr650_pot'] = (merged_df['hr%_pot'] * ((650 - merged_df['bb650_pot'])))
merged_df['k650_pot'] = (merged_df['k%_pot'] * ((650 - merged_df['bb650_pot'])))
merged_df['2b_pot'] = (merged_df['2b%_pot'] * ((650 - merged_df['bb650_pot']-merged_df['hr650_pot']-merged_df['k650_pot'])))
merged_df['3b_pot'] = (merged_df['3b%_pot'] * ((650 - merged_df['bb650_pot']-merged_df['hr650_pot']-merged_df['k650_pot'])))
merged_df['1b_pot'] = (merged_df['1b%_pot'] * ((650 - merged_df['bb650_pot']-merged_df['hr650_pot']-merged_df['k650_pot']-merged_df['2b_pot']-merged_df['3b_pot'])))
merged_df['obp_pot'] = ((merged_df['bb650_pot'] + merged_df['hr650_pot'] + merged_df['2b_pot'] + merged_df['3b_pot'] + merged_df['1b_pot']) / 650)
merged_df['slg_pot'] = (((merged_df['1b_pot'])+(2*merged_df['2b_pot'])+(3*merged_df['3b_pot'])+(4*merged_df['hr650_pot']))/(650-merged_df['bb650_pot']))
merged_df['ops_pot'] = (merged_df['obp_pot']+merged_df['slg_pot'])
merged_df['OPS+_p'] = (((merged_df['ops_pot'])/0.734)*100).round(0)
merged_df['HR_p'] = ((merged_df['hr650_pot'])).round(0)
merged_df['OBP_p'] = merged_df['obp_pot'].round(3)



# In[ ]:


# calculate OPS+ for mlb career for each player_id

merged_df['bb650_mlb'] = (merged_df['bb%_mlb'] * 650)
merged_df['hr650_mlb'] = (merged_df['hr%_mlb'] * 650)
merged_df['k650_mlb'] = (merged_df['k%_mlb'] * 650)
merged_df['2b_mlb'] = (merged_df['2b%_mlb'] * 650)
merged_df['3b_mlb'] = (merged_df['3b%_mlb'] * 650)
merged_df['1b_mlb'] = (merged_df['1b%_mlb'] * 650)
merged_df['obp_mlb'] = ((merged_df['bb650_mlb'] + merged_df['hr650_mlb'] + merged_df['2b_mlb'] + merged_df['3b_mlb'] + merged_df['1b_mlb']) / 650)
merged_df['slg_mlb'] = (((merged_df['1b_mlb'])+(2*merged_df['2b_mlb'])+(3*merged_df['3b_mlb'])+(4*merged_df['hr650_mlb']))/(650-merged_df['bb650_mlb']))
merged_df['ops_mlb'] = (merged_df['obp_mlb']+merged_df['slg_mlb'])
merged_df['OPS+_mlb'] = (((merged_df['ops_mlb'])/0.734)*100).round(0)
merged_df['HR_mlb'] = merged_df['hr650_mlb'].round(0)
merged_df['OBP_mlb'] = merged_df['obp_mlb'].round(3)


# In[ ]:


# Determine if a player is a minor leaguer
merged_df['minor'] = (merged_df['organization_id'] != merged_df['team_id']).astype(int)


# In[ ]:


# look up which club each player plays for based on 'organization_id' and a lookup table
club_lookup = pd.read_csv(base_dir + '/config/club_lookup.csv')

# Merge the 'club' column to the dataframe
merged_df = pd.merge(merged_df, club_lookup[['club_id', 'club']], left_on='organization_id', right_on='club_id', how='left')

# Drop the 'club_id' column as it's no longer needed
merged_df = merged_df.drop(columns='club_id')


# In[ ]:


# Read names from text file into a list - paste in here players to be flagged (eg players available in draft, or players in a shortlist or player search)
# convert to lowercase so can read if ALL CAPS (i.e. in a shortlist)
with open(base_dir + '/config/flagged.txt', 'r') as f:
    drafted_names = [name.lower() for name in f.read().splitlines()]

# Convert 'name' column to lowercase and check for membership
merged_df['in_list'] = np.where(merged_df['name'].str.lower().isin(drafted_names), 'flagged', '')


# In[ ]:


# This compares a batter's OPS+ against a standard trajectory for a player of their age
# Players are classified into three growth lanes (low, medium, high) based on their fielding position
# Instead of using best_sWAR_pos, we now use our multi-position logic (field column)

# Position groups:
groupA = ["1B", "DH"]         # First Base or Designated Hitter (Lowest Priority)
groupB = ["C", "SS", "CF"]    # Catcher, Shortstop, Center Field (Highest Priority)
groupC = ["2B", "3B", "LF", "RF"]  # Second Base, Third Base, Left Field, Right Field (Medium Priority)

# Track value dictionaries by age
groupA_lookup = {
    14: 64, 15: 65, 16: 65, 17: 66, 18: 68,
    19: 72, 20: 76, 21: 83, 22: 91, 23: 99,
    24: 103, 25: 107, 26: 108, 27: 110, 28: 110,
    29: 110, 30: 110
}
groupB_lookup = {
    14: 52, 15: 53, 16: 53, 17: 54, 18: 56,
    19: 59, 20: 62, 21: 68, 22: 75, 23: 81,
    24: 84, 25: 88, 26: 89, 27: 90, 28: 90,
    29: 90, 30: 90
}
groupC_lookup = {
    14: 58, 15: 59, 16: 59, 17: 60, 18: 62,
    19: 65, 20: 69, 21: 75, 22: 83, 23: 90,
    24: 93, 25: 98, 26: 99, 27: 100, 28: 100,
    29: 100, 30: 100
}

# Extend each dictionary to handle ages 31 through 50 by repeating the final value
for age in range(31, 51):
    groupA_lookup[age] = 110
    groupB_lookup[age] = 90
    groupC_lookup[age] = 100

def get_track_value(row):
    """
    Returns a track value based on a player's age and fielding positions.
    Uses multi-position eligibility (field column) instead of best_sWAR_pos.
    Clamps age to [14..50].
    Prioritization: Group B > Group C > Group A.
    """
    # Ensure age is an integer
    age = int(row['age'])
    # Clamp age to the range [14..50] for lookup
    if age < 14:
        age = 14
    elif age > 50:
        age = 50

    # Extract eligible positions from the 'field' column
    positions = row['field'].split(", ") if row['field'] else []

    # Default track value if no valid position found
    default_growth = 100

    # Determine the best track value based on position groups with the new prioritization
    if any(pos in groupB for pos in positions):  # Highest priority
        return groupB_lookup.get(age, 90)
    elif any(pos in groupC for pos in positions):  # Second priority
        return groupC_lookup.get(age, 100)
    elif any(pos in groupA for pos in positions):  # Lowest priority
        return groupA_lookup.get(age, 110)
    else:
        return default_growth  # If no valid position, assign default

# Create the 'track' column using the updated position logic with new prioritization
merged_df['track'] = merged_df.apply(get_track_value, axis=1)


# In[ ]:


# Calculate the OPS+ at age 21 and 27 for each player based on yearly growth factors for a median trajectory

import math

# Define the growth factors
growth_factors = {
    14: 0.00,
    15: 0.01,
    16: 0.00,
    17: 0.02,
    18: 0.04,
    19: 0.05,
    20: 0.06,
    21: 0.09,
    22: 0.10,
    23: 0.08,
    24: 0.04,
    25: 0.05,
    26: 0.01,
    27: 0.01,
    28: 0.00,
}
# Set ages 29 through 50 to zero
for a in range(29, 51):
    growth_factors[a] = 0.0

def get_ops21(age, current_ops):
    """
    Calculate projected OPS+ at age 21 based on yearly growth factors.
    If age >= 22, returns 0.
    If age == 21, returns current OPS+.
    If age < 21, multiplies by growth rates for (age+1) to 21.
    """
    if age >= 22:
        return 0
    elif age == 21:
        return current_ops

    projected_ops = float(current_ops)
    for next_age in range(age + 1, 22):
        factor = growth_factors.get(next_age, 0.0)
        projected_ops *= (1 + factor)

    return math.floor(projected_ops)

def get_ops27(age, current_ops):
    """
    Calculate projected OPS+ at age 27 based on yearly growth factors.
    If age >= 27, returns current OPS+ (no growth applied).
    If age < 27, multiplies by growth rates for (age+1) to 27.
    """
    if age >= 27:
        return current_ops
    elif age == 27:
        return current_ops

    projected_ops = float(current_ops)
    for next_age in range(age + 1, 28):  # Project up to age 27
        factor = growth_factors.get(next_age, 0.0)
        projected_ops *= (1 + factor)

    return math.floor(projected_ops)

# Apply functions to create the ops21 and ops27 columns in merged_df
merged_df['ops21'] = merged_df.apply(
    lambda row: get_ops21(row['age'], row['OPS+']),
    axis=1
)

merged_df['ops27'] = merged_df.apply(
    lambda row: get_ops27(row['age'], row['OPS+']),
    axis=1
)

merged_df['Tpct'] = (merged_df['OPS+'] / merged_df['track'].replace(0, float('nan'))).round(2)

# Add the onT column: If Tpct >= 1, set it to "track"
merged_df['onT'] = merged_df.apply(lambda row: f"{row['club']} track" if row['Tpct'] >= 1 else "", axis=1)


# In[ ]:


# Function to determine the denominator used to calc PPct based on fielding position (i.e. to gauge how impressive potential OPS+ is)
def get_divisor(row):
    """
    Determines the divisor based on a player's fielding positions.
    Uses multi-position eligibility (field column) instead of best_sWAR_pos.
    Prioritization: Group B > Group C > Group A.
    """
    positions = row['field'].split(", ") if row['field'] else []

    # Default divisor if no valid position is found
    default_divisor = 100

    # Determine the divisor based on position groups
    if any(pos in groupB for pos in positions):  # Highest priority (hardest positions)
        return 90
    elif any(pos in groupC for pos in positions):  # Medium priority
        return 100
    elif any(pos in groupA for pos in positions):  # Lowest priority (easiest positions)
        return 110
    else:
        return default_divisor  # If no valid position, assign default

# Add the Ppct column: OPS+_p divided by the appropriate divisor based on the new logic
merged_df['Ppct'] = merged_df.apply(
    lambda row: (row['OPS+_p'] / get_divisor(row)) if pd.notna(row['OPS+_p']) else None,
    axis=1
)

# Round Ppct to 2 decimal places
merged_df['Ppct'] = merged_df['Ppct'].round(2)

# Add the Pscore column: Tpct * Ppct, rounded to 2 decimal places
merged_df['Pscore'] = (merged_df['Tpct'] * merged_df['Ppct']).round(2)


# In[ ]:


# export a simple dataframe with the pitcher outputs in the 'reports' folder of this pistachio project
columns = ['name', 'age', 'club', 'minor', 'ip', 'throws', 'sp_sWAR', 'rp_sWAR','sp_sWAR_pot', 'rp_sWAR_pot', 'FIP','FIP_pot', 'in_list']
df = merged_df[columns]

# change 'throws' so that 1 = R, 2 = L
df['throws'] = df['throws'].replace({1: 'R', 2: 'L'})

# Filter the DataFrame (WAR limit removes hitters and pitchers with no WAR potential)
pitchers = df[(df['club'] == team_managed) | (df['sp_sWAR'] >= 0.1) | (df['rp_sWAR'] >= 0.1) | (df['sp_sWAR_pot'] >= 0.1) | (df['rp_sWAR_pot'] >= 0.1) | (df['in_list'] == 'flagged')]
pitchers.rename(columns={
    'sp_sWAR': 'sp',
    'rp_sWAR': 'rp',
    'sp_sWAR_pot': 'spP',
    'rp_sWAR_pot': 'rpP'
}, inplace=True)

pitchers.to_csv(export_filepath + '/pitcher_sWAR.csv', index=False)


# In[ ]:


# Add new column 'OPS+_pF' based on 'has_pos'
# this means the output is searchable for batting projections for players that 'have a position' in the field
# and are not just 1b/dh prospects
merged_df['OPS+_pF'] = merged_df.apply(lambda row: row['OPS+_p'] if row['has_pos'] == "yes" else -999, axis=1)
# same for Pscore
merged_df['PscoreF'] = merged_df.apply(lambda row: row['Pscore'] if row['has_pos'] == "yes" else -999, axis=1)


# In[ ]:


# round columns
round_zero_dp = ['pa', 'HR', 'OPS+', 'HR_p', 'OPS+_p', 'OPS+_pF', 'HR_mlb']
round_two_dp = [
    'best_sWAR', 'c_sWAR', '1b_sWAR', '2b_sWAR', '3b_sWAR', 'ss_sWAR',
    'lf_sWAR', 'cf_sWAR', 'rf_sWAR', 'dh_sWAR', 'best_sWAR_pot', 'c_sWAR_pot',
    '1b_sWAR_pot', '2b_sWAR_pot', '3b_sWAR_pot', 'ss_sWAR_pot', 'lf_sWAR_pot',
    'cf_sWAR_pot', 'rf_sWAR_pot', 'dh_sWAR_pot', 'toWAR', 'toWAR_pot',
    'c_tdWAR', '1b_tdWAR', '2b_tdWAR', '3b_tdWAR', 'ss_tdWAR',
    'lf_tdWAR', 'cf_tdWAR', 'rf_tdWAR', 'dh_tdWAR', 'sp_sWAR', 'rp_sWAR',
    'sp_FIP', 'rp_FIP', 'sp_sWAR_pot', 'rp_sWAR_pot', 'sp_FIP_pot', 'rp_FIP_pot', 'FIP', 'FIP_pot', 'Pscore', 'PscoreF'
]

# Ensure empty strings are replaced before converting to int
merged_df[round_zero_dp] = merged_df[round_zero_dp].replace('', 0).fillna(0).round(0).astype(int)

# Fill NaN values in round_two_dp columns with -999 before rounding to 2 decimal places
merged_df[round_two_dp] = merged_df[round_two_dp].fillna(-999).round(2)

# Make HR_mlb blank if 0 or NaN
merged_df['HR_mlb'] = merged_df['HR_mlb'].replace({0: '', np.nan: ''})


# In[ ]:


# rename columns for use in batter output as required (some other columns are renamed below also)
merged_df.rename(columns={
    "batting_ratings_overall_eye": "eye",
    "batting_ratings_overall_power": "power",
    "batting_ratings_overall_babip": "babip",
    "batting_ratings_overall_gap": "gap",
    "batting_ratings_overall_strikeouts": "avoidk",
    "running_ratings_speed_y": "speed",
}, inplace=True)


# In[ ]:


print(merged_df.head())

# export merged_df to csv
merged_df.to_csv(export_filepath + '/merged_df1329.csv', index=False)


# In[ ]:


# export a simple dataframe with the batter WAR outputs in the 'reports' folder of this pistachio project
columns = ['name', 'age', 'club', 'minor', 'pa', 'best_sWAR', 'best_sWAR_pos', 'field', 'bats', 'HR_mlb', 'HR', 'OBP', 'OPS+', 'best_sWAR_pot', 'HR_p', 'OBP_p', 'OPS+_p', 'OPS+_pF', 'Tpct', 'c_sWAR', '1b_sWAR', '2b_sWAR', '3b_sWAR', 'ss_sWAR', 'lf_sWAR', 'cf_sWAR', 'rf_sWAR', 'dh_sWAR', 'c_sWAR_pot', '1b_sWAR_pot', '2b_sWAR_pot', '3b_sWAR_pot', 'ss_sWAR_pot', 'lf_sWAR_pot', 'cf_sWAR_pot', 'rf_sWAR_pot', 'dh_sWAR_pot', 'toWAR', 'toWAR_pot', 'c_tdWAR', '1b_tdWAR', '2b_tdWAR', '3b_tdWAR', 'ss_tdWAR', 'lf_tdWAR', 'cf_tdWAR', 'rf_tdWAR', 'dh_tdWAR', 'in_list']
df = merged_df[columns]

# change 'bats' so that 1 = R, 2 = L, 3 = S
df['bats'] = df['bats'].replace({1: 'R', 2: 'L', 3: 'S'})

# Filter the DataFrame - include all of the club I manage and any player with a best_sWAR or best_sWAR_pot greater than or equal to 0.1
df = df[(df['club'] == team_managed) | (df['best_sWAR'] >= 0.1) | (df['best_sWAR_pot'] >= 0.1) | (df ['in_list'] == 'flagged')]

df = df.dropna(subset=['OPS+'])
df = df.dropna(subset=['OPS+_p'])
df = df.rename(columns={
    'best_sWAR': 'best',
    'best_sWAR_pos': 'pos',
    'c_sWAR': 'c',
    '1b_sWAR': '1b',
    '2b_sWAR': '2b',
    '3b_sWAR': '3b',
    'ss_sWAR': 'ss',
    'lf_sWAR': 'lf',
    'cf_sWAR': 'cf',
    'rf_sWAR': 'rf',
    'dh_sWAR': 'dh',
    'best_sWAR_pot': 'bestP',
    'c_sWAR_pot': 'cP',
    '1b_sWAR_pot': '1bP',
    '2b_sWAR_pot': '2bP',
    '3b_sWAR_pot': '3bP',
    'ss_sWAR_pot': 'ssP',
    'lf_sWAR_pot': 'lfP',
    'cf_sWAR_pot': 'cfP',
    'rf_sWAR_pot': 'rfP',
    'dh_sWAR_pot': 'dhP',
    'toWAR_pot': 'toWARP'
})

# Export the DataFrame to a CSV file
df.to_csv(export_filepath + '/batter_sWAR.csv', index=False)






