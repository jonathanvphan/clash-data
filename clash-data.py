from riotwatcher import LolWatcher, ApiError
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
import gspread
import time
import datetime

# functioned it since it's repetitive but fills out the numerical stats of an individual player per game
def update_laner_stats(lanetype, lanename, column, dfrow):
    df_sheet3.iat[sheet_row, column] = my_team.loc[my_team[lanetype] == lanename].values[dfrow][4]
    df_sheet3.iat[sheet_row, column+1] = my_team.loc[my_team[lanetype] == lanename].values[dfrow][5]
    df_sheet3.iat[sheet_row, column+2] = my_team.loc[my_team[lanetype] == lanename].values[dfrow][6]
    df_sheet3.iat[sheet_row, column+4] = my_team.loc[my_team[lanetype] == lanename].values[dfrow][7] + my_team.loc[my_team[lanetype] == lanename].values[0][15]
    df_sheet3.iat[sheet_row, column+5] = my_team.loc[my_team[lanetype] == lanename].values[dfrow][8]
    df_sheet3.iat[sheet_row, column+7] = my_team.loc[my_team[lanetype] == lanename].values[dfrow][9]
    df_sheet3.iat[sheet_row, column+9] = my_team.loc[my_team[lanetype] == lanename].values[dfrow][10]
    df_sheet3.iat[sheet_row, column+10] = my_team.loc[my_team[lanetype] == lanename].values[dfrow][11]

# sets up the api for Riot
api_key = 'XXXXX-XXXXX-XXXXX-XXXXX-XXXXX-XXXXX'
watcher = LolWatcher(api_key)
my_region = 'na1'
summoner = 'Wombätt'

# the game that you want this to start recording at, 0 being the most recent
game = 0
# offset either 15 for Lethal Logic or -2 for Lethal PogChamps
offset = -2

# dataframe display in the output window options
pd.set_option("display.max_columns", 100)
pd.options.display.width = 175

# sets up the google spreadsheet
gc = gspread.service_account()
sh = gc.open("Lethal PogChamps Clash Records and Statistics")
sheet1 = sh.get_worksheet(0)
sheet2 = sh.get_worksheet(1)
sheet3 = sh.get_worksheet(2)
sheet4 = sh.get_worksheet(3)

# check league's latest version
latest = watcher.data_dragon.versions_for_region(my_region)['n']['champion']

# champions static information
static_champ_list = watcher.data_dragon.champions(latest, False, 'en_US')

# grabs the summoner's match history
me = watcher.summoner.by_name(my_region, summoner)

# grabs the matches specifically clash
my_matches = watcher.match.matchlist_by_account(my_region, me['accountId'], 700)

# gets the spreadsheets as a dataframe
df_sheet1 = get_as_dataframe(sheet1).astype(str)
df_sheet3 = get_as_dataframe(sheet3).astype(str)
print(df_sheet1)
print(df_sheet3)

# input used for doing matches 1 by 1
#game = int(input('Input the game to extract data from (0 being the most recent, 1 second most recent, etc., -1 to end loop): '))

# loops to go through each match, the number determines how many matches it will go back
while(game <= 0):
    # determines which row of the sheet to operate on based on the game number, + 14 for Lethal Logic and - 2 for Lethal PogChamps
    sheet_row = sheet1.row_count - game + offset
    sheet_row_actual = sheet1.row_count - game

    # fetches last match details and timeline
    clash_match = my_matches['matches'][game]
    while True:
        try:
            match_detail = watcher.match.by_id(my_region, clash_match['gameId'])
            match_timeline = watcher.match.timeline_by_match(my_region, match_detail['gameId'])
            break
        except:
            print('Failed to get game data, retrying...')

    # creates a dataframe from each player in the game and organizes their stats into columns, then appends the row
    participants = []
    for row in match_detail['participants']:
        participants_row = {}
        participants_row['participantId'] = row['participantId']
        participants_row['teamId'] = row['teamId']
        participants_row['champion'] = row['championId']
        participants_row['win'] = row['stats']['win']
        participants_row['kills'] = row['stats']['kills']
        participants_row['deaths'] = row['stats']['deaths']
        participants_row['assists'] = row['stats']['assists']
        participants_row['totalMinionsKilled'] = row['stats']['totalMinionsKilled']
        participants_row['goldEarned'] = row['stats']['goldEarned']
        participants_row['totalDamageDealtToChampions'] = row['stats']['totalDamageDealtToChampions']
        participants_row['visionScore'] = row['stats']['visionScore']
        participants_row['CCScore'] = row['stats']['timeCCingOthers']
        participants_row['largestKillingSpree'] = row['stats']['largestKillingSpree']
        participants_row['role'] = row['timeline']['role']
        participants_row['lane'] = row['timeline']['lane']
        participants_row['neutralMinionsKilled'] = row['stats']['neutralMinionsKilled']
        participants.append(participants_row)

    # champ static list data to dict for looking up champions and assigning them to their champ id then adds the column to the dataframe
    champ_dict = {}
    for key in static_champ_list['data']:
        row = static_champ_list['data'][key]
        champ_dict[row['key']] = row['id']
    for row in participants:
        row['championName'] = champ_dict[str(row['champion'])]

    # creates the dataframe
    df1 = pd.DataFrame(participants)

    # grabs the summoner names and adds them to the dataframe
    column = []
    for row in match_detail['participantIdentities']:
        participants_column = row['player']['summonerName']
        column.append(participants_column)

    df1['summonerName'] = column

    #print(df1)

    # specifies which team belongs to the summoner
    team = df1.loc[df1['summonerName'] == summoner]['teamId']
    
    # grabs stats for the teams and makes a dataframe
    team_details = []

    for row in match_detail['teams']:
        teams_row = {}
        teams_row['teamId'] = row['teamId']
        teams_row['win'] = row['win']
        teams_row['firstBlood'] = row['firstBlood']
        teams_row['firstTower'] = row['firstTower']
        teams_row['firstDragon'] = row['firstDragon']
        teams_row['firstHerald'] = row['firstRiftHerald']
        teams_row['firstInhibitor'] = row['firstInhibitor']
        teams_row['firstBaron'] = row['firstBaron']
        teams_row['towerKills'] = row['towerKills']
        teams_row['inhibitorKills'] = row['inhibitorKills']
        teams_row['dragonKills'] = row['dragonKills']
        teams_row['riftHeraldKills'] = row['riftHeraldKills']
        teams_row['baronKills'] = row['baronKills']
        team_details.append(teams_row)

    # creates the dataframe
    df2 = pd.DataFrame(team_details)
    
    # separates the participants stats dataframe into the two different teams
    my_team = df1.loc[df1['teamId'] == team.values[0]]
    other_team = df1.loc[df1['teamId'] != team.values[0]]
    print(my_team)
    print(other_team)

    # separates the per team stats dataframe into the two different teams
    my_team_overall = df2.loc[df2['teamId'] == team.values[0]]
    other_team_overall = df2.loc[df2['teamId'] != team.values[0]]

    print(my_team_overall)
    print(other_team_overall)

    # finds the elements of the dragons killed
    dragons = []
    for rows in match_timeline['frames']:
        for rower in rows['events']:
            dragons_row = {}
            if rower['type'] == 'ELITE_MONSTER_KILL' and rower['monsterType'] == 'DRAGON':
                dragons_row['dragon'] = rower['monsterSubType']
                dragons_row['killer'] = rower['killerId']
                dragons.append(dragons_row)

    df_dragons = pd.DataFrame(dragons)

    # Date and Link
    df_sheet1.at[sheet_row, 'Date'] = "=HYPERLINK(\"https://matchhistory.na.leagueoflegends.com/en/#match-details/NA1/" + str(match_detail['gameId']) + "/43638158?tab=overview\", \"" + datetime.datetime.fromtimestamp(match_detail['gameCreation']/1000).strftime('%m/%d/%Y') + "\")" 

    # Win
    if my_team_overall.iloc[0]['win'] == 'Win':
        df_sheet1.at[sheet_row, 'Result'] = 'Victory'
    elif other_team_overall.iloc[0]['win'] == 'Win':
        df_sheet1.at[sheet_row, 'Result'] = 'Defeat'

    # Team Gold
    df_sheet1.at[sheet_row, 'Team Gold'] = int(my_team['goldEarned'].sum())/1000

    # Enemy Gold
    df_sheet1.at[sheet_row, 'Enemy Gold'] = int(other_team['goldEarned'].sum())/1000

    # Side
    if my_team_overall.iloc[0]['teamId'] == 100:
        df_sheet1.at[sheet_row, 'Side'] = 'Blue'
    else:
        df_sheet1.at[sheet_row, 'Side'] = 'Red'

    # Game Length
    df_sheet1.at[sheet_row, 'Game Length'] = int(match_detail['gameDuration'])/(24*60*60)

    # First Blood
    if my_team_overall['firstBlood'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Blood'] = 'Ally'
    elif other_team_overall['firstBlood'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Blood'] = 'Enemy'
    else:
        df_sheet1.at[sheet_row, 'First Blood'] = 'Neither'

    # First Tower
    if my_team_overall['firstTower'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Tower'] = 'Ally'
    elif other_team_overall['firstTower'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Tower'] = 'Enemy'
    else:
        df_sheet1.at[sheet_row, 'First Tower'] = 'Neither'

    # First Dragon
    if my_team_overall['firstDragon'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Dragon'] = 'Ally'
    elif other_team_overall['firstDragon'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Dragon'] = 'Enemy'
    else:
        df_sheet1.at[sheet_row, 'First Dragon'] = 'Neither'

    # First Herald
    if my_team_overall['firstHerald'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Herald'] = 'Ally'
    elif other_team_overall['firstHerald'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Herald'] = 'Enemy'
    else:
        df_sheet1.at[sheet_row, 'First Herald'] = 'Neither'

    # First Inhibitor
    if my_team_overall['firstInhibitor'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Inhibitor'] = 'Ally'
    elif other_team_overall['firstInhibitor'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Inhibitor'] = 'Enemy'
    else:
        df_sheet1.at[sheet_row, 'First Inhibitor'] = 'Neither'

    # First Baron
    if my_team_overall['firstBaron'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Baron'] = 'Ally'
    elif other_team_overall['firstBaron'].bool() == 1:
        df_sheet1.at[sheet_row, 'First Baron'] = 'Enemy'
    else:
        df_sheet1.at[sheet_row, 'First Baron'] = 'Neither'

    # Top
    if len(my_team.loc[my_team['lane'] == 'TOP'].index) == 1:
        df_sheet1.at[sheet_row, 'Top'] = my_team.loc[my_team['lane'] == 'TOP'].values[0][17]
        df_sheet1.at[sheet_row, 'Champion 1'] = my_team.loc[my_team['lane'] == 'TOP'].values[0][16]
    else:
        print('**********************************************Missing Ally TOP!**********************************************')
        print(my_team.loc[my_team['lane'] == 'TOP'])
    
    if len(other_team.loc[other_team['lane'] == 'TOP'].index) == 1:
        df_sheet1.at[sheet_row, 'Champion Against 1'] = other_team.loc[other_team['lane'] == 'TOP'].values[0][16]
    else:
        print('**********************************************Missing Enemy TOP!**********************************************')
        print(other_team.loc[other_team['lane'] == 'TOP'])

    # extra statements in case the lanes or roles are incorrect in the data
    if len(my_team.loc[my_team['lane'] == 'TOP'].index) == 1:
        update_laner_stats('lane', 'TOP', 6, 0)
    elif len(my_team.loc[my_team['lane'] == 'TOP'].index) == 2 and len(my_team.loc[my_team['lane'] == 'MIDDLE'].index) == 0:
        update_laner_stats('lane', 'TOP', 6, 0)
        update_laner_stats('lane', 'TOP', 34, 1)
    elif len(my_team.loc[my_team['lane'] == 'TOP'].index) == 2 and len(my_team.loc[my_team['lane'] == 'NONE'].index) == 0:
        update_laner_stats('lane', 'TOP', 6, 0)
        update_laner_stats('lane', 'TOP', 20, 1)


    # Jungle
    if len(my_team.loc[my_team['lane'] == 'JUNGLE'].index) == 1:
        df_sheet1.at[sheet_row, 'Jungle'] = my_team.loc[my_team['lane'] == 'JUNGLE'].values[0][17]
        df_sheet1.at[sheet_row, 'Champion 2'] = my_team.loc[my_team['lane'] == 'JUNGLE'].values[0][16]
    else:
        print('**********************************************Missing Ally JUNGLE!**********************************************')
        print(my_team.loc[my_team['lane'] == 'JUNGLE'])
    
    if len(other_team.loc[other_team['lane'] == 'JUNGLE'].index) == 1:
        df_sheet1.at[sheet_row, 'Champion Against 2'] = other_team.loc[other_team['lane'] == 'JUNGLE'].values[0][16]
    else:
        print('**********************************************Missing Enemy JUNGLE!**********************************************')
        print(other_team.loc[other_team['lane'] == 'JUNGLE'])

    # extra statements in case the lanes or roles are incorrect in the data
    if len(my_team.loc[my_team['lane'] == 'JUNGLE'].index) == 1:
        update_laner_stats('lane', 'JUNGLE', 20, 0)
    elif len(my_team.loc[my_team['lane'] == 'JUNGLE'].index) == 2 and len(my_team.loc[my_team['lane'] == 'TOP'].index) == 0:
        update_laner_stats('lane', 'JUNGLE', 6, 0)
        update_laner_stats('lane', 'JUNGLE', 20, 1)
    elif len(my_team.loc[my_team['role'] == 'NONE'].index) == 2 and len(my_team.loc[my_team['role'] == 'DUO_SUPPORT'].index) == 0:
        update_laner_stats('role', 'NONE', 20, 0)
        update_laner_stats('lane', 'BOTTOM', 48, 0)
        update_laner_stats('role', 'NONE', 62, 1)

    # Mid
    if len(my_team.loc[my_team['lane'] == 'MIDDLE'].index) == 1:
        df_sheet1.at[sheet_row, 'Middle'] = my_team.loc[my_team['lane'] == 'MIDDLE'].values[0][17]
        df_sheet1.at[sheet_row, 'Champion 3'] = my_team.loc[my_team['lane'] == 'MIDDLE'].values[0][16]
    else:
        print('**********************************************Missing Ally MIDDLE!**********************************************')
        print(my_team.loc[my_team['lane'] == 'MIDDLE'])

    if len(other_team.loc[other_team['lane'] == 'MIDDLE'].index) == 1:
        df_sheet1.at[sheet_row, 'Champion Against 3'] = other_team.loc[other_team['lane'] == 'MIDDLE'].values[0][16]
    else:
        print('**********************************************Missing Enemy MIDDLE!**********************************************')
        print(other_team.loc[other_team['lane'] == 'MIDDLE'])

    # extra statements in case the lanes or roles are incorrect in the data
    if len(my_team.loc[my_team['lane'] == 'MIDDLE'].index) == 1:
        update_laner_stats('lane', 'MIDDLE', 34, 0)
    elif len(my_team.loc[my_team['lane'] == 'MIDDLE'].index) == 2 and len(my_team.loc[my_team['lane'] == 'TOP'].index) == 0:
        update_laner_stats('lane', 'MIDDLE', 6, 0)
        update_laner_stats('lane', 'MIDDLE', 34, 1)

    # Bottom
    if len(my_team.loc[my_team['role'] == 'DUO_CARRY'].index) == 1:
        df_sheet1.at[sheet_row, 'Bottom'] = my_team.loc[my_team['role'] == 'DUO_CARRY'].values[0][17]
        df_sheet1.at[sheet_row, 'Champion 4'] = my_team.loc[my_team['role'] == 'DUO_CARRY'].values[0][16]
    else:
        print('**********************************************Missing Ally BOTTOM!**********************************************')
        print(my_team.loc[my_team['role'] == 'DUO_CARRY'])

    if len(other_team.loc[other_team['role'] == 'DUO_CARRY'].index) == 1:
        df_sheet1.at[sheet_row, 'Champion Against 4'] = other_team.loc[other_team['role'] == 'DUO_CARRY'].values[0][16]
    else:
        print('**********************************************Missing Enemy BOTTOM!**********************************************')
        print(other_team.loc[other_team['lane'] == 'BOTTOM'])

    # extra statements in case the lanes or roles are incorrect in the data
    if len(my_team.loc[my_team['role'] == 'DUO_CARRY'].index) == 1:
        update_laner_stats('role', 'DUO_CARRY', 48, 0)
    elif len(my_team.loc[my_team['lane'] == 'BOTTOM'].index) == 2:
        update_laner_stats('lane', 'BOTTOM', 48, 0)
        update_laner_stats('lane', 'BOTTOM', 62, 1)

    # Support
    if len(my_team.loc[my_team['role'] == 'DUO_SUPPORT'].index) == 1:
        df_sheet1.at[sheet_row, 'Support'] = my_team.loc[my_team['role'] == 'DUO_SUPPORT'].values[0][17]
        df_sheet1.at[sheet_row, 'Champion 5'] = my_team.loc[my_team['role'] == 'DUO_SUPPORT'].values[0][16]
    else:
        print('**********************************************Missing Ally SUPPORT!**********************************************')
        print(my_team.loc[my_team['role'] == 'DUO_SUPPORT'])

    if len(other_team.loc[other_team['role'] == 'DUO_SUPPORT'].index) == 1:
        df_sheet1.at[sheet_row, 'Champion Against 5'] = other_team.loc[other_team['role'] == 'DUO_SUPPORT'].values[0][16]
    else:
        print('**********************************************Missing Enemy SUPPORT!**********************************************')
        print(other_team.loc[other_team['lane'] == 'BOTTOM'])

    # extra statements in case the lanes or roles are incorrect in the data
    if len(my_team.loc[my_team['role'] == 'DUO_SUPPORT'].index) == 1:
        update_laner_stats('role', 'DUO_SUPPORT', 62, 0)
    elif len(my_team.loc[my_team['lane'] == 'BOTTOM'].index) == 2:
        update_laner_stats('lane', 'BOTTOM', 48, 0)
        update_laner_stats('lane', 'BOTTOM', 62, 1)
    elif len(my_team.loc[my_team['role'] == 'DUO_SUPPORT'].index) == 2 and len(my_team.loc[my_team['lane'] == 'JUNGLE'].index) == 0:
        update_laner_stats('lane', 'BOTTOM', 20, 0)
        update_laner_stats('lane', 'BOTTOM', 62, 1)

    # if broke, extra statements in case the lanes or roles are incorrect in the data
    if len(my_team.loc[my_team['lane'] == 'NONE'].index) == 5:
        update_laner_stats('lane', 'NONE', 6, 0)
        update_laner_stats('lane', 'NONE', 20, 1)
        update_laner_stats('lane', 'NONE', 34, 2)
        update_laner_stats('lane', 'NONE', 48, 3)
        update_laner_stats('lane', 'NONE', 62, 4)
        df_sheet1.at[sheet_row, 'Top'] = my_team.loc[my_team['lane'] == 'NONE'].values[0][17]
        df_sheet1.at[sheet_row, 'Champion 1'] = my_team.loc[my_team['lane'] == 'NONE'].values[0][16]
        df_sheet1.at[sheet_row, 'Jungle'] = my_team.loc[my_team['lane'] == 'NONE'].values[1][17]
        df_sheet1.at[sheet_row, 'Champion 2'] = my_team.loc[my_team['lane'] == 'NONE'].values[1][16]
        df_sheet1.at[sheet_row, 'Middle'] = my_team.loc[my_team['lane'] == 'NONE'].values[2][17]
        df_sheet1.at[sheet_row, 'Champion 3'] = my_team.loc[my_team['lane'] == 'NONE'].values[2][16]
        df_sheet1.at[sheet_row, 'Bottom'] = my_team.loc[my_team['lane'] == 'NONE'].values[3][17]
        df_sheet1.at[sheet_row, 'Champion 4'] = my_team.loc[my_team['lane'] == 'NONE'].values[3][16]
        df_sheet1.at[sheet_row, 'Support'] = my_team.loc[my_team['lane'] == 'NONE'].values[4][17]
        df_sheet1.at[sheet_row, 'Champion 5'] = my_team.loc[my_team['lane'] == 'NONE'].values[4][16]

    if len(other_team.loc[other_team['lane'] == 'NONE'].index) == 5:
        df_sheet1.at[sheet_row, 'Champion Against 1'] = other_team.loc[other_team['lane'] == 'NONE'].values[0][16]
        df_sheet1.at[sheet_row, 'Champion Against 2'] = other_team.loc[other_team['lane'] == 'NONE'].values[1][16]
        df_sheet1.at[sheet_row, 'Champion Against 3'] = other_team.loc[other_team['lane'] == 'NONE'].values[2][16]
        df_sheet1.at[sheet_row, 'Champion Against 4'] = other_team.loc[other_team['lane'] == 'NONE'].values[3][16]
        df_sheet1.at[sheet_row, 'Champion Against 5'] = other_team.loc[other_team['lane'] == 'NONE'].values[4][16]

       
    # Towers
    df_sheet1.at[sheet_row, 'Towers Destroyed'] = int(my_team_overall.iloc[0]['towerKills'])
    df_sheet1.at[sheet_row, 'Towers Lost'] = int(other_team_overall.iloc[0]['towerKills'])

    # Inhibs
    df_sheet1.at[sheet_row, 'Inhibitors Destroyed'] = int(my_team_overall.iloc[0]['inhibitorKills'])
    df_sheet1.at[sheet_row, 'Inhibitors Lost'] = int(other_team_overall.iloc[0]['inhibitorKills'])

    # Dragons
    df_sheet1.at[sheet_row, 'Dragons Slain'] = int(my_team_overall.iloc[0]['dragonKills'])
    df_sheet1.at[sheet_row, 'Dragons Lost'] = int(other_team_overall.iloc[0]['dragonKills'])

    if game != 39:
        df_sheet1.at[sheet_row, 'Ocean'] = int(df_dragons.loc[(df_dragons['killer'] >= my_team['participantId'].min()) & (df_dragons['killer'] <= my_team['participantId'].max()) & (df_dragons['dragon'] == 'WATER_DRAGON'), 'dragon'].count())
        df_sheet1.at[sheet_row, 'Mountain'] = int(df_dragons.loc[(df_dragons['killer'] >= my_team['participantId'].min()) & (df_dragons['killer'] <= my_team['participantId'].max()) & (df_dragons['dragon'] == 'EARTH_DRAGON'), 'dragon'].count())
        df_sheet1.at[sheet_row, 'Cloud'] = int(df_dragons.loc[(df_dragons['killer'] >= my_team['participantId'].min()) & (df_dragons['killer'] <= my_team['participantId'].max()) & (df_dragons['dragon'] == 'AIR_DRAGON'), 'dragon'].count())
        df_sheet1.at[sheet_row, 'Infernal'] = int(df_dragons.loc[(df_dragons['killer'] >= my_team['participantId'].min()) & (df_dragons['killer'] <= my_team['participantId'].max()) & (df_dragons['dragon'] == 'FIRE_DRAGON'), 'dragon'].count())
        df_sheet1.at[sheet_row, 'Elder'] = int(df_dragons.loc[(df_dragons['killer'] >= my_team['participantId'].min()) & (df_dragons['killer'] <= my_team['participantId'].max()) & (df_dragons['dragon'] == 'ELDER_DRAGON'), 'dragon'].count())
     
    # Rift Heralds
    df_sheet1.at[sheet_row, 'Heralds Slain'] = int(my_team_overall.iloc[0]['riftHeraldKills'])
    df_sheet1.at[sheet_row, 'Heralds Lost'] = int(other_team_overall.iloc[0]['riftHeraldKills'])

    # Barons
    df_sheet1.at[sheet_row, 'Barons Slain'] = int(my_team_overall.iloc[0]['baronKills'])
    df_sheet1.at[sheet_row, 'Barons Lost'] = int(other_team_overall.iloc[0]['baronKills'])

    # General equations
    df_sheet1.at[sheet_row, 'Kills'] = "=SUM('Individual Scores'!G" + str(sheet_row_actual) + " + 'Individual Scores'!U" + str(sheet_row_actual) + " + 'Individual Scores'!AI" + str(sheet_row_actual) + " + 'Individual Scores'!AW" + str(sheet_row_actual) + " + 'Individual Scores'!BK" + str(sheet_row_actual) + ")"
    df_sheet1.at[sheet_row, 'Deaths'] = "=SUM('Individual Scores'!H" + str(sheet_row_actual) + " + 'Individual Scores'!V" + str(sheet_row_actual) + " + 'Individual Scores'!AJ" + str(sheet_row_actual) + " + 'Individual Scores'!AX" + str(sheet_row_actual) + " + 'Individual Scores'!BL" + str(sheet_row_actual) + ")"
    df_sheet1.at[sheet_row, 'Assists'] = "=SUM('Individual Scores'!I" + str(sheet_row_actual) + " + 'Individual Scores'!W" + str(sheet_row_actual) + " + 'Individual Scores'!AK" + str(sheet_row_actual) + " + 'Individual Scores'!AY" + str(sheet_row_actual) + " + 'Individual Scores'!BM" + str(sheet_row_actual) + ")"
    df_sheet1.at[sheet_row, 'Gold Difference'] = '=U' + str(sheet_row_actual) + '-V' + str(sheet_row_actual)
    df_sheet1.at[sheet_row, 'Soul'] = '=IF(SUM(AP' + str(sheet_row_actual) + ':AS' + str(sheet_row_actual)  + ') = 4, IFS(AP' + str(sheet_row_actual) + '> 1, "Cloud", AQ' + str(sheet_row_actual) + '> 1, "Infernal", AR' + str(sheet_row_actual) + '> 1, "Mountain", AS' + str(sheet_row_actual) + '> 1, "Ocean"), "None")'
    
    df_sheet3.at[sheet_row, 'Date'] = "='Match History and Averages'!A" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Result'] = "='Match History and Averages'!B" + str(sheet_row_actual)
    
    df_sheet3.at[sheet_row, 'Top'] = "='Match History and Averages'!C" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Champion'] = "='Match History and Averages'!D" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Champion Against'] = "='Match History and Averages'!E" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'KDA'] = '=IFERROR(ROUNDUP(((G' + str(sheet_row_actual) + '+I' + str(sheet_row_actual) + ')/H' + str(sheet_row_actual) + '),2), "Perfect")'
    df_sheet3.at[sheet_row, 'G%'] = '=L' + str(sheet_row_actual) + '/(L' + str(sheet_row_actual) + '+Z' + str(sheet_row_actual) + '+AN' + str(sheet_row_actual) + '+BB' + str(sheet_row_actual) + '+BP' + str(sheet_row_actual) + ')'
    df_sheet3.at[sheet_row, 'D%'] = '=N' + str(sheet_row_actual) + '/(N' + str(sheet_row_actual) + '+AB' + str(sheet_row_actual) + '+AP' + str(sheet_row_actual) + '+BD' + str(sheet_row_actual) + '+BR' + str(sheet_row_actual) + ')'
    
    df_sheet3.at[sheet_row, 'Jungle'] = "='Match History and Averages'!F" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Champion.1'] = "='Match History and Averages'!G" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Champion Against.1'] = "='Match History and Averages'!H" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'KDA.1'] = '=IFERROR(ROUNDUP(((U' + str(sheet_row_actual) + '+W' + str(sheet_row_actual) + ')/V' + str(sheet_row_actual) + '),2), "Perfect")'
    df_sheet3.at[sheet_row, 'G%.1'] = '=Z' + str(sheet_row_actual) + '/(L' + str(sheet_row_actual) + '+Z' + str(sheet_row_actual) + '+AN' + str(sheet_row_actual) + '+BB' + str(sheet_row_actual) + '+BP' + str(sheet_row_actual) + ')'
    df_sheet3.at[sheet_row, 'D%.1'] = '=AB' + str(sheet_row_actual) + '/(N' + str(sheet_row_actual) + '+AB' + str(sheet_row_actual) + '+AP' + str(sheet_row_actual) + '+BD' + str(sheet_row_actual) + '+BR' + str(sheet_row_actual) + ')'

    df_sheet3.at[sheet_row, 'Middle'] = "='Match History and Averages'!I" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Champion.2'] = "='Match History and Averages'!J" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Champion Against.2'] = "='Match History and Averages'!K" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'KDA.2'] = '=IFERROR(ROUNDUP(((AI' + str(sheet_row_actual) + '+AK' + str(sheet_row_actual) + ')/AG' + str(sheet_row_actual) + '),2), "Perfect")'
    df_sheet3.at[sheet_row, 'G%.2'] = '=AN' + str(sheet_row_actual) + '/(L' + str(sheet_row_actual) + '+Z' + str(sheet_row_actual) + '+AN' + str(sheet_row_actual) + '+BB' + str(sheet_row_actual) + '+BP' + str(sheet_row_actual) + ')'
    df_sheet3.at[sheet_row, 'D%.2'] = '=AP' + str(sheet_row_actual) + '/(N' + str(sheet_row_actual) + '+AB' + str(sheet_row_actual) + '+AP' + str(sheet_row_actual) + '+BD' + str(sheet_row_actual) + '+BR' + str(sheet_row_actual) + ')'

    df_sheet3.at[sheet_row, 'Bottom'] = "='Match History and Averages'!L" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Champion.3'] = "='Match History and Averages'!M" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Champion Against.3'] = "='Match History and Averages'!N" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'KDA.3'] = '=IFERROR(ROUNDUP(((AW' + str(sheet_row_actual) + '+AY' + str(sheet_row_actual) + ')/AX' + str(sheet_row_actual) + '),2), "Perfect")'
    df_sheet3.at[sheet_row, 'G%.3'] = '=BB' + str(sheet_row_actual) + '/(L' + str(sheet_row_actual) + '+Z' + str(sheet_row_actual) + '+AN' + str(sheet_row_actual) + '+BB' + str(sheet_row_actual) + '+BP' + str(sheet_row_actual) + ')'
    df_sheet3.at[sheet_row, 'D%.3'] = '=BD' + str(sheet_row_actual) + '/(N' + str(sheet_row_actual) + '+AB' + str(sheet_row_actual) + '+AP' + str(sheet_row_actual) + '+BD' + str(sheet_row_actual) + '+BR' + str(sheet_row_actual) + ')'

    df_sheet3.at[sheet_row, 'Support'] = "='Match History and Averages'!O" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Champion.4'] = "='Match History and Averages'!P" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'Champion Against.4'] = "='Match History and Averages'!Q" + str(sheet_row_actual)
    df_sheet3.at[sheet_row, 'KDA.4'] = '=IFERROR(ROUNDUP(((BK' + str(sheet_row_actual) + '+BM' + str(sheet_row_actual) + ')/BL' + str(sheet_row_actual) + '),2), "Perfect")'
    df_sheet3.at[sheet_row, 'G%.4'] = '=BP' + str(sheet_row_actual) + '/(L' + str(sheet_row_actual) + '+Z' + str(sheet_row_actual) + '+AN' + str(sheet_row_actual) + '+BB' + str(sheet_row_actual) + '+BP' + str(sheet_row_actual) + ')'
    df_sheet3.at[sheet_row, 'D%.4'] = '=BR' + str(sheet_row_actual) + '/(N' + str(sheet_row_actual) + '+AB' + str(sheet_row_actual) + '+AP' + str(sheet_row_actual) + '+BD' + str(sheet_row_actual) + '+BR' + str(sheet_row_actual) + ')'

    print('Last game inputted: ' + str(game))

    game += 1
    # skipping some clashes
    if game == 74:
        game = 76
        offset = 16

    #game = int(input('Input the game to extract data from (0 being the most recent, 1 second most recent, etc., -1 to end loop): '))
  
# replaces all the 'nan's in the dataframe to blank spaces
df_sheet1 = df_sheet1.replace('nan', '')
df_sheet3 = df_sheet3.replace('nan', '')

#print(df_sheet1)
#print(df_sheet3)
# updates the google spreadsheet

while True:
    try:
        set_with_dataframe(sheet1, df_sheet1)
        set_with_dataframe(sheet3, df_sheet3)
        break
    except:
        print('Quota exceeded, waiting 30 seconds...')
        time.sleep(30)