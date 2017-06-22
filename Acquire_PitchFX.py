# -*- coding: utf-8 -*-
"""
Created on Sun May  7 16:36:07 2017

@author: Ryan Kuhns
"""

# List of information pieces
# active-date -- day of games being queried
# Mlbs game id for each game on a given active_date
# The game number (1 for single game, 2 for second of double-header, etc)

# Note: We need a lookup table for game_type
# Note: We need a lookup table for team_id that provides looked up team name values 
# Game data should store team_id, division_id and league_id, and team w and l for the  home and away teams
import re
from bs4 import BeautifulSoup
from urllib.request import urlopen
import xml.etree.ElementTree as ET
import os
from datetime import datetime, timedelta
import sqlite3

# First we'll set up the SQL tables
os.chdir('C:/Ryan/Baseball_data/Databases')

conn = sqlite3.connect('MLB_Data.sqlite')
cur = conn.cursor()
# Eventually we won't want to drop the table if it exists,but I've got that in there to keep it small for testing
cur.execute('''Drop Table if exists Games''')
# Need to store datetimes as "YYYY-MM-DD HH:MM:SS.SSS"
cur.execute('''CREATE TABLE IF NOT EXISTS Games
    (game_id char(30) primary key, game_type char(1), game_number char(1),
     local_game_datetime datetime, game_time_et datetime, game_pk char(6),
     gameday_sw char(1))''')
cur.execute('''CREATE TABLE IF NOT EXISTS Stadiums
            (stadium_id char(2) primary key, stadium_name varchar(150),
            stadium_location varchar(150), venue_w_chan_loc char(8))''')

# The first thing we'll do is create a start and ending date to pull data
start = '2017-05-05'
end = '2017-05-06'
startdate = datetime.strptime(start, '%Y-%m-%d')
enddate = datetime.strptime(end, '%Y-%m-%d')

# This is the base url for the mlb game data website
base_url = "http://gd2.mlb.com/components/game/mlb/"

# Now we'll loop through the dates from one to the chnage plus one
delta = enddate - startdate
for i in range(delta.days+1):
    active_date = (startdate+timedelta(days=i))
    url_yr = "year_"+str(active_date.year)
    url_mt = "/month_"+active_date.strftime('%m')
    url_d = "/day_"+active_date.strftime('%d')+"/"
    day_url = base_url+url_yr+url_mt+url_d
    print(day_url)
    # We open the url if possible or print an description of the error
    try:
        mlb_site = urlopen(day_url)
    except:
        print('Unable to open the url for:', active_date)
    mlb_soup = BeautifulSoup(mlb_site, "lxml")

#   print(mlb_soup)
# for each date we want to find the games that were played
# game ids always start with gid_ and we use regular expression search to find these types of anchors
    games = mlb_soup.find_all("a", href=re.compile("gid_.*"))
    for game in games:
        g = game.get_text().strip()
        try:
        # We scrape the game number from as the last digit in the game id
            game_number = int(g[len(g)-2:len(g)-1])
        except:
        # If we encounter a problem we assume there was just 1 game (no double-header)
            game_number = 1
        # Now we augment the url to include the game id
        game_url = day_url+g
        # We should probably try/except the opening of the game url to track errors
        if BeautifulSoup(urlopen(game_url), "lxml").find("a", href="game.xml"):
            game_xml = urlopen(game_url+"game.xml")
            game_xml_tree = ET.parse(game_xml)
            game = game_xml_tree.getroot()
            # we'll add the game specific data to the game attributes dictionary
            game_data = game.attrib
            # We use g[:-3] to remove the '/' at the end of the game id (g variable)
            game_data['game_id'] = g[:-1]
            game_data['game_number'] = g[-2:-1]
            # We insert a datetime version of the game date and start time
            local_datetime = str(active_date.date())+' '+game_data['local_game_time']+':00'
            game_data['local_game_datetime'] = datetime.strptime(local_datetime, '%Y-%m-%d %H:%M:%S')
            # We also do this for the eastern time version of game time
            AM_or_PM = game_data['game_time_et'][-2:]
            game_time_et = str(active_date.date())+' '+game_data['game_time_et'][:-3]+':00'
            game_datetime_et = datetime.strptime(game_time_et, '%Y-%m-%d %H:%M:%S')
            # We adjust 12 hour time clock to 24 hour time clock
            if AM_or_PM.strip() == 'PM':
                game_datetime_et = game_datetime_et+timedelta(hours=12)
            game_data['game_datetime_et'] = game_datetime_et
            # We now want to load the game specific data to the Games SQL db
            cur.execute('''INSERT OR IGNORE INTO Games
                        (game_id, game_type, game_number, local_game_datetime,
                         game_time_et, game_pk, gameday_sw)
                        Values(:game_id,:type,:game_number,
                        :local_game_datetime,:game_datetime_et,:game_pk,
                        :gameday_sw)''', game_data)
            # We now want to load the stadium description data into the stadiums SQL db
            # This part obviously needs to be finished
            cur.execute('''INSERT OR IGNORE INTO Stadiums
                        ()
                        Values()''', )
            conn.commit()
            # Now we look at getting data on teams in the games
            # Have to finish parsing this out, but we'll parse out team info and load it to a teams SQL db
            team_data = game.findall('team')
#            for team in team_data:
#                print(team.attrib)
# Next step after finishing these will be to go inning by inning and get the data on what occurred for each pitch
    print('All done')
conn.close()
