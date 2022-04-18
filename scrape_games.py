from bs4 import BeautifulSoup
from multiprocessing import Pool
import re
import json
import argparse
import requests
import time
import pandas as pd

SCRAPE_PROCESSES = 30

"""
Useful urls;
https://www.pro-football-reference.com/boxscores/{4 digit year}{2 digit month}{2 digit day}0{3 letter lowercase home team}.htm
-> 
https://www.pro-football-reference.com/boxscores/%Y%m%d0{3 letter lowercase home team}.htm

https://www.pro-football-reference.com/boxscores/game_scores_find.cgi?pts_win=20&pts_lose=17
"""

"""
Processing flow:
pull out game info, replace with 3-letter pfr code, find exact date, query pbp, join with original data
"""

def dump_games(json_data):
    matches = list()
    for k,v in json_data.items():
        teams = list(map(lambda x: re.sub('[_]', ' ', x), v['teams']))
        year = v['year']
        matches.append((teams[0], teams[1], year))
    return pd.DataFrame(matches, columns=['home_team', 'visiting_team', 'year'])

def unbreak_html_text(html_text):
    # lol
    table_start = '<!--\n\n<div class="table_container"'
    table_end = '</table>\n\n\n</div>\n-->'
    return html_text.replace(table_start, '<div class="table_container"').replace(table_end, '</table>\n\n\n</div>\n')

def parse_table(table):
    table_data = list()
    # print(table)
    for tr in table.find_all('tr'):
        # print(tr.attrs)
        if 'class' in tr.attrs and ('thead' in tr.attrs['class'] or 'divider' in tr.attrs['class']):
            continue
        # valid row
        row_data = list()
        for elem in tr.find_all(['td','th']):
            row_data.append(elem.get_text().strip())
        table_data.append(row_data)
    return table_data

def get_game(team, date):
    formatted_string = time.strftime(f'https://www.pro-football-reference.com/boxscores/%Y%m%d0{team}.htm', date)
    # req = pool_manager.request('GET', formatted_string)
    req = requests.get(formatted_string)
    if req.status_code == 200:
        html = unbreak_html_text(req.text)
        # print(html)
        parsed_html = BeautifulSoup(html, 'html.parser')
        table = parsed_html.find('table', id='pbp')
        table_data = parse_table(table.find('tbody'))
        return pd.DataFrame(table_data, columns=['quarter', 'time', 'down', 'togo', 'location', 'detail', 'away_points', 'home_points', 'epb', 'epa'])
    print('Error occureced scraping:', req.status, formatted_string)
    return None

def get_manifest(query_string):
    req = requests.get(query_string)
    if req.status_code == 200:
        html = unbreak_html_text(req.text)
        parsed_html = BeautifulSoup(html, 'html.parser')
        table = parsed_html.find(id='games').find('tbody')
        table_data = parse_table(table)
        print(f'Scraped {query_string}')
        return pd.DataFrame(table_data, columns=['rk', 'week', 'day', 'date', 'unk0', 'winner', 'unk1', 'loser', 'unk2', 'points_winner', 'points_loser', 'yards_winner', 'tow', 'yards_loswer', 'tol'])
    print('Error occured scraping:', req.status, query_string)
    return None

def get_manifests():
    unique_scores = pd.read_csv('dataset/unique_scores.csv')
    queries = list()
    for score in unique_scores.Score:
        team_scores = score.strip().split('-')
        query_string = f'https://www.pro-football-reference.com/boxscores/game_scores_find.cgi?pts_win={team_scores[0]}&pts_lose={team_scores[1]}'
        queries.append(query_string)

    with Pool(SCRAPE_PROCESSES) as p:
        tables = list(p.map(get_manifest, queries))
        return pd.concat(tables)
    return None
        

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dump-games', action='store_true', dest='dump_games')
    parser.add_argument('--scrape-manifest', action='store_true', dest='scrape_manifest')
    parser.add_argument('--scrape-processes', action='store', dest='scrape_processes', default=30)
    args = parser.parse_args()
    SCRAPE_PROCESSES = args.scrape_processes
    with open('dataset/raw_transcripts.json', 'r') as f:
        json_data = json.loads(f.read())
        matches = dump_games(json_data)
        if args.dump_games:
            matches.to_csv('dataset/matches.csv', index=False)

        if args.scrape_manifest:
            manifest = get_manifests()
            manifest.to_csv('dataset/game_manifest.csv', index=False)

        # game_pbp = get_game('gnb', time.strptime('20141109', '%Y%m%d'))
        # game_pbp.to_csv('test_gnb.csv', index=False)

