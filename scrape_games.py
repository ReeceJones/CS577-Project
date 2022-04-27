from datetime import date
from bs4 import BeautifulSoup
from multiprocessing import Pool
import re
import json
import argparse
from numpy import int64
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
        transcript = v['transcript']
        matches.append((teams[0], teams[1], year, transcript))
    return pd.DataFrame(matches, columns=['home_team', 'visiting_team', 'year', 'transcript'])

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

def get_manifest(query_string):
    req = requests.get(query_string)
    if req.status_code == 200:
        html = unbreak_html_text(req.text)
        parsed_html = BeautifulSoup(html, 'html.parser')
        table = parsed_html.find(id='games').find('tbody')
        table_data = parse_table(table)
        print(f'Scraped {query_string}')
        return pd.DataFrame(table_data, columns=['rk', 'week', 'day', 'date', 'outcome', 'winner', 'visit_indicator', 'loser', 'score_method', 'points_winner', 'points_loser', 'yards_winner', 'tow', 'yards_loswer', 'tol'])
    print('Error occured scraping:', req.status_code, query_string)
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

def join_matches(manifest, search_list):
    # preprocess team names
    a = manifest.copy()
    a.winner = a.winner.str.lower()
    a.loser = a.loser.str.lower()
    b = search_list.copy()[['short', 'long']]
    b.long = b.long.str.lower()

    # make team names use home/away schema
    def order_teams(x):
        home = x.loser if x.visit_indicator == '@' else x.winner
        away = x.winner if x.visit_indicator == '@' else x.loser
        date = pd.to_datetime(x.date, format='%Y-%m-%d')
        return pd.Series({'date': date, 'year': date.year, 'home_team': home, 'visiting_team': away})
    c = a.apply(order_teams, axis=1)

    # now join with pfr team search list to get search code
    d = dict()
    for idx, x in b.iterrows():
        d[x.long] = x.short

    c['home_short'] = c.home_team.replace(d)
    c['visiting_short'] = c.visiting_team.replace(d)

    return c

def get_game(t):
    # formatted_string = time.strftime(f'https://www.pro-football-reference.com/boxscores/%Y%m%d0{team}.htm', date)
    # req = pool_manager.request('GET', formatted_string)
    query_string, row = t
    req = requests.get(query_string)
    if req.status_code == 200:
        html = unbreak_html_text(req.text)
        # print(html)
        parsed_html = BeautifulSoup(html, 'html.parser')
        table = parsed_html.find('table', id='pbp')
        if table is not None:
            table_data = parse_table(table.find('tbody'))
            print(f'Scraped {query_string}')
            j = dict()
            j['pbp'] = json.loads(pd.DataFrame(table_data, columns=['quarter', 'time', 'down', 'togo', 'location', 'detail', 'away_points', 'home_points', 'epb', 'epa']).to_json(orient='records'))
            j['date'] = row.date.timetuple()
            j['teams'] = [row.home_team, row.visiting_team]
            return j
    print('Error occureced scraping:', req.status_code, row.home_team, row.visiting_team, query_string)
    return None

def scrape_matches(joined_matches):
    queries = list()
    for idx, row in joined_matches.iterrows():
        query_string = time.strftime(f'https://www.pro-football-reference.com/boxscores/%Y%m%d0{row.home_short}.htm', row.date.timetuple())
        queries.append((query_string, row))
    with Pool(SCRAPE_PROCESSES) as p:
        matches = list(p.map(get_game, queries))
        return matches
    return None

def process_game(game):
    data = list()
    pbp = game['pbp']
    stripped_pbp = list()
    # remove any extraneous penality/game status information
    for row in pbp:
        no_penalties = re.sub(r'(Penalty|Timeout).+\.?', '', row['detail']).strip()
        no_penalties = re.sub(r'\.', '', no_penalties)
        if len(no_penalties) > 0 and len(row['time'].strip()) > 0:
            # remove any unnecessary information (player names, yard quantities, etc.)
            trimmed_detail = re.sub(r'((to |by |at )?[A-Z][^ ]*)|(\(.+\))|((for )?\-?[0-9]+( yard(s)?)?)|(intended for )', '', row['detail'])
            row['trimmed_detail'] = trimmed_detail
            stripped_pbp.append(row)

    # process good text
    for i, row in enumerate(stripped_pbp):
        # look forward 1 play to add detail (down, togo, location, time)
        if i >= len(stripped_pbp)-1:
            continue
        next_row = stripped_pbp[i+1]

        trimmed_detail = row['trimmed_detail']

        # format detail
        togo_after_play = next_row['togo'].strip()
        _time_after_play = next_row['time'].strip().split(':')
        time_after_play = (' , and ' + ' '.join([_time_after_play[0] + " minutes", _time_after_play[1] + " seconds"]) + ' time remaining') if len(_time_after_play) > 1 else ''
        down_after_play = next_row['down'].strip()
        expanded_detail = f'{trimmed_detail} on down {down_after_play} with {togo_after_play} togo {time_after_play}' if len(down_after_play) > 0 else \
                            f'{trimmed_detail} with {togo_after_play} togo {time_after_play}'
        # expanded_detail = f'{trimmed_detail} {" on " + row["location"].split(" ")[1] if len(row["location"].split(" ")) > 0 else ""} with {row["togo"]} togo and {row["time"]} left'.lower()
        # clean up any unneeded whitespace
        cleaned_detail = re.sub(r'[ ]+', ' ', expanded_detail.lower().strip())
        data.append((cleaned_detail.split(' '), re.sub(r'[ ]+', ' ', stripped_pbp[i+1]['trimmed_detail'].lower().strip()).split(' ')))

    # data = list()
    # for i, row in enumerate(final_details):
    #     data.append((final_details[i], stripped_pbp[i+1]['trimmed_detail']))

    return {
        'date': game['date'],
        'teams': game['teams'],
        'data': data,
    }


def process_games(scraped_matches):
    with Pool() as p:
        processed_games = list(p.map(process_game, scraped_matches))
        return processed_games
    return None

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--scrape-manifest', action='store_true', dest='scrape_manifest')
    parser.add_argument('--scrape-games', action='store_true', dest='scrape_games')
    parser.add_argument('--process-games', action='store_true', dest='process_games')
    parser.add_argument('--scrape-processes', action='store', dest='scrape_processes', default=SCRAPE_PROCESSES, type=int)
    args = parser.parse_args()
    SCRAPE_PROCESSES = args.scrape_processes
    with open('dataset/raw_transcripts.json', 'r') as f:
        json_data = json.loads(f.read())

        manifest = None
        if args.scrape_manifest:
            manifest = get_manifests()
            manifest.to_csv('dataset/game_manifest.csv', index=False)
        else:
            manifest = pd.read_csv('dataset/game_manifest.csv')

        scraped_matches = None
        if args.scrape_games:
            search_list = pd.read_csv('dataset/teams_search_list.csv', names=['short', 'long', 'period', 'unk0', 'unk1', 'unk2', 'unk3', 'unk4', 'unk5'])
            joined_matches = join_matches(manifest, search_list)
            # print(len(joined_matches))
            scraped_matches = [x for x in scrape_matches(joined_matches) if x is not None]
            print(f'{len(scraped_matches)} games scraped')
            with open('dataset/games.json', 'w') as f:
                f.write(json.dumps(scraped_matches))

        if args.process_games:
            if scraped_matches is None:
                with open('dataset/games.json', 'r') as f:
                    scraped_matches = json.loads(f.read())
            processed_games = process_games(scraped_matches)
            with open('dataset/processed_games.json', 'w') as f:
                f.write(json.dumps(processed_games))
            

        # game_pbp = get_game('gnb', time.strptime('20141109', '%Y%m%d'))
        # game_pbp.to_csv('test_gnb.csv', index=False)

