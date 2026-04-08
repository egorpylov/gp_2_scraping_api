import csv
import json

d1 = json.load(open('details_data2.json'))
d2 = json.load(open('current_online_data2.json'))
rev = json.load(open('reviews_data2.json'))
d4 = json.load(open('month_top_games_data.json'))
d5 = json.load(open('year_top_games_data.json'))
d6 = json.load(open('game_ids_names.json'))

names = {}
for i, a in d6.get('appid', {}).items():
    names[str(a)]=d6.get('name',{}).get(i, '')

details = {}
for p in d1:
    for a,x in p.items():
        details[a]=x

online = {}
for p in d2:
    for a, x in p.items():
        try: online[a]=x['response']['player_count']
        except: pass

reviews = {}
for p in rev:
    for a, x in p.items():
        if type(x) == dict and 'query_summary' in x:
            reviews[a]=x['query_summary']