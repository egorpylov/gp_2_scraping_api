import csv
import json

def top(data, pre):
    r = {}
    for p in data:
        for a,e in p.items():
            try: bl = e['response'].values()
            except: continue
            for b in bl:
                if type(b)!=list: continue
                for it in b:
                    if type(it)!=dict or 'appid' not in it: continue
                    aid = str(it['appid'])
                    rank = it.get('app_release_rank')
                    if rank is None: continue
                    if aid not in r or rank < r[aid].get(pre + '_top_rank', 999999):
                        if aid not in r: r[aid] = {}
                        r[aid][pre + '_top_rank'] = rank
                        r[aid][pre + '_top_release_timestamp'] = it.get('rtime_release')
                        if pre=='year':
                            r[aid]['year_top_type'] = it.get('type')
    return r

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

cols = ['appid', 'name', 'type', 'required_age', 'is_free', 'release_date',
        'metacritic_score', 'metacritic_url', 'developers', 'publishers',
        'categories', 'genres', 'platforms', 'header_image', 'website',
        'short_description', 'price_currency', 'price_initial', 'price_final',
        'discount_percent', 'player_count', 'review_score', 'review_score_desc',
        'total_positive', 'total_negative', 'total_reviews', 'month_top_rank',
        'month_top_release_timestamp', 'year_top_rank', 'year_top_release_timestamp', 'year_top_type']

m = top(d4, 'month')
y = top(d5, 'year')

all_ids = set()
for src in [details,online,reviews,m,y]:
    all_ids.update(src.keys())
ids = sorted(all_ids, key=lambda x: int(x) if x.isdigit() else 0)

f = open('combined_data.csv', 'w')
w = csv.writer(f) # https://docs.python.org/3/library/csv.html
w.writerow(cols) 