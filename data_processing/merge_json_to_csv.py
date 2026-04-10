import csv
import json

cols = ['appid', 'name', 'type', 'required_age', 'is_free', 'release_date',
        'metacritic_score', 'metacritic_url', 'developers', 'publishers',
        'categories', 'genres', 'platforms', 'header_image', 'website',
        'short_description', 'price_currency', 'price_initial', 'price_final',
        'discount_percent', 'player_count', 'review_score', 'review_score_desc',
        'total_positive', 'total_negative', 'total_reviews', 'month_top_rank',
        'month_top_release_timestamp', 'year_top_rank', 'year_top_release_timestamp', 'year_top_type']

d1 = json.load(open('details_data2.json'))
d2 = json.load(open('current_online_data2.json'))
rev = json.load(open('reviews_data2.json'))
d4 = json.load(open('month_top_games_data.json'))
d5 = json.load(open('year_top_games_data.json'))
d6 = json.load(open('game_ids_names.json'))
names = {}
for i, a in d6.get('appid', {}).items():
    names[str(a)] = d6.get('name', {}).get(i, '')

details = {}
for p in d1:
    for a, x in p.items():
        details[a] = x

online = {}
for p in d2:
    for a, x in p.items():
        try: online[a] = x['response']['player_count']
        except: pass

reviews = {}
for p in rev:
    for a, x in p.items():
        if type(x) == dict and 'query_summary' in x:
            reviews[a] = x['query_summary']
def top(data, pre):
    r = {}
    for p in data:
        for a, e in p.items():
            try: bl = e['response'].values()
            except: continue
            for b in bl:
                if type(b) != list: continue
                for it in b:
                    if type(it) != dict or 'appid' not in it: continue
                    aid = str(it['appid'])
                    rank = it.get('app_release_rank')
                    if rank is None: continue
                    if aid not in r or rank < r[aid].get(pre + '_top_rank', 999999):
                        if aid not in r: r[aid] = {}
                        r[aid][pre + '_top_rank'] = rank
                        r[aid][pre + '_top_release_timestamp'] = it.get('rtime_release')
                        if pre == 'year':
                            r[aid]['year_top_type'] = it.get('type')
    return r
def safe_join(x):
    if not x:
        return ''
    if type(x)==list:
        r = []
        for i in x:
            if type(i)==dict:
                if 'description' in i:
                    r.append(str(i['description']))
                elif 'name' in i:
                    r.append(str(i['name']))
                elif 'id' in i:
                    r.append(str(i['id']))
                else:
                    r.append(str(i))
            else:
                r.append(str(i))
        return ' | '.join(r)
    if type(x)==dict:
        return json.dumps(x, ensure_ascii=False)
    return str(x)
m = top(d4, 'month')
y = top(d5, 'year')

all_ids = set()
for src in [details, online, reviews, m, y]:
    all_ids.update(src.keys())
ids = sorted(all_ids, key=lambda x: int(x) if x.isdigit() else 0)
f = open('combined_data.csv', 'w')
w = csv.writer(f) # https://docs.python.org/3/library/csv.html
w.writerow(cols) 

for appid in ids:
    de = details.get(appid, {})
    d = {}
    if de:
        try: d = list(de.values())[0].get('data', {})
        except: pass
    price = d.get('price_overview', {})
    meta = d.get('metacritic', {})
    rv = reviews.get(appid, {})
    mt = m.get(appid, {})
    yt = y.get(appid, {})
    plats = [k for k,v in (d.get('platforms') or {}).items() if v]
    w.writerow([
        appid,
        d.get('name') or names.get(appid, ''),
        d.get('type', ''),
        d.get('required_age', ''),
        d.get('is_free', ''),
        (d.get('release_date') or {}).get('date', ''),
        meta.get('score', ''),
        meta.get('url', ''),
        safe_join(d.get('developers')),
        safe_join(d.get('publishers')),
        safe_join(d.get('categories')),
        safe_join(d.get('genres')),
        safe_join(plats),
        d.get('header_image', ''),
        d.get('website', ''),
        d.get('short_description', ''),
        price.get('currency', ''),
        price.get('initial', ''),
        price.get('final', ''),
        price.get('discount_percent', ''),
        online.get(appid, ''),
        rv.get('review_score', ''),
        rv.get('review_score_desc', ''),
        rv.get('total_positive', ''),
        rv.get('total_negative', ''),
        rv.get('total_reviews', ''),
        mt.get('month_top_rank', ''),
        mt.get('month_top_release_timestamp', ''),
        yt.get('year_top_rank', ''),
        yt.get('year_top_release_timestamp', ''),
        yt.get('year_top_type', '')
    ])
f.close()
print('Done')