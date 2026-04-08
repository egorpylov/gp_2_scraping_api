import pandas as pd
import os

ROOT = os.path.abspath(__file__ + '/../../data')

combined = pd.read_csv(os.path.join(ROOT, 'combined_data.csv'))
steamspy = pd.read_csv(os.path.join(ROOT, 'steamspy_data.csv'))

steamspy['appid'] = steamspy['appid'].astype(str)
combined['appid'] = combined['appid'].dropna().apply(lambda x: str(int(float(x))))
cols = ['appid', 'year', 'price_usd', 'score_rank_pct', 'userscore_rank_pct', 'metascore_rank_pct', 'owners_lower', 'owners_upper', 'playtime_median_min']
merged = combined.merge(steamspy[cols], on='appid', how='left')
out = os.path.join(ROOT, 'joined_data.csv')
merged.to_csv(out, index=False)
print(len(merged), out)
