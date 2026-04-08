import pandas as pd
import os

ROOT = os.path.abspath(__file__ + '/../../data')

df = pd.read_csv(os.path.join(ROOT, 'joined_data.csv'), low_memory=False)
filtered = df[df['owners_lower'].notna()].copy()
good_cols = [
    'record_type', 'source_file', 'appid', 'name', 'type', 'required_age', 'is_free', 'categories', 'platforms', 'genres', 'header_image', 'short_description', 'developers', 'publishers', 'release_date', 'review_score', 'review_score_desc', 'total_positive', 'total_negative', 'total_reviews', 'player_count', 'price_final', 'price_initial', 'price_currency', 'discount_percent', 'owners_lower', 'owners_upper', 'price_usd', 'year', 'playtime_median_min'
]
result = filtered[good_cols]
out = os.path.join(ROOT, 'final_data.csv')
result.to_csv(out, index=False)
print(len(result), out)
