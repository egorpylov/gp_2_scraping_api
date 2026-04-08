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