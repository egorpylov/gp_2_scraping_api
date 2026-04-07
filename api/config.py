import os
from dotenv import load_dotenv
import requests

load_dotenv()  

app_details_url = "https://store.steampowered.com/api/appdetails?appids={appid}"

reviews_url = "https://store.steampowered.com/appreviews/{appid}/?json=1&num_per_page=0"


current_online_url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={appid}"


all_games_url = "https://api.steampowered.com/IStoreService/GetAppList/v1/" 

month_top_url = "https://api.steampowered.com/ISteamChartsService/GetMonthTopAppReleases/v1/?key={key}&rtime_month={time}"
year_top_url = "https://api.steampowered.com/ISteamChartsService/GetYearTopAppReleases/v1/?key={key}&rtime_year={time}"

private_urls_list = {
    "countries":    {"url": "https://api.steampowered.com/IStoreTopSellersService/GetCountryList/v1/", "params": {"key": os.environ["API_KEY"]}},
    "price_stops":  {"url": "https://api.steampowered.com/IStoreBrowseService/GetPriceStops/v1/", "params": {"key": os.environ["API_KEY"], "input_json": '{"country_code":"US","currency_code":"USD"}'}},
    "popular_tags": {"url": "https://api.steampowered.com/IStoreService/GetMostPopularTags/v1/", "params": {"key": os.environ["API_KEY"]}},
    "all_tags":     {"url": "https://api.steampowered.com/IStoreService/GetTagList/v1/", "params": {"key": os.environ["API_KEY"], "input_json": '{"language":"english"}'}},
}
   


data_private_api_params = {
     "key": os.environ["API_KEY"],
     "max_results": 50000,
  }


PROXIES = requests.get(
    "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=100",
    headers={"Authorization": os.environ["PROXY_API_KEY"]}
).json()["results"]

PROXIES = list(map(lambda x: f'http://{x["username"]}:{x["password"]}@{x["proxy_address"]}:{x["port"]}', PROXIES))
