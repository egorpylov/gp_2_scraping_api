import random
import requests
import json
from copy import deepcopy
import pandas as pd
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
import config
import httpx
import asyncio
import numpy as np

logger.add(".log/{time}.log")


@logger.catch
def get_next_dataset(params):
  return requests.get(config.all_games_url, params).json()["response"]


@retry(wait=wait_exponential(max=60, min=5), stop=stop_after_attempt(3), retry=retry_if_exception_type(httpx.ReadTimeout))
async def get_with_retry(client, url):
  return await client.get(url)

@logger.catch
async def fetch_steam_api(client, urls, ids):
  data = dict()  
  await asyncio.sleep(random.uniform(0, 2))

  for i in range(len(urls)):
    try:
      response = await get_with_retry(client, urls[i])
      data[ids[i]] = response.json()
    except Exception as e:
      logger.warning(f"URL {urls[i]} failed to fetch\n Error: {e}")
    await asyncio.sleep(random.uniform(1, 3))
  logger.success(f"client {id(client)} successfully fetched {urls[0]} group")
  return data  



# urls is a list of url lists generated for each client
@logger.catch
async def fetch_steam_full(clients, urls, ids, list_params=None):

  tasks = [fetch_steam_api(clients[i], urls[i], ids[i]) for i in range(len(urls))] 
  
  data = await asyncio.gather(*tasks)

  return data


@logger.catch
async def fetch_all_pub_steam_sources(appids, urls):
  clients = list(map(lambda p: httpx.AsyncClient(proxy=p, timeout=20), config.PROXIES))
  logger.success(f"{len(clients)} clients created")
  splitted_ids = [i.astype(int).tolist() for i in np.array_split(appids, len(config.PROXIES))]

  data = list()

  for url in urls:
    
    url_list = list(map(lambda y: list(map(lambda x: url.format(appid=x), y)), splitted_ids))
    current_data = await fetch_steam_full(clients, url_list, splitted_ids)
    data.append(current_data)
    logger.success(f"Data for url: {url} successfully fetched")


  logger.success("List of all data returning...")
  return data


@logger.catch
def get_full_appids():
  
  last = get_next_dataset(config.data_private_api_params)
  acc = deepcopy(last["apps"])

  while "have_more_results" in last.keys():
    logger.success(f"Steam API dataset successfully fetched last game id: {last['last_appid']}")

    last = get_next_dataset({**config.data_private_api_params, "last_appid": last["last_appid"]})

    acc += last["apps"]
    
  logger.success(f"Fetching ended successfully. Total lines: {len(acc)}")  

  return pd.DataFrame(acc)

'''
ids = get_full_appids()

with open("game_ids_names.json", "w") as f:
  ids.to_json(f)


data_tuple = asyncio.run(fetch_all_pub_steam_sources(ids, (
  config.app_details_url,
  config.reviews_url,
  config.current_online_url
)))


with open("details_data2.json", "w") as f:
    json.dump(data_tuple[0], f)

with open("reviews_data2.json", "w") as f:
    json.dump(data_tuple[1], f)

with open("current_online_data2.json", "w") as f:
    json.dump(data_tuple[2], f)

'''
@logger.catch
async def get_all_private_steam_once(urls, params):
  clients = list(map(lambda p: httpx.AsyncClient(proxy=p, timeout=20), config.PROXIES[:len(urls) + 1]))
  tasks = [client.get(url, params=param) for client, url, param in zip(clients, urls, params)]

  data = await asyncio.gather(*tasks)
  return data

data = asyncio.run(get_all_private_steam_once(
    [i["url"] for i in config.private_urls_list.values()],
    [i["params"] for i in config.private_urls_list.values()]
))

for dataset, name in zip(data, config.private_urls_list.keys()):
    with open(f"data/{name}.json", "w") as f:
        json.dump(dataset.json()["response"], f)


'''

from datetime import datetime

@logger.catch
async def get_all_top_games_timed():

  months = []
  years = []

  
  for year in range(2003, 2027):
    years.append(datetime(year, 1, 1).timestamp())
    for month in range(1, 13):
      months.append(datetime(year, month, 1).timestamp())

  month_urls = [config.month_top_url.format(key=os.environ["API_KEY"], time=month) for month in months]
  year_urls = [config.year_top_url.format(key=os.environ["API_KEY"], time=year) for year in years]
  
  splitted_month_urls = [i.tolist() for i in np.array_split(month_urls, min(len(config.PROXIES), len(month_urls)) )]
  splitted_year_urls = [i.tolist() for i in np.array_split(year_urls, min(len(config.PROXIES), len(year_urls)))]

  month_ids = [i.astype(int).tolist() for i in np.array_split([int(month) for month in months], len(config.PROXIES))]
  year_ids = [i.astype(int).tolist() for i in np.array_split([int(year) for year in months], len(config.PROXIES))]
  
  clients = list(map(lambda p: httpx.AsyncClient(proxy=p, timeout=20), config.PROXIES))
  month_data = await fetch_steam_full(clients, splitted_month_urls, month_ids)
  logger.success("Top month game data fetched successfully fetched")
  year_data = await fetch_steam_full(clients, splitted_year_urls, year_ids)
  logger.success("Top year game data fetched successfully fetched")

  return (month_data, year_data)

time_data_tuple = asyncio.run(get_all_top_games_timed())
with open("data/month_top_games_data.json", "w") as f:
    json.dump(time_data_tuple[0], f)

with open("data/year_top_games_data.json", "w") as f:
    json.dump(time_data_tuple[1], f)

'''