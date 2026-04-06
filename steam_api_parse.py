import requests
import json
from copy import deepcopy
import pandas as pd
import os
from loguru import logger
import config
import httpx
import asyncio
from more_itertools import divide
import numpy as np

logger.add(".log/{time}.log")



@logger.catch
def get_next_dataset(params):
  return requests.get(config.all_games_url, params).json()["response"]


@logger.catch
async def fetch_steam_pub_api(client, urls):
  data = dict()  
  
  for url in urls:
    response = await client.get(url)
    data |= response.json()
    await asyncio.sleep(1)

  return data  



# urls is a list of url lists generated for each client
@logger.catch
async def fetch_pub_steam_full(clients, urls, list_params=None):

  tasks = [fetch_steam_pub_api(clients[i], urls.iloc[i]) for i in range(len(clients))] 
  
  data = await asyncio.gather(*tasks)

  return data


@logger.catch
async def fetch_all_pub_steam_sources(appids, urls):
  clients = list(map(lambda p: httpx.AsyncClient(proxy=p), config.PROXIES))
  splitted_ids = pd.DataFrame(np.array_split(appids, len(config.PROXIES)))

  data = list()

  for url in urls:
    
    url_list = splitted_ids.map(lambda x: config.app_details_url.format(appid=x))
    current_data = await fetch_pub_steam_full(clients, url_list)
    data.append(current_data)
    logger.success(f"Data for url: {url} successfully fetched")


  logger.success("List of all data returning...")
  return data


@logger.catch
def get_full_dataset():
  
  last = get_next_dataset(config.data_private_api_params)
  acc = deepcopy(last["apps"])

  while "have_more_results" in last.keys():
    logger.success(f"Steam API dtaset successfully fetched last game id: {last['last_appid']}")

    last = get_next_dataset({**config.data_private_api_params, "last_appid": last["last_appid"]})

    acc += last["apps"]
    
  logger.success(f"Fetching ended successfully. Total lines: {len(acc)}")  

  return pd.DataFrame(acc)


ids = get_full_dataset()["appid"].head(100)

data_tuple = asyncio.run(fetch_all_pub_steam_sources(ids, (
  config.app_details_url,
  config.reviews_url,
  config.current_online_url
)))

pd.DataFrame(data_tuple[0]).to_json("details_data.csv")
pd.DataFrame(data_tuple[1]).to_json("reviews_data.csv")
pd.DataFrame(data_tuple[2]).to_json("current_online_data.csv")
