import random

import requests
import json
from copy import deepcopy
import pandas as pd
import os
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import config
import httpx
import asyncio
from more_itertools import divide
import numpy as np

logger.add(".log/{time}.log")


@logger.catch
def get_next_dataset(params):
  return requests.get(config.all_games_url, params).json()["response"]


@retry(wait=wait_exponential(max=60, min=5), stop=stop_after_attempt(3))
async def get_with_retry(client, url):
  return await client.get(url)

@logger.catch
async def fetch_steam_pub_api(client, urls, ids):
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
async def fetch_pub_steam_full(clients, urls, ids, list_params=None):

  tasks = [fetch_steam_pub_api(clients[i], urls[i], ids[i]) for i in range(len(clients))] 
  
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
    current_data = await fetch_pub_steam_full(clients, url_list, splitted_ids)
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


ids = get_full_dataset()

with open("game_ids_names.json", "w") as f:
  ids.to_json(f)

'''

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
