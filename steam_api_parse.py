import requests
import json
from copy import deepcopy
import pandas as pd
from dotenv import load_dotenv
import os
from loguru import logger

load_dotenv()  
logger.add(".log")






all_games_url = "https://api.steampowered.com/IStoreService/GetAppList/v1/" 

@logger.catch
def get_next_dataset(params):
  return requests.get(all_games_url, params).json()["response"]

@logger.catch
def get_full_dataset(params):
  
  last = get_next_dataset(params)
  acc = deepcopy(last["apps"])

  

  while "have_more_results" in last.keys():
    logger.success(f"Dataset successfully fetched last game id: {last['last_appid']}")

    last = get_next_dataset({**params, "last_appid": last["last_appid"]})

    acc += last["apps"]
    
  logger.success(f"Fetching ended successfully. Total lines: {len(acc)}")  
  return acc


@logger.catch
def parse_steam_api():

  params = {
     "key": os.environ["API_KEY"],
     "max_results": 50000,
     
  }

  #full_dataset = list(reduce(lambda acc, n: acc + n["apps"], takewhile_inclusive(lambda res: res["have_more_results"], iterate(lambda pr: get_next_dataset({**all_games_params, "last_appid": pr["last_appid"]}), get_next_dataset(all_games_params))), []))
  
  data = get_full_dataset(params)
  pd.DataFrame(data).to_csv("main_data.csv", index=False)

parse_steam_api()