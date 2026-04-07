import json
import os
import sys
import time
import random
import pandas as pd

from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

def setup_log(cfg):
    logger.remove()
    if not cfg.get("enabled", True):
        return
    lvl = cfg.get("level", "INFO")
    logger.add(sys.stderr, level=lvl)
    lf = cfg.get("file", ".log/steamspy_{time}.log")
    if os.path.dirname(lf):
        os.makedirs(os.path.dirname(lf), exist_ok=True)
    logger.add(lf, level=lvl, rotation=cfg.get("rotation", "50 MB"), retention=cfg.get("retention", "7 days"), encoding="utf-8")

def get_drv(hl=True):
    o = ChromeOptions()
    if hl:
        o.add_argument("--headless=new")
    o.add_argument("--no-sandbox")
    o.add_argument("--disable-dev-shm-usage")
    o.add_argument("--disable-blink-features=AutomationControlled")
    o.add_experimental_option("excludeSwitches", ["enable-automation"])
    o.add_experimental_option("useAutomationExtension", False)
    o.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    d = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=o)
    d.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    logger.info("driver ready")
    return d

def parse_own(txt):
    if not txt:
        return None, None
    txt = txt.replace(",", "").replace("&nbsp;", " ")
    nums = []
    for p in txt.split():
        if p.isdigit():
            nums.append(int(p))
    if len(nums) >= 2:
        return nums[0], nums[1]
    elif len(nums) == 1:
        return nums[0], nums[0]
    return None, None

def to_v(s):
    s = s.replace("%", "").strip()
    if not s or s.upper() == "N/A":
        return None
    if s.replace(".", "").isdigit():
        return float(s)
    return None

def parse_sc(txt):
    if not txt:
        return None, None, None
    bef = txt.strip()
    ins = ""
    if "(" in txt and ")" in txt:
        bef = txt[:txt.index("(")].strip()
        ins = txt[txt.index("(") + 1:txt.index(")")].strip()

    ov = to_v(bef)
    us, mt = None, None
    if ins:
        pp = [p.strip() for p in ins.split("/")]
        if len(pp) >= 1:
            us = to_v(pp[0])
        if len(pp) >= 2:
            mt = to_v(pp[1])
    return ov, us, mt

def to_min(s):
    if not s:
        return None
    s = s.strip()
    if ":" in s:
        pp = s.split(":")
        return int(pp[0]) * 60 + int(pp[1])
    if s.isdigit():
        return int(s)
    return None

def clean(txt):
    if not txt:
        return None
    txt = " ".join(txt.split())
    txt = txt.encode("ascii", "ignore").decode()
    return txt if txt else None

def parse_tbl(html, yr):
    soup = BeautifulSoup(html, "lxml")
    tbl = soup.find("table", {"id": "gamesbygenre"})
    if tbl is None:
        logger.warning(f"{yr}: no table")
        return []
    tbody = tbl.find("tbody")
    rows = tbody.find_all("tr") if tbody else tbl.find_all("tr")[1:]
    logger.debug(f"{yr}: {len(rows)} rows")
    res = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 8:
            continue
        a = cells[1].find("a")
        if not a:
            continue
        appid = None
        href = a.get("href", "")
        if "/app/" in href:
            appid = int(href.split("/app/")[1].split("/")[0])
        nm = clean(cells[1].get("data-order") or a.get_text())
        rdate = cells[2].get("data-order")
        pr = None
        pr_raw = cells[3].get("data-order")
        if pr_raw and pr_raw.isdigit():
            pr = round(int(pr_raw) / 100, 2)

        score_txt = cells[4].get_text()
        ov, us, mt = parse_sc(clean(score_txt))
        own_lo, own_hi = parse_own(cells[5].get_text())
        pt = to_min(cells[6].get("data-order"))
        dev = clean(cells[7].get("data-order") or cells[7].get_text())
        pub = None
        if len(cells) > 8:
            pub = clean(cells[8].get("data-order") or cells[8].get_text())
        res.append({"appid": appid, "name": nm, "year": yr, "release_date": rdate,
            "price_usd": pr, "score_rank_pct": ov, "userscore_rank_pct": us,
            "metascore_rank_pct": mt, "owners_lower": own_lo, "owners_upper": own_hi,
            "playtime_median_min": pt, "developer": dev, "publisher": pub})
    return res

def scrape_yr(drv, yr, cfg):
    url = cfg["base_url"] + str(yr)
    tmt = cfg.get("page_load_timeout", 60)
    logger.info(f"{yr}: {url}")
    try:
        drv.set_page_load_timeout(tmt)
        drv.get(url)
        WebDriverWait(drv, tmt).until(EC.presence_of_element_located((By.ID, "gamesbygenre")))
        n = random.randint(3, 6)
        for i in range(1, n + 1):
            drv.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {i / n});")
            time.sleep(random.uniform(0.3, 0.8))
        time.sleep(random.uniform(cfg.get("delay_min", 2.0), cfg.get("delay_max", 5.0)))
        raw = drv.page_source
        if not raw or len(raw) < 200:
            logger.warning(f"{yr}: page empty")
            return []
        html = raw
    except Exception as e:
        logger.error(f"{yr}: {e}")
        return []

    rr = parse_tbl(html, yr)
    logger.info(f"{yr}: {len(rr)} games")
    return rr

def dump(recs, path):
    if not recs:
        logger.warning("nothing to save")
        return pd.DataFrame()
    df = pd.DataFrame(recs)
    df.drop_duplicates(subset=["appid", "year"], inplace=True)
    df.sort_values(["year", "appid"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.to_csv(path, index=False, encoding="utf-8")
    return df

def run(config_path="config.json"):
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    setup_log(config["logging"])
    cfg = config["scraping"]
    yrs = cfg["years"]
    out = cfg.get("output_file", "steamspy_data.csv")
    logger.info(f"years: {yrs}")
    drv = get_drv(hl=cfg.get("headless", True))
    data = []
    try:
        for yr in yrs:
            data.extend(scrape_yr(drv, yr, cfg))
            if data:
                dump(data, out)
                logger.info(f"saved {len(data)} recs")
            time.sleep(random.uniform(3.0, 7.5))
    finally:
        drv.quit()
        logger.info("done")
    df = dump(data, out)
    logger.info(f"total: {len(df)} rec")
    return df
