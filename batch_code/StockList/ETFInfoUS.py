import requests
import pandas as pd
import re
from datetime import datetime
import time
from pymongo import MongoClient


# ------------------------------------------------------------
# 1. 미국 ETF 종목 리스트 수집
# ------------------------------------------------------------
def get_us_etf_list_with_issuer():
    base_url = "https://api.nasdaq.com/api/screener/etf?tableonly=true&limit=50&offset={}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nasdaq.com/market-activity/etfs",
    }

    all_rows = []
    offset = 0

    while True:
        url = base_url.format(offset)
        r = requests.get(url, headers=headers, timeout=20)

        if r.status_code != 200:
            print(f"[WARN] 요청 실패 (offset={offset}) → {r.status_code}")
            break

        data = r.json()
        try:
            rows = data["data"]["records"]["data"]["rows"]
        except KeyError:
            print(f"[WARN] 데이터 구조 오류 (offset={offset})")
            break

        if not rows:
            break

        all_rows.extend(rows)
        print(f"[INFO] {offset} ~ {offset + 50} 구간 수집 (누적 {len(all_rows)}개)")

        offset += 50
        time.sleep(0.5)

        if offset > 4000:
            break

    df = pd.DataFrame(all_rows)[["symbol", "companyName"]]
    df.columns = ["code", "name"]

    df["market"] = "US_ETF"
    df = df.dropna(subset=["code", "name"])
    df["code"] = df["code"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    issuer_patterns = {
        "Vanguard": "Vanguard",
        "iShares": "BlackRock (iShares)",
        "SPDR": "State Street (SPDR)",
        "Invesco": "Invesco",
        "Schwab": "Charles Schwab",
        "Global X": "Mirae Asset (Global X)",
        "ARK": "ARK Invest",
        "VanEck": "VanEck",
        "WisdomTree": "WisdomTree",
        "ProShares": "ProShares",
        "Direxion": "Direxion",
        "Amplify": "Amplify",
        "First Trust": "First Trust",
        "Gabelli": "Gabelli",
        "FT Vest": "First Trust",
        "Genter Capital": "Genter Capital",
        "PIMCO": "PIMCO",
        "JPMorgan": "J.P. Morgan",
    }

    def extract_issuer(name):
        for keyword, issuer_name in issuer_patterns.items():
            if re.search(rf"^{keyword}|{keyword}", name, re.IGNORECASE):
                return issuer_name
        first_word = name.split()[0] if name else "N/A"
        return f"[Unknown: {first_word}]"

    df["issuer"] = df["name"].apply(extract_issuer)

    return df


# ------------------------------------------------------------
# 2. MongoDB 저장 (UPSERT)
# ------------------------------------------------------------
def save_us_etf_info_mongo(df):
    client = MongoClient("mongodb://root:0806@localhost:27017/?authSource=admin")
    db = client["investar"]
    col = db["etf_info_us"]

    today = datetime.now().strftime("%Y-%m-%d")

    for _, row in df.iterrows():
        doc = {
            "code": row["code"],
            "name": row["name"],
            "issuer": row["issuer"],
            "market": row["market"],
            "last_update": today
        }

        col.update_one(
            {"code": row["code"]},
            {"$set": doc},
            upsert=True
        )

    client.close()
    print(f"[OK] {len(df)}개 ETF MongoDB 저장 완료")


# ------------------------------------------------------------
# 3. 실행
# ------------------------------------------------------------
if __name__ == "__main__":
    etf_df = get_us_etf_list_with_issuer()
    print(f"총 {len(etf_df)}개 미국 ETF 수집 완료")

    save_us_etf_info_mongo(etf_df)
