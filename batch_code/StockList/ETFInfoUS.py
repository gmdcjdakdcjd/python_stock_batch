import requests
import pandas as pd
import pymysql
import re
from datetime import datetime
import time


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
            print(f"⚠️ 요청 실패 (offset={offset}) → {r.status_code}")
            break

        data = r.json()
        try:
            rows = data["data"]["records"]["data"]["rows"]
        except KeyError:
            print(f"⚠️ 데이터 구조 이상 (offset={offset})")
            break

        if not rows:
            break

        all_rows.extend(rows)
        print(f"✅ {offset} ~ {offset + 50} 구간 수집 완료 ({len(all_rows)} 누적)")
        offset += 50

        # API rate limit 방지 (너무 빨리 요청하면 429 뜸)
        time.sleep(0.5)

        # 혹시나 4000개 넘어가면 중단 (안전장치)
        if offset > 4000:
            break

    df = pd.DataFrame(all_rows)[["symbol", "companyName"]]
    df.columns = ["code", "name"]
    df["market"] = "US_ETF"

    # ✅ 데이터 클린업: null 제거, 줄바꿈/공백 제거
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
        for key, val in issuer_patterns.items():
            if re.search(rf"^{key}|{key}", name, re.IGNORECASE):
                return val
        first_word = name.split()[0] if name else "N/A"
        return f"[Unknown: {first_word}]"

    df["issuer"] = df["name"].apply(extract_issuer)
    return df


def save_us_etf_info(df):
    conn = pymysql.connect(
        host="localhost", user="root", password="0806",
        db="INVESTAR", charset="utf8"
    )
    with conn.cursor() as curs:
        for _, row in df.iterrows():
            sql = """
                INSERT INTO etf_info_us (code, name, issuer, market, last_update)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    issuer = VALUES(issuer),
                    market = VALUES(market),
                    last_update = VALUES(last_update)
            """
            curs.execute(sql, (
                row["code"],
                row["name"],
                row["issuer"],
                row["market"],
                datetime.now()
            ))
    conn.commit()
    conn.close()
    print(f"{len(df)}건 저장 완료")


if __name__ == "__main__":
    etf_df = get_us_etf_list_with_issuer()
    print(f"총 {len(etf_df)}개 ETF 수집 완료")
    save_us_etf_info(etf_df)
