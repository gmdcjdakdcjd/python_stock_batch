import pandas as pd
from bs4 import BeautifulSoup
import pymysql
import requests
from datetime import datetime
import json
import re


class FXDBUpdater:
    def __init__(self):
        """MariaDB 연결 + config_fx.json 로드"""
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='0806',
            db='INVESTAR',
            charset='utf8'
        )

        # config_fx.json 로드
        try:
            with open('config_fx.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.pages_to_fetch = config.get("pages_to_fetch", 1)
        except FileNotFoundError:
            self.pages_to_fetch = 1
            with open('config_fx.json', 'w', encoding='utf-8') as f:
                json.dump({"pages_to_fetch": 1}, f, indent=4, ensure_ascii=False)

        print(f"[INFO] pages_to_fetch = {self.pages_to_fetch}")

    def __del__(self):
        self.conn.close()

    # ----------------------------------------------------------------
    # 1) USDKRW 한 페이지 스크래핑
    # ----------------------------------------------------------------
    def read_fx_usdkrw(self, page):
        """네이버 USD/KRW 특정 페이지 스크래핑"""
        try:
            url = (
                f"https://finance.naver.com/marketindex/exchangeDailyQuote.naver"
                f"?marketindexCd=FX_USDKRW&page={page}"
            )

            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://finance.naver.com/marketindex/exchangeDetail.naver?marketindexCd=FX_USDKRW"
            }

            html = requests.get(url, headers=headers).text
            soup = BeautifulSoup(html, "lxml")

            rows = soup.select("table.tbl_exchange.today tbody tr")
            data = []

            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 3:
                    continue

                # 날짜
                date_raw = cols[0].text.strip()
                if date_raw == "" or date_raw.count(".") != 2:
                    continue
                date = date_raw.replace(".", "-")

                # 매매기준율(close)
                close_raw = cols[1].text.strip().replace(",", "")
                if close_raw == "":
                    continue
                close = float(close_raw)

                # 전일대비(change_amount)
                diff_td = cols[2]

                sign = 1
                img = diff_td.find("img")
                if img and "하락" in img.get("alt", ""):
                    sign = -1

                text = diff_td.get_text(strip=True)
                text = text.replace("상승", "").replace("하락", "").strip()

                m = re.search(r"-?\d+\.?\d*", text)
                if not m:
                    continue

                change_amount = sign * float(m.group())

                # 네이버 특유 100배 스케일 보정
                if abs(change_amount) >= 100:
                    change_amount /= 100

                data.append([date, close, change_amount])

            return pd.DataFrame(data, columns=["date", "close", "change_amount"])

        except Exception as e:
            print("Exception (USD page):", e)
            return pd.DataFrame()

    # ----------------------------------------------------------------
    # 2) pages_to_fetch 만큼 전체 페이지 수집
    # ----------------------------------------------------------------
    def collect_all_pages(self):
        frames = []

        for page in range(1, self.pages_to_fetch + 1):
            print(f"[INFO] USDKRW {page}/{self.pages_to_fetch} 페이지 수집 중...")
            df = self.read_fx_usdkrw(page)
            if df.empty:
                print(f"[WARN] {page} 페이지는 데이터 없음 → 중단")
                break
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)

        # 등락률 계산
        df_all["change_rate"] = df_all.apply(
            lambda row: (row["change_amount"] /
                         (row["close"] - row["change_amount"]) * 100)
            if row["close"] != row["change_amount"] else 0.0, axis=1
        )

        return df_all

    # ----------------------------------------------------------------
    # 3) DB 저장 (과거 → 최신)
    # ----------------------------------------------------------------
    def replace_fx_into_db(self, df):
        with self.conn.cursor() as curs:
            df_sorted = df.sort_values("date")

            for r in df_sorted.itertuples():
                sql = f"""
                REPLACE INTO market_indicator 
                (code, date, close, change_amount, change_rate)
                VALUES ('USD', '{r.date}', {r.close}, {r.change_amount}, {r.change_rate});
                """
                curs.execute(sql)

        self.conn.commit()
        print(f"[INFO] USD {len(df)} rows 저장 완료 (과거 → 최신)")

    # ----------------------------------------------------------------
    # 4) 실행
    # ----------------------------------------------------------------
    def update_usdkrw(self):
        print("[INFO] USD 환율 수집 시작...")

        df = self.collect_all_pages()

        if df is not None and not df.empty:
            self.replace_fx_into_db(df)
            print("[INFO] USD 환율 업데이트 완료")
        else:
            print("[WARN] 데이터가 비어 있어 업데이트 중단")


if __name__ == '__main__':
    updater = FXDBUpdater()
    updater.update_usdkrw()
